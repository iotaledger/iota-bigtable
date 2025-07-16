// Copyright (c) Mysten Labs, Inc.
// Modifications Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, RwLock},
    task::{Context, Poll},
    time::{Duration, Instant},
};

use gcp_auth::{Token, TokenProvider};
use http::{HeaderValue, Request, Response};
use prometheus::Registry;
use prost::Message;
use serde::{Deserialize, Serialize};
use tonic::{
    Streaming,
    body::Body,
    codegen::Service,
    transport::{Certificate, Channel, ClientTlsConfig},
};

use crate::{
    errors::{BigTableClientError, Result},
    metrics::Metrics,
    proto::bigtable::v2::{
        MutateRowsRequest, MutateRowsResponse, Mutation, ReadRowsRequest, RowFilter, RowRange,
        RowSet,
        bigtable_client::BigtableClient as BigtableInternalClient,
        mutate_rows_request::Entry,
        mutation::{self, SetCell},
        read_rows_response::cell_chunk::RowStatus,
        row_range::EndKey,
    },
};

/// BigTable GRPC Server max request size in bytes.
const GRPC_MAX_REQUEST_SIZE: usize = 250 * 1024 * 1024; // 250 MB
/// PEM certificate from Google Trust Services.
const PEM_CERT: &[u8] = include_bytes!("../certs/google.pem");
const MAX_MUTATIONS_PER_MUTATE_ROWS_REQUEST: usize = 100_000;
const BIGTABLE_POLICY: &str = "https://www.googleapis.com/auth/bigtable.data";
const BIGTABLE_API: &str = "https://bigtable.googleapis.com";
const BIGTABLE_DOMAIN: &str = "bigtable.googleapis.com";

pub type Bytes = Vec<u8>;

/// Represents a single cell in a BigTable row.
///
/// Each cell contains a column name and a value.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Cell {
    /// The column name (qualifier) of the cell.
    pub name: Bytes,
    /// The value stored in the cell.
    pub value: Bytes,
}

impl Cell {
    pub fn new(name: Bytes, value: Bytes) -> Self {
        Self { name, value }
    }
}

/// Represents a row in a BigTable table.
///
/// Each row has a unique key and a collection of cells.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Row {
    /// The unique key identifying the row.
    pub key: Bytes,
    /// The cells contained in the row.
    pub cells: Vec<Cell>,
}

impl Row {
    pub fn new(key: Bytes, cells: Vec<Cell>) -> Self {
        Self { key, cells }
    }
}

/// A high-level client for interacting with Google BigTable using authenticated
/// requests over gRPC.
#[derive(Clone)]
pub struct BigTableClient {
    client: BigtableInternalClient<AuthChannel>,
    client_name: String,
    column_family: String,
    table_prefix: String,
    metrics: Option<Arc<Metrics>>,
}

impl BigTableClient {
    /// Creates a new BigTableClient instance for a local instance using the
    /// emulator feature. It reads the emulator host from the
    /// `BIGTABLE_EMULATOR_HOST` environment variable.
    pub async fn new_local(
        instance_id: impl AsRef<str>,
        column_family: impl Into<String>,
    ) -> Result<Self> {
        let emulator_host = std::env::var("BIGTABLE_EMULATOR_HOST")?;
        let channel = Channel::from_shared(format!("http://{emulator_host}"))?.connect_lazy();
        let auth_channel = AuthChannel::new_localhost(channel, BIGTABLE_POLICY);
        Ok(Self {
            table_prefix: format!(
                "projects/emulator/instances/{}/tables/",
                instance_id.as_ref()
            ),
            client: BigtableInternalClient::new(auth_channel),
            client_name: "local".to_string(),
            column_family: column_family.into(),
            metrics: None,
        })
    }

    /// Creates a new BigTableClient instance for a remote instance. It checks
    /// for the `GOOGLE_APPLICATION_CREDENTIALS` environment variable.
    pub async fn new_remote(
        instance_id: impl AsRef<str>,
        is_read_only: bool,
        timeout: Option<Duration>,
        client_name: impl Into<String>,
        column_family: impl Into<String>,
        registry: Option<&Registry>,
    ) -> Result<Self> {
        let policy = is_read_only
            .then_some(format!("{BIGTABLE_POLICY}.readonly"))
            .unwrap_or(BIGTABLE_POLICY.into());

        let token_provider = gcp_auth::provider().await?;

        let tls_config = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(PEM_CERT))
            .domain_name(BIGTABLE_DOMAIN);
        let mut endpoint = Channel::from_static(BIGTABLE_API)
            .http2_keep_alive_interval(Duration::from_secs(60))
            .keep_alive_while_idle(true)
            .tls_config(tls_config)?;
        if let Some(timeout) = timeout {
            endpoint = endpoint.timeout(timeout);
        }
        let table_prefix = format!(
            "projects/{}/instances/{}/tables/",
            token_provider.project_id().await?,
            instance_id.as_ref()
        );
        let auth_channel = AuthChannel::new_remote(endpoint.connect_lazy(), policy, token_provider);
        Ok(Self {
            table_prefix,
            client: BigtableInternalClient::new(auth_channel),
            client_name: client_name.into(),
            column_family: column_family.into(),
            metrics: registry.map(Metrics::new),
        })
    }

    fn table_name(&self, table_name: &str) -> String {
        format!("{}{table_name}", self.table_prefix)
    }

    /// Mutates multiple rows in a batch. Each individual row is mutated
    /// atomically as in MutateRow, but the entire batch is not executed
    /// atomically.
    pub async fn mutate_rows(
        &mut self,
        request: MutateRowsRequest,
    ) -> Result<Streaming<MutateRowsResponse>> {
        Ok(self.client.mutate_rows(request).await?.into_inner())
    }

    /// Reads rows from Bigtable and returns their keys and cell values.
    pub async fn read_rows(&mut self, request: ReadRowsRequest) -> Result<Vec<Row>> {
        let mut rows = vec![];
        let mut response = self.client.read_rows(request).await?.into_inner();

        let mut row_key = None;
        let mut row = vec![];
        let mut cell_value = vec![];
        let mut cell_name = None;
        let mut timestamp = 0;

        while let Some(message) = response.message().await? {
            for mut chunk in message.chunks.into_iter() {
                // new row check
                if !chunk.row_key.is_empty() {
                    row_key = Some(chunk.row_key);
                }
                match chunk.qualifier {
                    // new cell started
                    Some(qualifier) => {
                        if let Some(cell_name) = cell_name {
                            row.push(Cell::new(cell_name, cell_value));
                            cell_value = vec![];
                        }
                        cell_name = Some(qualifier);
                        timestamp = chunk.timestamp_micros;
                        cell_value.append(&mut chunk.value);
                    }
                    None => {
                        if chunk.timestamp_micros == 0 {
                            cell_value.append(&mut chunk.value);
                        } else if chunk.timestamp_micros >= timestamp {
                            // newer version of cell is available
                            timestamp = chunk.timestamp_micros;
                            cell_value = chunk.value;
                        }
                    }
                }
                if chunk.row_status.is_some() {
                    if let Some(RowStatus::CommitRow(_)) = chunk.row_status {
                        if let Some(cell_name) = cell_name {
                            row.push(Cell::new(cell_name, cell_value));
                        }
                        if let Some(row_key) = row_key {
                            rows.push(Row::new(row_key, row));
                        }
                    }
                    row_key = None;
                    row = vec![];
                    cell_value = vec![];
                    cell_name = None;
                }
            }
        }
        Ok(rows)
    }

    /// Sets multiple rows in Bigtable in a single batch operation.
    ///
    /// The input is automatically split into chunks of up to 100,000 rows.
    /// If a chunk exceeds the gRPC request size limit imposed by Google
    /// Bigtable, it will be further split into smaller chunks, each not
    /// exceeding 250MB. This ensures optimal throughput for bulk data
    /// writes while maintaining compatibility with server constraints.
    pub async fn multi_set(
        &mut self,
        table_name: &str,
        values: impl IntoIterator<Item = Row> + std::marker::Send,
    ) -> Result<()> {
        for chunk in values
            .into_iter()
            .collect::<Vec<_>>()
            .chunks(MAX_MUTATIONS_PER_MUTATE_ROWS_REQUEST)
        {
            self.multi_set_internal(table_name, chunk.iter().cloned())
                .await?;
        }
        Ok(())
    }

    async fn multi_set_internal(
        &mut self,
        table_name: &str,
        values: impl IntoIterator<Item = Row> + std::marker::Send,
    ) -> Result<()> {
        let entries = values
            .into_iter()
            .map(|row| {
                let mutations = row
                    .cells
                    .into_iter()
                    .map(|cell| Mutation {
                        mutation: Some(mutation::Mutation::SetCell(SetCell {
                            family_name: self.column_family.clone(),
                            column_qualifier: cell.name,
                            // The timestamp of the cell into which new data should be written.
                            // Use -1 for current Bigtable server time.
                            timestamp_micros: -1,
                            value: cell.value,
                        })),
                    })
                    .collect();
                Entry {
                    row_key: row.key,
                    mutations,
                }
            })
            .collect::<Vec<Entry>>();

        for entries in Self::batch_entries_by_size(entries) {
            let request = MutateRowsRequest {
                table_name: self.table_name(table_name),
                entries,
                ..MutateRowsRequest::default()
            };

            let mut response = self.mutate_rows(request).await?;
            while let Some(part) = response.message().await? {
                for entry in part.entries {
                    if let Some(status) = entry.status {
                        if status.code != 0 {
                            return Err(BigTableClientError::BigtableWriteError {
                                status: status.code,
                                message: status.message,
                            });
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Retrieves the cells for multiple rows from Bigtable in a single batch
    /// operation.
    pub async fn multi_get(
        &mut self,
        table_name: &str,
        keys: Vec<Bytes>,
        filter: Option<RowFilter>,
    ) -> Result<Vec<Vec<Cell>>> {
        let start_time = Instant::now();
        let num_keys_requested = keys.len();
        let result = self.multi_get_internal(table_name, keys, filter).await;
        let elapsed_ms = start_time.elapsed().as_millis() as f64;

        let Some(metrics) = &self.metrics else {
            return result;
        };

        let labels = [&self.client_name, table_name];
        let Ok(rows) = &result else {
            metrics.get_errors.with_label_values(&labels).inc();
            return result;
        };

        metrics
            .get_batch_size
            .with_label_values(&labels)
            .observe(num_keys_requested as f64);

        if num_keys_requested > rows.len() {
            metrics
                .get_not_found
                .with_label_values(&labels)
                .inc_by((num_keys_requested - rows.len()) as u64);
        }

        metrics
            .get_success
            .with_label_values(&labels)
            .inc_by(rows.len() as u64);

        metrics
            .get_latency_ms
            .with_label_values(&labels)
            .observe(elapsed_ms);

        if num_keys_requested > 0 {
            metrics
                .get_latency_ms_per_key
                .with_label_values(&labels)
                .observe(elapsed_ms / num_keys_requested as f64);
        }

        result
    }

    async fn multi_get_internal(
        &mut self,
        table_name: &str,
        keys: Vec<Bytes>,
        filter: Option<RowFilter>,
    ) -> Result<Vec<Vec<Cell>>> {
        let request = ReadRowsRequest {
            table_name: self.table_name(table_name),
            rows_limit: keys.len() as i64,
            rows: Some(RowSet {
                row_keys: keys,
                row_ranges: vec![],
            }),
            filter,
            ..ReadRowsRequest::default()
        };
        let mut result = vec![];
        for row in self.read_rows(request).await? {
            result.push(row.cells);
        }
        Ok(result)
    }

    /// Performs a reverse scan over rows in Bigtable, starting from an upper
    /// limit. Useful for retrieving data in descending order.
    pub async fn reversed_scan(
        &mut self,
        table_name: &str,
        upper_limit: Bytes,
    ) -> Result<Vec<Row>> {
        let start_time = Instant::now();
        let result = self.reversed_scan_internal(table_name, upper_limit).await;
        let elapsed_ms = start_time.elapsed().as_millis() as f64;
        let labels = [&self.client_name, table_name];
        match &self.metrics {
            Some(metrics) => match result {
                Ok(result) => {
                    metrics.scan_success.with_label_values(&labels).inc();
                    if result.is_empty() {
                        metrics.scan_not_found.with_label_values(&labels).inc();
                    }
                    metrics
                        .scan_latency_ms
                        .with_label_values(&labels)
                        .observe(elapsed_ms);
                    Ok(result)
                }
                Err(e) => {
                    metrics.scan_error.with_label_values(&labels).inc();
                    Err(e)
                }
            },
            None => result,
        }
    }

    async fn reversed_scan_internal(
        &mut self,
        table_name: &str,
        upper_limit: Bytes,
    ) -> Result<Vec<Row>> {
        let range = RowRange {
            start_key: None,
            end_key: Some(EndKey::EndKeyClosed(upper_limit)),
        };
        let request = ReadRowsRequest {
            table_name: self.table_name(table_name),
            rows_limit: 1,
            rows: Some(RowSet {
                row_keys: vec![],
                row_ranges: vec![range],
            }),
            reversed: true,
            ..ReadRowsRequest::default()
        };
        self.read_rows(request).await
    }

    /// Splits a vector of `Entry` messages into batches such that the total
    /// serialized size of each batch does not exceed the gRPC message size
    /// limit (250 MB).
    ///
    /// This function serializes each entry to determine its size, and
    /// accumulates entries into a batch until adding another entry would
    /// exceed the size limit. It then starts a new batch. The result is a
    /// vector of batches, each of which can be safely sent as a single gRPC
    /// request without exceeding the configured maximum message size.
    fn batch_entries_by_size(entries: Vec<Entry>) -> Vec<Vec<Entry>> {
        let mut batches = Vec::new();
        let mut current_batch = Vec::new();
        let mut current_size = 0;

        for entry in entries {
            // estimate size by serializing the entry.
            let entry_size = entry.encoded_len();

            // if adding this entry would exceed the limit, start a new batch.
            if current_size + entry_size >= GRPC_MAX_REQUEST_SIZE && !current_batch.is_empty() {
                batches.push(current_batch);
                current_batch = Vec::new();
                current_size = 0;
            }

            current_batch.push(entry);
            current_size += entry_size;
        }

        // push the last batch if not empty.
        if !current_batch.is_empty() {
            batches.push(current_batch);
        }

        batches
    }
}

/// A smart, thread-safe wrapper around a gRPC channel that transparently
/// manages authentication and header injection for requests to Google Bigtable.
///
/// # Purpose
/// - Handles authentication using tokens, automatically injecting a valid
///   `Authorization` header into each outgoing request if a `token_provider` is
///   configured.
/// - Caches tokens and refreshes them only when expired, ensuring efficient and
///   secure communication.
/// - Injects additional headers (such as `bigtable-features`) required for
///   enabling specific Bigtable features.
///
/// # Behavior
/// - On each request, checks if a valid token is cached; if not, fetches a new
///   one.
/// - Adds the `Authorization: Bearer <token>` header when needed.
/// - Always adds the `bigtable-features` header.
/// - Implements the `Service` trait to act as middleware in the gRPC stack,
///   intercepting and modifying requests.
///
/// # Usage
/// Used internally by `BigTableClient` to ensure all requests are properly
/// authorized and feature-enabled when communicating with Google Bigtable.
#[derive(Clone)]
struct AuthChannel {
    // The underlying gRPC channel used for communication.
    channel: Channel,
    // The access policy (scope) for which tokens are requested.
    policy: String,
    // Provides tokens for authentication.
    token_provider: Option<Arc<dyn TokenProvider>>,
    // Caches the current token.
    token: Arc<RwLock<Option<Arc<Token>>>>,
}

impl AuthChannel {
    /// Creates a new `AuthChannel` for localhost communication. It does not
    /// require authentication.
    fn new_localhost(channel: Channel, policy: impl Into<String>) -> Self {
        Self {
            channel,
            policy: policy.into(),
            token_provider: None,
            token: Arc::new(RwLock::new(None)),
        }
    }

    /// Creates a new `AuthChannel` for remote communication. It requires
    /// authentication.
    fn new_remote(
        channel: Channel,
        policy: impl Into<String>,
        token_provider: Arc<dyn TokenProvider>,
    ) -> Self {
        Self {
            channel,
            policy: policy.into(),
            token_provider: Some(token_provider),
            token: Arc::new(RwLock::new(None)),
        }
    }

    /// Get a valid cached token if a provider exists.
    fn cached_token(&self) -> Option<Arc<Token>> {
        self.token_provider.as_ref()?;
        self.token
            .read()
            .expect("failed to acquire a read lock")
            .as_ref()
            .filter(|token| !token.has_expired())
            .cloned()
    }
}

impl Service<Request<Body>> for AuthChannel {
    type Response = Response<Body>;
    type Error = BigTableClientError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response>> + Send>>;

    // Checks if the underlying channel is ready to send a request.
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.channel.poll_ready(cx).map_err(Into::into)
    }
    // Handles an outgoing request:
    // - Injects authentication and feature headers.
    // - Forwards the request to the underlying channel.
    fn call(&mut self, mut request: Request<Body>) -> Self::Future {
        let cloned_channel = self.channel.clone();
        let cloned_token = self.token.clone();
        let mut inner = std::mem::replace(&mut self.channel, cloned_channel);
        let policy = self.policy.clone();
        let token_provider = self.token_provider.clone();

        let auth_token = self.cached_token();

        Box::pin(async move {
            // if a token provider exists, ensure a valid token is present and then
            // if no valid token cached, fetch a new one and cache it, otherwise use the
            // cached valid token.
            if let Some(ref provider) = token_provider {
                let token = match auth_token {
                    None => {
                        let new_token = provider.token(&[&policy]).await?;
                        let mut guard = cloned_token.write().unwrap();
                        *guard = Some(new_token.clone());
                        new_token
                    }
                    Some(token) => token,
                };
                // insert the Authorization header with the Bearer token.
                let header = HeaderValue::from_str(&format!("Bearer {}", token.as_str()))?;
                request.headers_mut().insert("authorization", header);
            }
            // always insert the Bigtable features header (e.g., to enable reverse scan).
            let header = HeaderValue::from_static("CAE=");
            request.headers_mut().insert("bigtable-features", header);

            // forward the request to the underlying channel and return the response.
            Ok(inner.call(request).await?)
        })
    }
}
