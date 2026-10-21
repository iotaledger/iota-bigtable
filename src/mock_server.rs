// Copyright (c) Mysten Labs, Inc.
// Modifications Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! Minimal Bigtable gRPC server for channel pool tests.

use std::{
    net::SocketAddr,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use tokio_stream::Stream;
use tonic::{
    Request, Response, Status,
    transport::{Channel, Endpoint, Server},
};

use crate::proto::bigtable::v2::{
    CheckAndMutateRowRequest, CheckAndMutateRowResponse, ExecuteQueryRequest, ExecuteQueryResponse,
    GenerateInitialChangeStreamPartitionsRequest, GenerateInitialChangeStreamPartitionsResponse,
    MutateRowRequest, MutateRowResponse, MutateRowsRequest, MutateRowsResponse, PingAndWarmRequest,
    PingAndWarmResponse, PrepareQueryRequest, PrepareQueryResponse, ReadChangeStreamRequest,
    ReadChangeStreamResponse, ReadModifyWriteRowRequest, ReadModifyWriteRowResponse,
    ReadRowsRequest, ReadRowsResponse, SampleRowKeysRequest, SampleRowKeysResponse,
    bigtable_server::{Bigtable, BigtableServer},
};

type UnimplementedStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

/// The mocked Bigtable gRPC service installed in the tonic server. It shares
/// the request counter with the [`MockBigtableServerHandle`] returned by
/// [`start`](Self::start).
#[derive(Clone, Default)]
pub(crate) struct MockBigtableServer {
    request_count: Arc<AtomicUsize>,
}

impl MockBigtableServer {
    /// Binds to a random local port and serves in a background task until the
    /// returned handle is dropped.
    pub(crate) async fn start() -> std::io::Result<MockBigtableServerHandle> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let server = Self::default();
        let request_count = server.request_count.clone();

        let task = tokio::spawn(async move {
            let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
            Server::builder()
                .add_service(BigtableServer::new(server))
                .serve_with_incoming(incoming)
                .await
                .ok();
        });

        Ok(MockBigtableServerHandle {
            addr,
            request_count,
            task,
        })
    }
}

/// Handle to a running [`MockBigtableServer`] listening on a random local
/// port. The serving task is aborted when this is dropped, so a test keeps it
/// alive for as long as the server is needed.
pub(crate) struct MockBigtableServerHandle {
    addr: SocketAddr,
    request_count: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}

impl MockBigtableServerHandle {
    /// Plain-HTTP endpoint pointing at this server.
    pub(crate) fn endpoint(&self) -> Endpoint {
        Channel::from_shared(format!("http://{}", self.addr)).expect("valid loopback URL")
    }

    /// Number of RPCs received so far.
    pub(crate) fn request_count(&self) -> usize {
        self.request_count.load(Ordering::Relaxed)
    }
}

impl Drop for MockBigtableServerHandle {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[tonic::async_trait]
impl Bigtable for MockBigtableServer {
    type ReadRowsStream = UnimplementedStream<ReadRowsResponse>;
    type SampleRowKeysStream = UnimplementedStream<SampleRowKeysResponse>;
    type MutateRowsStream = UnimplementedStream<MutateRowsResponse>;
    type GenerateInitialChangeStreamPartitionsStream =
        UnimplementedStream<GenerateInitialChangeStreamPartitionsResponse>;
    type ReadChangeStreamStream = UnimplementedStream<ReadChangeStreamResponse>;
    type ExecuteQueryStream = UnimplementedStream<ExecuteQueryResponse>;

    async fn ping_and_warm(
        &self,
        _request: Request<PingAndWarmRequest>,
    ) -> Result<Response<PingAndWarmResponse>, Status> {
        self.request_count.fetch_add(1, Ordering::Relaxed);
        Ok(Response::new(PingAndWarmResponse {}))
    }

    async fn read_rows(
        &self,
        _request: Request<ReadRowsRequest>,
    ) -> Result<Response<Self::ReadRowsStream>, Status> {
        Err(Status::unimplemented("read_rows"))
    }

    async fn sample_row_keys(
        &self,
        _request: Request<SampleRowKeysRequest>,
    ) -> Result<Response<Self::SampleRowKeysStream>, Status> {
        Err(Status::unimplemented("sample_row_keys"))
    }

    async fn mutate_row(
        &self,
        _request: Request<MutateRowRequest>,
    ) -> Result<Response<MutateRowResponse>, Status> {
        Err(Status::unimplemented("mutate_row"))
    }

    async fn mutate_rows(
        &self,
        _request: Request<MutateRowsRequest>,
    ) -> Result<Response<Self::MutateRowsStream>, Status> {
        Err(Status::unimplemented("mutate_rows"))
    }

    async fn check_and_mutate_row(
        &self,
        _request: Request<CheckAndMutateRowRequest>,
    ) -> Result<Response<CheckAndMutateRowResponse>, Status> {
        Err(Status::unimplemented("check_and_mutate_row"))
    }

    async fn read_modify_write_row(
        &self,
        _request: Request<ReadModifyWriteRowRequest>,
    ) -> Result<Response<ReadModifyWriteRowResponse>, Status> {
        Err(Status::unimplemented("read_modify_write_row"))
    }

    async fn generate_initial_change_stream_partitions(
        &self,
        _request: Request<GenerateInitialChangeStreamPartitionsRequest>,
    ) -> Result<Response<Self::GenerateInitialChangeStreamPartitionsStream>, Status> {
        Err(Status::unimplemented(
            "generate_initial_change_stream_partitions",
        ))
    }

    async fn read_change_stream(
        &self,
        _request: Request<ReadChangeStreamRequest>,
    ) -> Result<Response<Self::ReadChangeStreamStream>, Status> {
        Err(Status::unimplemented("read_change_stream"))
    }

    async fn prepare_query(
        &self,
        _request: Request<PrepareQueryRequest>,
    ) -> Result<Response<PrepareQueryResponse>, Status> {
        Err(Status::unimplemented("prepare_query"))
    }

    async fn execute_query(
        &self,
        _request: Request<ExecuteQueryRequest>,
    ) -> Result<Response<Self::ExecuteQueryStream>, Status> {
        Err(Status::unimplemented("execute_query"))
    }
}
