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
use tonic::{Request, Response, Status, transport::Server};

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

#[derive(Clone, Default)]
pub(crate) struct MockBigtableServer {
    /// Number of RPCs received.
    pub(crate) request_count: Arc<AtomicUsize>,
}

impl MockBigtableServer {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Bind to a random local port and serve in a background task.
    pub(crate) async fn start(&self) -> std::io::Result<(SocketAddr, tokio::task::JoinHandle<()>)> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let mock = self.clone();

        let handle = tokio::spawn(async move {
            let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
            Server::builder()
                .add_service(BigtableServer::new(mock))
                .serve_with_incoming(incoming)
                .await
                .ok();
        });

        Ok((addr, handle))
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
