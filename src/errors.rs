// Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use thiserror::Error;

pub type Result<T> = std::result::Result<T, BigTableClientError>;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum BigTableClientError {
    #[error("gRPC client error: `{0}`")]
    Grpc(#[from] tonic::Status),
    #[error("gRPC transport error: `{0}`")]
    GrpcTransport(#[from] tonic::transport::Error),
    #[error("Environment variable error: `{0}`")]
    Env(#[from] std::env::VarError),
    #[error("gRPC channel error: `{0}`")]
    InvalidUri(#[from] http::uri::InvalidUri),
    #[error("GCP Auth error: `{0}`")]
    Gcp(#[from] gcp_auth::Error),
    #[error("Bigtable write error: code `{status}`, message: `{message}`")]
    BigtableWriteError { status: i32, message: String },
    #[error("Header value error: `{0}`")]
    InvalidHeaderValue(#[from] http::header::InvalidHeaderValue),
}
