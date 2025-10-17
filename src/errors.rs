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
    #[error("environment variable error: `{0}`")]
    Env(#[from] std::env::VarError),
    #[error("invalid URI: `{0}`")]
    InvalidUri(#[from] http::uri::InvalidUri),
    #[error("gpc auth error: `{0}`")]
    GcpAuth(#[from] gcp_auth::Error),
    #[error("nigtable write error: code `{status}`, message: `{message}`")]
    BigtableWriteError { status: i32, message: String },
    #[error("header value error: `{0}`")]
    InvalidHeaderValue(#[from] http::header::InvalidHeaderValue),
    #[error("io error: `{0}`")]
    Io(#[from] std::io::Error),
}
