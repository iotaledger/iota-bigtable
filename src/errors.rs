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

impl BigTableClientError {
    /// Converts this error into a [`backoff::Error`].
    pub(crate) fn into_backoff_error(self) -> backoff::Error<Self> {
        use tonic::Code::*;

        if let BigTableClientError::Grpc(status) = &self
            && matches!(
                status.code(),
                Cancelled
                    | Aborted
                    | Internal
                    | Unknown
                    | Unavailable
                    | DeadlineExceeded
                    | ResourceExhausted
            )
        {
            return backoff::Error::transient(self);
        }

        // we don't have access to the error kind, so we use the error message instead.
        if let BigTableClientError::GrpcTransport(e) = &self
            && e.to_string().contains("transport error")
        {
            return backoff::Error::transient(self);
        }

        backoff::Error::permanent(self)
    }
}
