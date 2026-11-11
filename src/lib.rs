// Copyright (c) Mysten Labs, Inc.
// Modifications Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

mod channel_pool;
/// Implementation of the BigTableDB client and its R&W operations.
mod client;
/// BigTableDB client errors.
pub mod errors;
/// DB operations metrics.
pub mod metrics;
#[cfg(test)]
mod mock_server;
/// Compiled protobuf definitions.
pub mod proto;

pub use channel_pool::PoolConfig;
pub use client::{BigTableClient, Bytes, Cell, Row};
