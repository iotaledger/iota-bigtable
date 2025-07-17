// Copyright (c) Mysten Labs, Inc.
// Modifications Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use prometheus::{
    HistogramVec, IntCounterVec, Registry, register_histogram_vec_with_registry,
    register_int_counter_vec_with_registry,
};

pub(crate) struct Metrics {
    pub(crate) get_success: IntCounterVec,
    pub(crate) get_not_found: IntCounterVec,
    pub(crate) get_errors: IntCounterVec,
    pub(crate) get_latency_ms: HistogramVec,
    pub(crate) get_batch_size: HistogramVec,
    pub(crate) get_latency_ms_per_key: HistogramVec,
    pub(crate) scan_success: IntCounterVec,
    pub(crate) scan_not_found: IntCounterVec,
    pub(crate) scan_error: IntCounterVec,
    pub(crate) scan_latency_ms: HistogramVec,
}

impl Metrics {
    const LABLES: &[&'static str; 2] = &["client", "table"];

    pub(crate) fn new(registry: &Registry) -> Arc<Self> {
        Arc::new(Self {
            get_success: register_int_counter_vec_with_registry!(
                "get_success",
                "Number of successful fetches from BigTableDB",
                Self::LABLES,
                registry,
            )
            .unwrap(),
            get_not_found: register_int_counter_vec_with_registry!(
                "get_not_found",
                "Number of fetches from BigTableDB that returned not found",
                Self::LABLES,
                registry,
            )
            .unwrap(),
            get_errors: register_int_counter_vec_with_registry!(
                "get_errors",
                "Number of fetches from BigTableDB that returned an error",
                Self::LABLES,
                registry,
            )
            .unwrap(),
            get_latency_ms: register_histogram_vec_with_registry!(
                "get_latency_ms",
                "Latency of fetches from BigTableDB",
                Self::LABLES,
                prometheus::exponential_buckets(1.0, 1.6, 24)
                    .unwrap()
                    .to_vec(),
                registry,
            )
            .unwrap(),
            get_batch_size: register_histogram_vec_with_registry!(
                "get_batch_size",
                "Number of keys fetched per batch from BigTableDB",
                Self::LABLES,
                prometheus::exponential_buckets(1.0, 1.6, 20)
                    .unwrap()
                    .to_vec(),
                registry,
            )
            .unwrap(),
            get_latency_ms_per_key: register_histogram_vec_with_registry!(
                "get_latency_ms_per_key",
                "Latency of fetches from BigTableDB per key",
                Self::LABLES,
                prometheus::exponential_buckets(1.0, 1.6, 24)
                    .unwrap()
                    .to_vec(),
                registry,
            )
            .unwrap(),
            scan_success: register_int_counter_vec_with_registry!(
                "scan_success",
                "Number of successful scans from BigTableDB",
                Self::LABLES,
                registry,
            )
            .unwrap(),
            scan_not_found: register_int_counter_vec_with_registry!(
                "scan_not_found",
                "Number of fetches from BigTableDB that returned not found",
                Self::LABLES,
                registry,
            )
            .unwrap(),
            scan_error: register_int_counter_vec_with_registry!(
                "scan_error",
                "Number of scans from BigTableDB that returned an error",
                Self::LABLES,
                registry,
            )
            .unwrap(),
            scan_latency_ms: register_histogram_vec_with_registry!(
                "scan_latency_ms",
                "Latency of scans from BigTableDB",
                Self::LABLES,
                prometheus::exponential_buckets(1.0, 1.6, 24)
                    .unwrap()
                    .to_vec(),
                registry,
            )
            .unwrap(),
        })
    }
}
