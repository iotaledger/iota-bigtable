// Copyright (c) Mysten Labs, Inc.
// Modifications Copyright (c) 2026 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! A pool of gRPC connections to BigTable.
//!
//! Individual gRPC connections are limited to a maximum number of concurrent
//! streams (typically 100 for Google services). This pool distributes
//! requests round-robin across multiple connections to prevent queueing
//! bottlenecks.

// TODO: The pool is not used by the client yet. Remove once done.
#![allow(dead_code)]

use std::{
    future::Future,
    num::NonZeroUsize,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use arc_swap::ArcSwap;
use bytes::Bytes;
use http::{Request, Response};
use http_body::{Frame, SizeHint};
use prometheus::{
    IntCounter, IntGauge, Registry, register_int_counter_with_registry,
    register_int_gauge_with_registry,
};
use tonic::{
    Status,
    body::Body,
    codegen::Service,
    transport::{Channel, Endpoint},
};

use crate::errors::{BigTableClientError, Result};

/// Warms a freshly connected channel before it enters the pool.
pub(crate) trait ChannelPrimer: Send + Sync + 'static {
    fn prime<'a>(
        &'a self,
        channel: &'a Channel,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
}

/// Sizing parameters of the Connection Pool.
///
/// # Example
///
/// ```rust
/// # use iota_bigtable::PoolConfig;
/// let config = PoolConfig::new().initial_pool_size(4)?.max_pool_size(10)?;
/// # Ok::<(), iota_bigtable::errors::BigTableClientError>(())
/// ```
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Channels opened on connect.
    initial_pool_size: usize,
    /// Lower bound the pool never shrinks below.
    min_pool_size: usize,
    /// Upper bound the pool never grows above.
    max_pool_size: usize,
    /// Timeout for connecting and priming a single channel.
    ///
    /// Covers TCP, TLS, and HTTP/2 handshakes. Because connection and priming
    /// run sequentially, a failed attempt can take up to double this
    /// duration.
    channel_init_timeout: Duration,
}

impl PoolConfig {
    /// Creates a config with production-ready defaults. Use the setters to
    /// override individual values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Represents a single-channel pool with no scaling, for testing or local
    /// use.
    pub fn singleton() -> Self {
        Self {
            initial_pool_size: 1,
            min_pool_size: 1,
            max_pool_size: 1,
            ..Self::default()
        }
    }

    /// Channels opened on connect. Default 10.
    pub fn initial_pool_size(mut self, value: impl TryInto<NonZeroUsize>) -> Result<Self> {
        self.initial_pool_size = non_zero(value, "initial_pool_size")?;
        Ok(self)
    }

    /// Smallest the pool ever shrinks to. Default 1.
    pub fn min_pool_size(mut self, value: impl TryInto<NonZeroUsize>) -> Result<Self> {
        self.min_pool_size = non_zero(value, "min_pool_size")?;
        Ok(self)
    }

    /// Largest the pool ever grows to. Default 200.
    pub fn max_pool_size(mut self, value: impl TryInto<NonZeroUsize>) -> Result<Self> {
        self.max_pool_size = non_zero(value, "max_pool_size")?;
        Ok(self)
    }

    /// Timeout for opening and priming a single channel. Default 10s.
    ///
    /// Covers TCP connect, TLS, and HTTP/2 handshakes.
    ///
    /// # Error
    ///
    /// The [`Duration`] value must be non-zero.
    pub fn channel_init_timeout(mut self, value: Duration) -> Result<Self> {
        if value.is_zero() {
            return Err(BigTableClientError::InvalidPoolConfig(
                "`channel_init_timeout` must be greater than zero".into(),
            ));
        }
        self.channel_init_timeout = value;
        Ok(self)
    }

    /// Rejects field combinations the setters cannot catch on their own,
    /// since they would depend on call order.
    fn validate(&self) -> Result<()> {
        if self.min_pool_size > self.initial_pool_size
            || self.initial_pool_size > self.max_pool_size
        {
            return Err(BigTableClientError::InvalidPoolConfig(
                "`min_pool_size` <= `initial_pool_size` <= `max_pool_size` must hold".into(),
            ));
        }
        Ok(())
    }
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            initial_pool_size: 10,
            min_pool_size: 1,
            max_pool_size: 200,
            channel_init_timeout: Duration::from_secs(10),
        }
    }
}

/// Converts a setter argument to `usize`, rejecting zero with a
/// [`BigTableClientError::InvalidPoolConfig`] naming the field.
fn non_zero(value: impl TryInto<NonZeroUsize>, field: &str) -> Result<usize> {
    value.try_into().map(NonZeroUsize::get).map_err(|_| {
        BigTableClientError::InvalidPoolConfig(format!("`{field}` must be greater than zero"))
    })
}

/// A round-robin gRPC connection pool for BigTable.
///
/// Distributes requests across multiple pooled [`Channel`] instances to prevent
/// BigTable's per-connection concurrency limits (typically 100 concurrent
/// streams) from creating queueing bottlenecks.
pub(crate) struct ChannelPool {
    inner: Arc<ChannelPoolInner>,
    // Stores the entry and a *clone* of its channel after poll_ready. We need
    // the clone because Channel::poll_ready takes &mut self, and
    // entry.channel isn't mutable through Arc.
    reserved: Option<(Arc<PoolEntry>, Channel)>,
}

impl ChannelPool {
    /// Create a pool without connecting.
    pub(crate) fn new(
        endpoint: Endpoint,
        config: PoolConfig,
        primer: Option<Box<dyn ChannelPrimer>>,
        registry: Option<&Registry>,
    ) -> Self {
        Self {
            inner: Arc::new(ChannelPoolInner {
                entries: ArcSwap::from_pointee(Vec::new()),
                ticker: AtomicUsize::new(0),
                endpoint,
                config,
                primer,
                metrics: registry.map(|r| Arc::new(Metrics::new(r))),
            }),
            reserved: None,
        }
    }

    /// Create and connect.
    pub(crate) async fn new_connected(
        endpoint: Endpoint,
        config: PoolConfig,
        primer: Option<Box<dyn ChannelPrimer>>,
        registry: Option<&Registry>,
    ) -> Result<Self> {
        config.validate()?;
        let pool = Self::new(endpoint, config, primer, registry);
        pool.connect().await?;
        Ok(pool)
    }

    /// Connect `initial_pool_size` channels, priming each one.
    pub(crate) async fn connect(&self) -> Result<()> {
        let mut entries = Vec::with_capacity(self.inner.config.initial_pool_size);
        for _ in 0..self.inner.config.initial_pool_size {
            entries.push(Arc::new(self.inner.create_primed_entry().await?));
        }
        if let Some(m) = &self.inner.metrics {
            m.pool_size.set(entries.len() as i64);
        }
        self.inner.entries.store(Arc::new(entries));
        Ok(())
    }
}

impl Clone for ChannelPool {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            reserved: None,
        }
    }
}

impl Drop for ChannelPool {
    fn drop(&mut self) {
        if let Some((entry, _)) = self.reserved.take() {
            entry.in_flight.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

/// Shared inner state for [`ChannelPool`]:
///
/// - **Creation:** Channels are opened and primed via `create_primed_entry`.
/// - **Load balancing:** Round-robin routing with per-channel in-flight
///   tracking.
///
/// # Concurrency
/// Held behind an `Arc` in every handle. `entries` is swapped atomically for
/// lock-free reads.
pub(crate) struct ChannelPoolInner {
    entries: ArcSwap<Vec<Arc<PoolEntry>>>,
    ticker: AtomicUsize,
    endpoint: Endpoint,
    config: PoolConfig,
    primer: Option<Box<dyn ChannelPrimer>>,
    metrics: Option<Arc<Metrics>>,
}

impl ChannelPoolInner {
    /// Opens, primes and wraps a new channel. This is the single place every
    /// pool entry is created.
    ///
    /// Both the connect and the prime are bounded by `channel_init_timeout`.
    /// `Endpoint::timeout` only applies to requests, so without an explicit
    /// bound a stalled handshake would block the caller indefinitely.
    ///
    /// Any failure or timeout is returned to the caller so a channel enters the
    /// pool only once it has proven it can round-trip an RPC, as in the
    /// official Go client.
    async fn create_primed_entry(&self) -> Result<PoolEntry> {
        let timeout = self.config.channel_init_timeout;
        let channel = tokio::time::timeout(timeout, self.endpoint.connect()).await??;
        if let Some(primer) = self.primer.as_deref() {
            tokio::time::timeout(timeout, primer.prime(&channel)).await??;
        }
        Ok(PoolEntry {
            channel,
            in_flight: AtomicUsize::new(0),
            success_count: AtomicUsize::new(0),
            error_count: AtomicUsize::new(0),
        })
    }
}

pub(crate) struct Metrics {
    pub(crate) pool_size: IntGauge,
    pub(crate) rpcs_completed: IntCounter,
}

impl Metrics {
    fn new(registry: &Registry) -> Self {
        Self {
            pool_size: register_int_gauge_with_registry!(
                "bt_pool_pool_size",
                "Current number of channels in the BigTable connection pool",
                registry,
            )
            .unwrap(),
            rpcs_completed: register_int_counter_with_registry!(
                "bt_pool_rpcs_completed",
                "Total RPCs completed through the pool",
                registry,
            )
            .unwrap(),
        }
    }
}

/// Wraps an actual [`Channel`] with additional information allowing the
/// [`ChannelPoolInner`] to manage it.
struct PoolEntry {
    channel: Channel,
    /// Active RPCs on this channel (unary and streaming).
    ///
    /// Incremented in `poll_ready` and decremented when [`InFlightGuard`]
    /// drops. For both unary and streaming calls the guard is released on the
    /// response trailers, the last frame of every gRPC response, or when the
    /// body is dropped before they arrive.
    in_flight: AtomicUsize,
    /// Successful HTTP/2 exchanges. Used in tests to verify round-robin
    /// distribution.
    ///
    /// *Note:* gRPC-level error statuses count as successes here because they
    /// arrive in HTTP/2 trailers. Does not influence channel selection.
    success_count: AtomicUsize,
    /// Transport failures and request timeouts. Does not influence channel
    /// selection.
    error_count: AtomicUsize,
}

/// Decrements `in_flight` when dropped, ensuring the count stays accurate
/// even if the RPC future is cancelled (e.g. client disconnect) or the
/// caller stops reading a streaming response early.
struct InFlightGuard {
    entry: Arc<PoolEntry>,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.entry.in_flight.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Response body that keeps the channel's in-flight guard alive until the
/// response trailers arrive or the body is dropped.
///
/// Because `Channel::call` resolves as soon as headers arrive, streaming RPCs
/// (such as `ReadRows`) would otherwise drop the guard prematurely while the
/// HTTP/2 stream is still active. Tying the guard to the body ensures
/// `in_flight` reflects open streams rather than just header delivery.
struct GuardedBody {
    inner: Body,
    /// Released when the inner body ends, or when the body is dropped,
    /// whichever comes first.
    guard: Option<InFlightGuard>,
}

// Forwards every call to the wrapped body and releases the guard on the
// first frame that ends the response like trailers, an error or end of stream.
impl http_body::Body for GuardedBody {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<std::result::Result<Frame<Self::Data>, Self::Error>>> {
        let frame = std::task::ready!(Pin::new(&mut self.inner).poll_frame(cx));
        // Trailers are the final frame of a gRPC response and carry the
        // HTTP/2 end-of-stream flag. Tonic stops polling after them.
        let ended = match &frame {
            Some(Ok(frame)) => frame.is_trailers(),
            Some(Err(_)) | None => true,
        };
        if ended {
            self.guard.take();
        }
        Poll::Ready(frame)
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

// Tower `Service` implementation that routes requests across the pooled
// channels.
//
// `poll_ready` reuses an already reserved channel or picks the next one in
// round-robin order, bumps its `in_flight` count and polls it for readiness. A
// readiness error rolls the count back, records the error and clears the
// reservation.
//
// `call` sends the request on the reserved channel, updates the success and
// error counters, and wraps the response body in a `GuardedBody` so
// `in_flight` stays up until the response is fully consumed or dropped.
impl Service<Request<Body>> for ChannelPool {
    type Response = Response<Body>;
    type Error = BigTableClientError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        if let Some((entry, channel)) = &mut self.reserved {
            return match channel.poll_ready(cx) {
                Poll::Ready(Err(e)) => {
                    // Same bookkeeping as a failed first selection, and clear
                    // the reservation so the next RPC picks a fresh channel.
                    entry.in_flight.fetch_sub(1, Ordering::Relaxed);
                    entry.error_count.fetch_add(1, Ordering::Relaxed);
                    self.reserved = None;
                    Poll::Ready(Err(e.into()))
                }
                other => other.map_err(Into::into),
            };
        }

        let entries = self.inner.entries.load();
        if entries.is_empty() {
            // Unavailable is transient, so the client's backoff retries it.
            return Poll::Ready(Err(Status::unavailable("no channels available").into()));
        }
        let idx = self.inner.ticker.fetch_add(1, Ordering::Relaxed) % entries.len();
        let entry = &entries[idx];
        let mut channel = entry.channel.clone();
        entry.in_flight.fetch_add(1, Ordering::Relaxed);
        match channel.poll_ready(cx) {
            Poll::Ready(Ok(())) => {
                self.reserved = Some((entry.clone(), channel));
                Poll::Ready(Ok(()))
            }
            Poll::Pending => {
                self.reserved = Some((entry.clone(), channel));
                Poll::Pending
            }
            Poll::Ready(Err(e)) => {
                entry.in_flight.fetch_sub(1, Ordering::Relaxed);
                entry.error_count.fetch_add(1, Ordering::Relaxed);
                Poll::Ready(Err(e.into()))
            }
        }
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        let (entry, mut channel) = self
            .reserved
            .take()
            .expect("poll_ready must be called before call");
        let metrics = self.inner.metrics.clone();

        Box::pin(async move {
            let guard = InFlightGuard {
                entry: entry.clone(),
            };
            let result = channel.call(request).await;
            if let Some(m) = &metrics {
                m.rpcs_completed.inc();
            }
            match result {
                Ok(response) => {
                    entry.success_count.fetch_add(1, Ordering::Relaxed);
                    // Move the guard into the body so `in_flight` stays
                    // accurate for the lifetime of the HTTP/2 stream, not
                    // just until the headers arrive.
                    Ok(response.map(|inner| {
                        Body::new(GuardedBody {
                            inner,
                            guard: Some(guard),
                        })
                    }))
                }
                Err(e) => {
                    entry.error_count.fetch_add(1, Ordering::Relaxed);
                    Err(e.into())
                }
            }
        })
    }
}
