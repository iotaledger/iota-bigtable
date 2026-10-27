// Copyright (c) Mysten Labs, Inc.
// Modifications Copyright (c) 2026 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! An adaptive pool of gRPC connections to BigTable.
//!
//! Individual gRPC connections are limited to a maximum number of concurrent
//! streams (typically 100 for Google services). This pool automatically
//! distributes requests across multiple connections to prevent queueing
//! bottlenecks.
//!
//! # Key Features
//! - **Dynamic Scaling:** Opens additional connections under high load and
//!   prunes idle ones.
//! - **Lifecycle Management:** Gracefully recycles connections every ~45
//!   minutes to prevent failures caused by BigTable's server-side connection
//!   termination policy (roughly 1 hour).

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
    time::{Duration, Instant},
};

use arc_swap::ArcSwap;
use bytes::Bytes;
use http::{Request, Response};
use http_body::{Frame, SizeHint};
use prometheus::{
    IntCounter, IntGauge, Registry, register_int_counter_with_registry,
    register_int_gauge_with_registry,
};
use rand::Rng;
use tonic::{
    Status,
    body::Body,
    codegen::Service,
    transport::{Channel, Endpoint},
};
use tracing::{debug, info, warn};

use crate::errors::{BigTableClientError, Result};

/// Maximum number of expired connections to be replaced at once.
const MAX_REPLACEMENTS_PER_CYCLE: usize = 2;

/// Warms a freshly connected channel before it enters the pool.
pub(crate) trait ChannelPrimer: Send + Sync + 'static {
    fn prime<'a>(
        &'a self,
        channel: &'a Channel,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
}

/// Sizing and maintenance parameters for the connection pool.
///
/// # Resizing
/// Evaluated every `maintenance_interval` based on average per-channel load:
/// - **Scale-up:** Grows the pool when load reaches or exceeds
///   `max_rpcs_per_channel`.
/// - **Scale-down:** Shrinks the pool (by up to `max_resize_delta` channels) if
///   load stays at or below `min_rpcs_per_channel` for `downscale_threshold`
///   consecutive cycles.
/// - **Bounds:** The pool size is always constrained between `min_pool_size`
///   and `max_pool_size`.
///
/// # Refresh
/// Channels older than `refresh_age` plus a random share of `refresh_jitter`
/// are replaced incrementally (up to two per cycle) to prevent server-side
/// termination after roughly one hour.
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
    /// Average in-flight RPCs per channel below which the pool shrinks.
    min_rpcs_per_channel: usize,
    /// Average in-flight RPCs per channel at or above which the pool grows.
    max_rpcs_per_channel: usize,
    /// Maximum channels removed in one shrink step.
    max_resize_delta: usize,
    /// Consecutive low-load cycles required before shrinking.
    downscale_threshold: usize,
    /// Period of the background refresh/resize loop.
    maintenance_interval: Duration,
    /// Timeout for connecting and priming a single channel.
    ///
    /// Covers TCP, TLS, and HTTP/2 handshakes. Because connection and priming
    /// run sequentially, a failed attempt can take up to double this
    /// duration. Kept short relative to `maintenance_interval` so
    /// connection delays never stall background maintenance tasks.
    channel_init_timeout: Duration,
    /// Age after which a channel is replaced.
    refresh_age: Duration,
    /// Random extra delay added to `refresh_age` per channel.
    refresh_jitter: Duration,
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

    /// Smallest the pool ever shrinks to. Default 10.
    pub fn min_pool_size(mut self, value: impl TryInto<NonZeroUsize>) -> Result<Self> {
        self.min_pool_size = non_zero(value, "min_pool_size")?;
        Ok(self)
    }

    /// Largest the pool ever grows to. Default 200.
    pub fn max_pool_size(mut self, value: impl TryInto<NonZeroUsize>) -> Result<Self> {
        self.max_pool_size = non_zero(value, "max_pool_size")?;
        Ok(self)
    }

    /// Average open RPCs per channel at or below which the pool is oversized.
    /// Default 5. Zero means only a fully idle pool shrinks.
    pub fn min_rpcs_per_channel(mut self, value: usize) -> Self {
        self.min_rpcs_per_channel = value;
        self
    }

    /// Average open RPCs per channel at or above which the pool grows.
    /// Default 50. BigTable allows about 100 concurrent streams per connection.
    pub fn max_rpcs_per_channel(mut self, value: impl TryInto<NonZeroUsize>) -> Result<Self> {
        self.max_rpcs_per_channel = non_zero(value, "max_rpcs_per_channel")?;
        Ok(self)
    }

    /// Maximum channels removed in one shrink step. Default 2.
    pub fn max_resize_delta(mut self, value: impl TryInto<NonZeroUsize>) -> Result<Self> {
        self.max_resize_delta = non_zero(value, "max_resize_delta")?;
        Ok(self)
    }

    /// Shrinks only after this many consecutive `maintenance_interval`s with
    /// load at or below `min_rpcs_per_channel`. Default 3.
    pub fn downscale_threshold(mut self, value: impl TryInto<NonZeroUsize>) -> Result<Self> {
        self.downscale_threshold = non_zero(value, "downscale_threshold")?;
        Ok(self)
    }

    /// How often the pool measures load and refreshes channels. Default 60s.
    /// A zero duration is rejected.
    pub fn maintenance_interval(mut self, value: Duration) -> Result<Self> {
        if value.is_zero() {
            return Err(BigTableClientError::InvalidPoolConfig(
                "`maintenance_interval` must be greater than zero".into(),
            ));
        }
        self.maintenance_interval = value;
        Ok(self)
    }

    /// Timeout for opening and priming a single channel. Default 10s.
    ///
    /// Covers TCP connect, TLS, and HTTP/2 handshakes.
    ///
    /// # Error
    ///
    /// The [`Duration`] value must be non-zero.
    ///
    /// # Sizing
    /// Keep this value significantly below `maintenance_interval`.
    ///
    /// Because channel creation is sequential and a failed attempt can take
    /// up to 2× this timeout (connect + prime), keeping this limit low ensures
    /// multiple retries can complete per cycle without delaying age-based
    /// refreshes.
    pub fn channel_init_timeout(mut self, value: Duration) -> Result<Self> {
        if value.is_zero() {
            return Err(BigTableClientError::InvalidPoolConfig(
                "`channel_init_timeout` must be greater than zero".into(),
            ));
        }
        self.channel_init_timeout = value;
        Ok(self)
    }

    /// Age at which a channel is replaced. Default 45 minutes.
    ///
    /// A zero duration causes constant reconnection and is only useful in
    /// tests. Keep this well above the maintenance interval in production.
    pub fn refresh_age(mut self, value: Duration) -> Self {
        self.refresh_age = value;
        self
    }

    /// Random extra age added to `refresh_age` per channel. Default 5 minutes.
    /// Zero disables jitter.
    pub fn refresh_jitter(mut self, value: Duration) -> Self {
        self.refresh_jitter = value;
        self
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
        if self.min_rpcs_per_channel >= self.max_rpcs_per_channel {
            return Err(BigTableClientError::InvalidPoolConfig(
                "`min_rpcs_per_channel` must be less than `max_rpcs_per_channel`".into(),
            ));
        }
        Ok(())
    }
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            initial_pool_size: 10,
            min_pool_size: 10,
            max_pool_size: 200,
            min_rpcs_per_channel: 5,
            max_rpcs_per_channel: 50,
            max_resize_delta: 2,
            downscale_threshold: 3,
            maintenance_interval: Duration::from_secs(60),
            channel_init_timeout: Duration::from_secs(10),
            // GFE forcibly disconnects channels after ~60 minutes for load
            // balancing. Refresh at 45m + up to 5m jitter to replace channels
            // before reaping.
            refresh_age: Duration::from_secs(45 * 60),
            refresh_jitter: Duration::from_secs(5 * 60),
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
/// streams) from creating queueing bottlenecks. A background task resizes
/// the pool with load and replaces channels before BigTable closes them.
pub(crate) struct ChannelPool {
    inner: Arc<ChannelPoolInner>,
    // Stores the entry and a *clone* of its channel after poll_ready. We need
    // the clone because Channel::poll_ready takes &mut self, and
    // entry.channel isn't mutable through Arc.
    reserved: Option<(Arc<PoolEntry>, Channel)>,
}

impl ChannelPool {
    /// Create a pool without connecting or spawning background tasks.
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
                consecutive_low_load: AtomicUsize::new(0),
                endpoint,
                config,
                primer,
                metrics: registry.map(|r| Arc::new(Metrics::new(r))),
            }),
            reserved: None,
        }
    }

    /// Create, connect, and spawn background maintenance tasks.
    pub(crate) async fn new_connected(
        endpoint: Endpoint,
        config: PoolConfig,
        primer: Option<Box<dyn ChannelPrimer>>,
        registry: Option<&Registry>,
    ) -> Result<Self> {
        config.validate()?;
        let pool = Self::new(endpoint, config, primer, registry);
        pool.connect().await?;
        pool.spawn_background_tasks();
        Ok(pool)
    }

    /// Connect `initial_pool_size` channels, priming each one.
    pub(crate) async fn connect(&self) -> Result<()> {
        let start = Instant::now();
        let mut entries = Vec::with_capacity(self.inner.config.initial_pool_size);
        for _ in 0..self.inner.config.initial_pool_size {
            entries.push(Arc::new(self.inner.create_primed_entry().await?));
        }
        info!(
            channels = entries.len(),
            elapsed = ?start.elapsed(),
            "bigtable channel pool connected"
        );
        if let Some(m) = &self.inner.metrics {
            m.pool_size.set(entries.len() as i64);
        }
        self.inner.entries.store(Arc::new(entries));
        Ok(())
    }

    /// Spawn the background maintenance loop that periodically refreshes
    /// aged-out channels and resizes the pool based on load.
    fn spawn_background_tasks(&self) {
        let weak = Arc::downgrade(&self.inner);
        let period = self.inner.config.maintenance_interval;
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(period);
            // A slow cycle must not be followed by a burst of catch-up cycles.
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            interval.tick().await; // first tick completes immediately
            loop {
                interval.tick().await;
                let Some(inner) = weak.upgrade() else {
                    return;
                };
                // Resize the pool before refresh. A replaced channel leaves the
                // list while its RPCs are still running, so
                // sampling the load after the replacement would
                // undercount it.
                inner.resize().await;
                inner.refresh().await;
            }
        });
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

/// Shared inner state for [`ChannelPool`], managed by a background maintenance
/// loop:
///
/// - **Creation:** Channels are opened and primed via `create_primed_entry`.
/// - **Load balancing:** Round-robin routing with per-channel in-flight
///   tracking.
/// - **Resizing:** Dynamically scales up/down based on average in-flight load
///   thresholds.
/// - **Replacement:** Rotates aging channels incrementally to avoid server-side
///   timeouts.
///
/// # Concurrency
/// Held behind an `Arc` in every handle. `entries` is swapped atomically for
/// lock-free reads, and the maintenance loop holds a `Weak` reference,
/// terminating when all handles drop.
pub(crate) struct ChannelPoolInner {
    entries: ArcSwap<Vec<Arc<PoolEntry>>>,
    ticker: AtomicUsize,
    consecutive_low_load: AtomicUsize,
    endpoint: Endpoint,
    config: PoolConfig,
    primer: Option<Box<dyn ChannelPrimer>>,
    metrics: Option<Arc<Metrics>>,
}

impl ChannelPoolInner {
    /// Swap out connections that are too old. At most
    /// `MAX_REPLACEMENTS_PER_CYCLE` per run, so we don't
    /// reconnect everything at once. Also logs and resets each connection's
    /// success and error counts.
    async fn refresh(&self) {
        let snapshot = self.entries.load();
        let mut new_vec: Vec<Arc<PoolEntry>> = (**snapshot).clone();
        let now = Instant::now();
        let mut replacements = 0;

        for (idx, entry) in snapshot.iter().enumerate() {
            let successes = entry.success_count.swap(0, Ordering::Relaxed);
            let errors = entry.error_count.swap(0, Ordering::Relaxed);
            if errors > 0 {
                warn!(
                    channel_idx = idx,
                    errors, successes, "channel transport errors since last maintenance cycle"
                );
            }

            if replacements >= MAX_REPLACEMENTS_PER_CYCLE {
                continue;
            }

            if now < entry.refresh_at {
                continue;
            }

            match self.create_primed_entry().await {
                Ok(new_entry) => {
                    info!(channel_idx = idx, "replacing bigtable channel");
                    new_vec[idx] = Arc::new(new_entry);
                    replacements += 1;
                }
                Err(e) => {
                    warn!(channel_idx = idx, error = %e, "failed to create replacement channel");
                }
            }
        }

        if replacements > 0 {
            self.entries.store(Arc::new(new_vec));
            if let Some(m) = &self.metrics {
                m.channels_replaced.inc_by(replacements as u64);
            }
        }
    }

    /// Scale the pool up or down based on current per-channel load. Uses a
    /// point-in-time sampling approach inherited from the official Google
    /// Cloud Go client's channel pool implementation.
    async fn resize(&self) {
        let entries = self.entries.load();
        let current_size = entries.len();
        if current_size == 0 {
            return;
        }

        let total_in_flight: usize = entries
            .iter()
            .map(|e| e.in_flight.load(Ordering::Relaxed))
            .sum();

        let load = total_in_flight as f64 / current_size as f64;
        let target_load = (self.config.min_rpcs_per_channel as f64
            + self.config.max_rpcs_per_channel as f64)
            / 2.0;

        if load <= self.config.min_rpcs_per_channel as f64 {
            // Ideal pool size to bring per-channel load back to target_load.
            // We don't jump straight to this size — delta is capped by
            // max_resize_delta to avoid removing too many channels at once
            // and causing a load spike.
            let pool_size_target = (total_in_flight as f64 / target_load.max(1.0)).ceil() as usize;
            let pool_size_target = pool_size_target.max(self.config.min_pool_size);
            let delta =
                (current_size.saturating_sub(pool_size_target)).min(self.config.max_resize_delta);
            if delta == 0 {
                self.consecutive_low_load.store(0, Ordering::Relaxed);
                return;
            }

            // Scale down: load is too low. Require consecutive low-load
            // observations before actually shrinking, to avoid flapping
            // (mirrors official Google Go client continuousDownscaleRuns
            // logic).
            let runs = self.consecutive_low_load.fetch_add(1, Ordering::Relaxed) + 1;
            if runs < self.config.downscale_threshold {
                return;
            }

            let mut new_entries = (**entries).clone();
            let new_len = current_size
                .saturating_sub(delta)
                .max(self.config.min_pool_size);
            new_entries.truncate(new_len);
            info!(
                current_size,
                new_len, total_in_flight, "pool resize: shrunk"
            );
            self.entries.store(Arc::new(new_entries));
            self.consecutive_low_load.store(0, Ordering::Relaxed);
            if let Some(m) = &self.metrics {
                m.pool_size.set(new_len as i64);
            }
        } else {
            // Load is above the low threshold — reset the downscale streak.
            self.consecutive_low_load.store(0, Ordering::Relaxed);

            if load >= self.config.max_rpcs_per_channel as f64 {
                // Scale up: load is too high.
                let pool_size_target = ((total_in_flight as f64 / target_load.max(1.0)).ceil()
                    as usize)
                    .min(self.config.max_pool_size);
                let delta = pool_size_target.saturating_sub(current_size);
                if delta == 0 {
                    return;
                }

                let mut new_entries = (**entries).clone();
                for _ in 0..delta {
                    match self.create_primed_entry().await {
                        Ok(entry) => new_entries.push(Arc::new(entry)),
                        Err(e) => {
                            warn!(error = %e, "failed to create channel during pool expansion");
                            break;
                        }
                    }
                }
                let new_size = new_entries.len();
                if new_size == current_size {
                    return;
                }
                info!(
                    current_size,
                    new_size, total_in_flight, "pool resize: expanded"
                );
                self.entries.store(Arc::new(new_entries));
                if let Some(m) = &self.metrics {
                    m.pool_size.set(new_size as i64);
                }
            }
        }
    }

    /// Computes the deadline at which a channel should be replaced.
    fn compute_refresh_at(refresh_age: Duration, refresh_jitter: Duration) -> Instant {
        let jitter = if refresh_jitter.is_zero() {
            Duration::ZERO
        } else {
            Duration::from_nanos(rand::thread_rng().gen_range(0..refresh_jitter.as_nanos() as u64))
        };
        Instant::now() + refresh_age + jitter
    }

    /// Opens, primes and wraps a new channel. This is the single place every
    /// pool entry is created, whether on connect, age refresh or scale-up.
    ///
    /// Both the connect and the prime are bounded by `channel_init_timeout`.
    /// `Endpoint::timeout` only applies to requests, so without an explicit
    /// bound a stalled handshake would block the maintenance loop.
    ///
    /// Any failure or timeout is returned to the caller so a channel enters the
    /// pool only once it has proven it can round-trip an RPC, as in the
    /// official Go client.
    async fn create_primed_entry(&self) -> Result<PoolEntry> {
        let timeout = self.config.channel_init_timeout;
        let start = Instant::now();
        let channel = tokio::time::timeout(timeout, self.endpoint.connect()).await??;
        debug!(elapsed = ?start.elapsed(), "bigtable channel connected");
        if let Some(primer) = self.primer.as_deref() {
            let start = Instant::now();
            tokio::time::timeout(timeout, primer.prime(&channel)).await??;
            debug!(elapsed = ?start.elapsed(), "bigtable channel primed");
        }
        Ok(PoolEntry {
            channel,
            refresh_at: Self::compute_refresh_at(
                self.config.refresh_age,
                self.config.refresh_jitter,
            ),
            in_flight: AtomicUsize::new(0),
            success_count: AtomicUsize::new(0),
            error_count: AtomicUsize::new(0),
        })
    }
}

pub(crate) struct Metrics {
    pub(crate) pool_size: IntGauge,
    pub(crate) channels_replaced: IntCounter,
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
            channels_replaced: register_int_counter_with_registry!(
                "bt_pool_channels_replaced",
                "Total channels replaced due to age refresh",
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
    refresh_at: Instant,
    /// Active RPCs on this channel (unary and streaming).
    ///
    /// Incremented in `poll_ready` and decremented when [`InFlightGuard`]
    /// drops. For both unary and streaming calls the guard is released on the
    /// response trailers, the last frame of every gRPC response, or when the
    /// body is dropped before they arrive.
    in_flight: AtomicUsize,
    /// Successful HTTP/2 exchanges this cycle. Used in tests to verify
    /// round-robin distribution.
    ///
    /// *Note:* gRPC-level error statuses count as successes here because they
    /// arrive in HTTP/2 trailers. Reset each maintenance cycle. Does not
    /// influence channel selection or replacement.
    success_count: AtomicUsize,
    /// Transport failures and request timeouts this cycle (triggers warning
    /// logs).
    ///
    /// Reset each maintenance cycle. Does not influence channel selection or
    /// replacement.
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
            match channel.call(request).await {
                Ok(response) => {
                    entry.success_count.fetch_add(1, Ordering::Relaxed);
                    if let Some(m) = &metrics {
                        m.rpcs_completed.inc();
                    }
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
