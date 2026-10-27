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
use tonic::{
    Status,
    body::Body,
    codegen::Service,
    transport::{Channel, Endpoint},
};
use tracing::{debug, info, warn};

use crate::errors::{BigTableClientError, Result};

/// Fraction of the pool that may be replaced in one maintenance cycle, as a
/// divisor.
const MAX_REPLACEMENTS_PER_CYCLE_DIVISOR: usize = 5;
/// Minimum connection replacements per maintenance cycle.
const MIN_REPLACEMENTS_PER_CYCLE: usize = 2;

/// Runs a first RPC on a freshly connected channel before it enters the
/// pool, so the first real request does not pay the connection setup cost.
///
/// The name follows the official Go client, whose `ChannelPrimer` issues a
/// `PingAndWarm` for the same purpose.
pub(crate) trait ChannelPrimer: Send + Sync + 'static {
    /// Primes `channel`. Errors and timeouts discard the channel.
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
/// - **Scale-down:** Shrinks the pool (by up to `max_removed_channels`
///   channels) if load stays at or below `min_rpcs_per_channel` for
///   `max_maintenance_intervals` consecutive cycles.
/// - **Bounds:** The pool size is always constrained between `min_pool_size`
///   and `max_pool_size`.
///
/// # Refresh
/// Channels older than `refresh_age` are replaced incrementally (up to two per
/// cycle) to prevent server-side termination after roughly one hour.
///
/// # Example
///
/// ```rust
/// # use iota_bigtable::PoolConfig;
/// let config = PoolConfig::new()
///     .min_pool_size(2)?
///     .initial_pool_size(4)?
///     .max_pool_size(10)?;
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
    max_removed_channels: usize,
    /// Consecutive low-load cycles required before shrinking.
    max_maintenance_intervals: usize,
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
}

impl PoolConfig {
    /// Creates a config with production-ready defaults. Use the setters to
    /// override individual values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a single-channel config with no scaling, for testing or local
    /// use.
    pub fn singleton() -> Self {
        Self {
            initial_pool_size: 1,
            min_pool_size: 1,
            max_pool_size: 1,
            ..Self::default()
        }
    }

    /// Sets the number of channels opened on connect. Default 10.
    pub fn initial_pool_size(mut self, value: impl TryInto<NonZeroUsize>) -> Result<Self> {
        self.initial_pool_size = non_zero(value, "initial_pool_size")?;
        Ok(self)
    }

    /// Sets the smallest size the pool ever shrinks to. Default 10.
    pub fn min_pool_size(mut self, value: impl TryInto<NonZeroUsize>) -> Result<Self> {
        self.min_pool_size = non_zero(value, "min_pool_size")?;
        Ok(self)
    }

    /// Sets the largest size the pool ever grows to. Default 200.
    pub fn max_pool_size(mut self, value: impl TryInto<NonZeroUsize>) -> Result<Self> {
        self.max_pool_size = non_zero(value, "max_pool_size")?;
        Ok(self)
    }

    /// Sets the average open RPCs per channel at or below which the pool is
    /// oversized. Default 5. Zero means only a fully idle pool shrinks.
    pub fn min_rpcs_per_channel(mut self, value: usize) -> Self {
        self.min_rpcs_per_channel = value;
        self
    }

    /// Sets the average open RPCs per channel at or above which the pool grows.
    /// Default 50. BigTable allows about 100 concurrent streams per connection.
    pub fn max_rpcs_per_channel(mut self, value: impl TryInto<NonZeroUsize>) -> Result<Self> {
        self.max_rpcs_per_channel = non_zero(value, "max_rpcs_per_channel")?;
        Ok(self)
    }

    /// Sets the maximum channels removed in one shrink step. Default 2.
    pub fn max_removed_channels(mut self, value: impl TryInto<NonZeroUsize>) -> Result<Self> {
        self.max_removed_channels = non_zero(value, "max_removed_channels")?;
        Ok(self)
    }

    /// Sets how many consecutive `maintenance_interval`s with load at or below
    /// `min_rpcs_per_channel` must pass before the pool shrinks. Default 3.
    pub fn max_maintenance_intervals(mut self, value: impl TryInto<NonZeroUsize>) -> Result<Self> {
        self.max_maintenance_intervals = non_zero(value, "max_maintenance_intervals")?;
        Ok(self)
    }

    /// Sets how often the pool measures load and refreshes channels. Default
    /// 60s. A zero duration is rejected.
    pub fn maintenance_interval(mut self, value: Duration) -> Result<Self> {
        if value.is_zero() {
            return Err(BigTableClientError::InvalidPoolConfig(
                "`maintenance_interval` must be greater than zero".into(),
            ));
        }
        self.maintenance_interval = value;
        Ok(self)
    }

    /// Sets the timeout for opening and priming a single channel. Default 10s.
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

    /// Sets the age at which a channel is replaced. Default 45 minutes.
    ///
    /// A zero duration causes constant reconnection and is only useful in
    /// tests. Keep this well above the maintenance interval in production.
    pub fn refresh_age(mut self, value: Duration) -> Self {
        self.refresh_age = value;
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
            max_removed_channels: 2,
            max_maintenance_intervals: 3,
            maintenance_interval: Duration::from_secs(60),
            channel_init_timeout: Duration::from_secs(10),
            // GFE forcibly disconnects channels after ~60 minutes for load
            // balancing. Refresh at 45m to replace channels before reaping.
            refresh_age: Duration::from_secs(45 * 60),
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
    reserved: Option<ReservedChannel>,
}

impl ChannelPool {
    /// Creates an empty connection pool with no active connections yet.
    ///
    /// Useful for tests that call `refresh` and `resize` manually.
    pub(crate) fn new(
        endpoint: Endpoint,
        config: PoolConfig,
        primer: Option<Box<dyn ChannelPrimer>>,
        registry: Option<&Registry>,
    ) -> Result<Self> {
        config.validate()?;
        Ok(Self {
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
        })
    }

    /// Starts the connection pool with its connections ready for use.
    ///
    /// Opens and primes `initial_pool_size` channels, then starts the
    /// maintenance task responsible for refreshing and resizing them.
    pub(crate) async fn start(
        endpoint: Endpoint,
        config: PoolConfig,
        primer: Option<Box<dyn ChannelPrimer>>,
        registry: Option<&Registry>,
    ) -> Result<Self> {
        let pool = Self::new(endpoint, config, primer, registry)?;
        pool.connect().await?;
        pool.spawn_maintenance_task();
        Ok(pool)
    }

    /// Connects `initial_pool_size` channels, priming each one.
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

    /// Spawns the background maintenance loop that periodically refreshes
    /// aged-out channels and resizes the pool based on load.
    fn spawn_maintenance_task(&self) {
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
    /// Selects the entry for the next RPC in round-robin order. Fails when the
    /// pool holds no channels.
    fn select_next(&self) -> Result<Arc<PoolEntry>> {
        let entries = self.entries.load();
        if entries.is_empty() {
            // Unavailable is transient, so the client's backoff retries it.
            return Err(Status::unavailable("no channels available").into());
        }
        let idx = self.ticker.fetch_add(1, Ordering::Relaxed) % entries.len();
        Ok(entries[idx].clone())
    }

    /// Swaps out connections that are too old, at most a fifth of the pool
    /// per run so we don't reconnect everything at once. Also logs and resets
    /// each connection's success and error counts.
    async fn refresh(self: &Arc<Self>) {
        let snapshot = self.entries.load();
        let now = Instant::now();
        let max_replacements =
            (snapshot.len() / MAX_REPLACEMENTS_PER_CYCLE_DIVISOR).max(MIN_REPLACEMENTS_PER_CYCLE);
        let mut tasks = tokio::task::JoinSet::new();

        for (idx, entry) in snapshot.iter().enumerate() {
            let successes = entry.success_count.swap(0, Ordering::Relaxed);
            let errors = entry.error_count.swap(0, Ordering::Relaxed);
            if errors > 0 {
                warn!(
                    channel_idx = idx,
                    errors, successes, "channel transport errors since last maintenance cycle"
                );
            }

            if tasks.len() >= max_replacements || now < entry.refresh_at {
                continue;
            }
            let inner = Arc::clone(self);
            tasks.spawn(async move { (idx, inner.create_primed_entry().await) });
        }

        let mut new_vec: Vec<Arc<PoolEntry>> = (**snapshot).clone();
        let mut replacements = 0;
        while let Some(joined) = tasks.join_next().await {
            match joined {
                Ok((idx, Ok(new_entry))) => {
                    info!(channel_idx = idx, "replacing bigtable channel");
                    new_vec[idx] = Arc::new(new_entry);
                    replacements += 1;
                }
                Ok((idx, Err(e))) => {
                    warn!(channel_idx = idx, error = %e, "failed to create replacement channel")
                }
                Err(e) => warn!(error = %e, "replacement task failed"),
            }
        }

        if replacements > 0 {
            self.entries.store(Arc::new(new_vec));
            if let Some(m) = &self.metrics {
                m.channels_replaced.inc_by(replacements as u64);
            }
        }
    }

    /// Scales the pool up or down based on current per-channel load. Uses a
    /// point-in-time sampling approach inherited from the official Google
    /// Cloud Go client's channel pool implementation.
    async fn resize(self: &Arc<Self>) {
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
            self.scale_down(&entries, total_in_flight, target_load);
        } else {
            // Load is above the low threshold, reset the downscale streak.
            self.consecutive_low_load.store(0, Ordering::Relaxed);
            if load >= self.config.max_rpcs_per_channel as f64 {
                self.scale_up(&entries, total_in_flight, target_load).await;
            }
        }
    }

    /// Removes up to `max_removed_channels` channels once load has stayed low
    /// for `max_maintenance_intervals` cycles.
    fn scale_down(&self, entries: &[Arc<PoolEntry>], total_in_flight: usize, target_load: f64) {
        let current_size = entries.len();
        // Ideal pool size to bring per-channel load back to target_load.
        // We don't jump straight to this size — delta is capped by
        // max_removed_channels to avoid removing too many channels at once
        // and causing a load spike.
        let pool_size_target = (total_in_flight as f64 / target_load.max(1.0)).ceil() as usize;
        let pool_size_target = pool_size_target.max(self.config.min_pool_size);
        let delta =
            (current_size.saturating_sub(pool_size_target)).min(self.config.max_removed_channels);
        if delta == 0 {
            self.consecutive_low_load.store(0, Ordering::Relaxed);
            return;
        }

        // Scale down: load is too low. Require consecutive low-load
        // observations before actually shrinking, to avoid flapping
        // (mirrors official Google Go client continuousDownscaleRuns
        // logic).
        let runs = self.consecutive_low_load.fetch_add(1, Ordering::Relaxed) + 1;
        if runs < self.config.max_maintenance_intervals {
            return;
        }

        let mut new_entries = entries.to_vec();
        let new_len = current_size
            .saturating_sub(delta)
            .max(self.config.min_pool_size);
        // Drop the channels closest to their refresh deadline so refresh
        // has less to replace later.
        new_entries.sort_by_key(|e| std::cmp::Reverse(e.refresh_at));
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
    }

    /// Opens channels to bring per-channel load back to `target_load`, up to
    /// `max_pool_size`.
    async fn scale_up(
        self: &Arc<Self>,
        entries: &[Arc<PoolEntry>],
        total_in_flight: usize,
        target_load: f64,
    ) {
        let current_size = entries.len();
        let pool_size_target = ((total_in_flight as f64 / target_load.max(1.0)).ceil() as usize)
            .min(self.config.max_pool_size);
        let delta = pool_size_target.saturating_sub(current_size);
        if delta == 0 {
            return;
        }

        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..delta {
            let inner = Arc::clone(self);
            tasks.spawn(async move { inner.create_primed_entry().await });
        }
        let mut new_entries = entries.to_vec();
        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(Ok(entry)) => new_entries.push(Arc::new(entry)),
                Ok(Err(e)) => warn!(error = %e, "failed to create channel during pool expansion"),
                Err(e) => warn!(error = %e, "channel creation task failed"),
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
        let channel = tokio::time::timeout(timeout, self.endpoint.connect())
            .await
            .map_err(|_| BigTableClientError::ChannelInitTimeout)??;
        debug!(elapsed = ?start.elapsed(), "bigtable channel connected");
        if let Some(primer) = self.primer.as_deref() {
            let start = Instant::now();
            tokio::time::timeout(timeout, primer.prime(&channel))
                .await
                .map_err(|_| BigTableClientError::ChannelInitTimeout)??;
            debug!(elapsed = ?start.elapsed(), "bigtable channel primed");
        }
        Ok(PoolEntry {
            channel,
            refresh_at: Instant::now() + self.config.refresh_age,
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

impl InFlightGuard {
    fn new(entry: Arc<PoolEntry>) -> Self {
        entry.in_flight.fetch_add(1, Ordering::Relaxed);
        Self { entry }
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.entry.in_flight.fetch_sub(1, Ordering::Relaxed);
    }
}

/// A [`PoolEntry`] channel reserved for one RPC by
/// [`ChannelPool::poll_ready`] and held until [`ChannelPool::call`] uses it.
///
/// Each entry holds a single HTTP/2 connection shared by many concurrent
/// RPCs, so the pool cannot hand out exclusive channels. Instead `poll_ready`
/// picks an entry, counts the RPC in its `in_flight`, and keeps the channel it
/// checked until `call` sends on it. The guard releases the count when the
/// response body ends or the reservation is dropped.
struct ReservedChannel {
    /// Counts this RPC in the entry's `in_flight` until the response body ends.
    guard: InFlightGuard,
    /// A clone of the inner guard channel.
    ///
    /// This must be the exact instance advanced by `poll_ready` and passed to
    /// `call`. Because a tonic `Channel` wraps a tower `Buffer`,
    /// `poll_ready` transitions it from `Idle` to `ReadyToSend` by
    /// reserving a queue slot. A fresh clone would stay `Idle` and panic in
    /// `call`.
    channel: Channel,
}

impl ReservedChannel {
    fn new(entry: Arc<PoolEntry>) -> Self {
        Self {
            channel: entry.channel.clone(),
            guard: InFlightGuard::new(entry),
        }
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
    /// Counts the RPC as completed when the body ends.
    metrics: Option<Arc<Metrics>>,
}

impl GuardedBody {
    fn new(inner: Body, guard: InFlightGuard, metrics: impl Into<Option<Arc<Metrics>>>) -> Self {
        Self {
            inner,
            guard: Some(guard),
            metrics: metrics.into(),
        }
    }
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
            if let Some(m) = &self.metrics {
                m.rpcs_completed.inc();
            }
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
        if let Some(reservation) = &mut self.reserved {
            return match reservation.channel.poll_ready(cx) {
                Poll::Ready(Err(e)) => {
                    // Same bookkeeping as a failed first selection, and clear
                    // the reservation so the next RPC picks a fresh channel.
                    reservation
                        .guard
                        .entry
                        .error_count
                        .fetch_add(1, Ordering::Relaxed);
                    self.reserved = None;
                    Poll::Ready(Err(e.into()))
                }
                other => other.map_err(Into::into),
            };
        }

        let entry = match self.inner.select_next() {
            Ok(entry) => entry,
            Err(e) => return Poll::Ready(Err(e)),
        };
        let mut reservation = ReservedChannel::new(entry.clone());
        match reservation.channel.poll_ready(cx) {
            Poll::Ready(Ok(())) => {
                self.reserved = Some(reservation);
                Poll::Ready(Ok(()))
            }
            Poll::Pending => {
                self.reserved = Some(reservation);
                Poll::Pending
            }
            Poll::Ready(Err(e)) => {
                entry.error_count.fetch_add(1, Ordering::Relaxed);
                Poll::Ready(Err(e.into()))
            }
        }
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        let ReservedChannel { guard, mut channel } = self
            .reserved
            .take()
            .expect("poll_ready must be called before call");
        let metrics = self.inner.metrics.clone();

        Box::pin(async move {
            match channel.call(request).await {
                Ok(response) => {
                    guard.entry.success_count.fetch_add(1, Ordering::Relaxed);
                    // Move the guard into the body so `in_flight` stays
                    // accurate for the lifetime of the HTTP/2 stream, not
                    // just until the headers arrive.
                    Ok(response.map(|inner| Body::new(GuardedBody::new(inner, guard, metrics))))
                }
                Err(e) => {
                    guard.entry.error_count.fetch_add(1, Ordering::Relaxed);
                    Err(e.into())
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use prometheus::Registry;
    use rstest::rstest;

    use super::*;
    use crate::{
        mock_server::MockBigTableServer,
        proto::bigtable::v2::{
            PingAndWarmRequest, ReadRowsRequest,
            bigtable_client::BigtableClient as BigtableInternalClient,
        },
    };

    fn metrics(pool: &ChannelPool) -> &Metrics {
        pool.inner.metrics.as_ref().expect("metrics not configured")
    }

    struct TestPrimer;

    impl ChannelPrimer for TestPrimer {
        fn prime<'a>(
            &'a self,
            channel: &'a Channel,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
            Box::pin(async move {
                let mut client = BigtableInternalClient::new(channel.clone());
                client
                    .ping_and_warm(PingAndWarmRequest {
                        name: String::new(),
                        app_profile_id: String::new(),
                    })
                    .await?;
                Ok(())
            })
        }
    }

    /// A primer whose future never completes.
    struct HangingPrimer;

    impl ChannelPrimer for HangingPrimer {
        fn prime<'a>(
            &'a self,
            _channel: &'a Channel,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
            Box::pin(std::future::pending())
        }
    }

    #[rstest]
    #[case::initial_pool_size(PoolConfig::default().initial_pool_size(0), "initial_pool_size")]
    #[case::max_resize_delta(PoolConfig::default().max_resize_delta(0), "max_resize_delta")]
    #[case::downscale_threshold(PoolConfig::default().downscale_threshold(0), "downscale_threshold")]
    #[case::maintenance_interval(PoolConfig::default().maintenance_interval(Duration::ZERO), "maintenance_interval")]
    #[case::channel_init_timeout(PoolConfig::default().channel_init_timeout(Duration::ZERO), "channel_init_timeout")]
    fn test_config_rejects_zero(#[case] result: Result<PoolConfig>, #[case] field: &str) {
        assert_eq!(
            result.unwrap_err().to_string(),
            format!("invalid pool config: `{field}` must be greater than zero")
        );
    }

    fn valid_config() -> PoolConfig {
        PoolConfig::default()
            .min_pool_size(2)
            .unwrap()
            .initial_pool_size(4)
            .unwrap()
            .max_pool_size(8)
            .unwrap()
            .min_rpcs_per_channel(0)
            .max_rpcs_per_channel(50)
            .unwrap()
    }

    const SIZE_MSG: &str =
        "invalid pool config: `min_pool_size` <= `initial_pool_size` <= `max_pool_size` must hold";
    const RPCS_MSG: &str =
        "invalid pool config: `min_rpcs_per_channel` must be less than `max_rpcs_per_channel`";

    #[test]
    fn test_config_validate_accepts_consistent_bounds() {
        assert!(valid_config().validate().is_ok());
    }

    #[rstest]
    #[case::min_above_initial(valid_config().min_pool_size(5).unwrap(), SIZE_MSG)]
    #[case::max_below_initial(valid_config().max_pool_size(3).unwrap(), SIZE_MSG)]
    #[case::min_rpcs_not_below_max(valid_config().min_rpcs_per_channel(50), RPCS_MSG)]
    fn test_config_validate_rejects(#[case] config: PoolConfig, #[case] msg: &str) {
        assert_eq!(config.validate().unwrap_err().to_string(), msg);
    }

    #[rstest]
    #[case::zero(Duration::ZERO)]
    #[case::sub_millisecond(Duration::from_micros(500))]
    #[case::five_minutes(Duration::from_secs(300))]
    fn test_compute_refresh_at_bounds_jitter(#[case] jitter: Duration) {
        let age = Duration::from_secs(45 * 60);
        let before = Instant::now();
        let at = ChannelPoolInner::compute_refresh_at(age, jitter);
        assert!(at >= before + age);
        assert!(at <= Instant::now() + age + jitter);
    }

    #[tokio::test]
    async fn test_prime_timeout_fails_entry() {
        let server = MockBigTableServer::start().await.unwrap();
        let registry = Registry::new();

        let config = PoolConfig {
            initial_pool_size: 1,
            min_pool_size: 1,
            max_pool_size: 10,
            channel_init_timeout: Duration::from_millis(100),
            ..PoolConfig::default()
        };
        let pool = ChannelPool::new(
            server.endpoint(),
            config,
            Some(Box::new(HangingPrimer)),
            Some(&registry),
        )
        .unwrap();

        // The primer never completes. Without the bound this would hang
        // forever; with it, the entry fails and nothing enters the pool.
        let err = tokio::time::timeout(Duration::from_secs(2), pool.connect())
            .await
            .expect("connect must not block on a hanging primer")
            .unwrap_err();
        assert!(
            matches!(err, BigTableClientError::ChannelInitTimeout),
            "unexpected error: {err}"
        );
        assert_eq!(metrics(&pool).pool_size.get(), 0);
    }

    #[tokio::test]
    async fn test_round_robin_rpcs() {
        let server = MockBigTableServer::start().await.unwrap();
        let registry = Registry::new();

        let config = PoolConfig {
            initial_pool_size: 2,
            min_pool_size: 1,
            max_pool_size: 10,
            ..PoolConfig::default()
        };
        let pool = ChannelPool::new(
            server.endpoint(),
            config,
            Some(Box::new(TestPrimer)),
            Some(&registry),
        )
        .unwrap();
        pool.connect().await.unwrap();

        // Send RPCs through the pool via PingAndWarm.
        let n = 6;
        for _ in 0..n {
            let mut client = BigtableInternalClient::new(pool.clone());
            client
                .ping_and_warm(PingAndWarmRequest {
                    name: String::new(),
                    app_profile_id: String::new(),
                })
                .await
                .unwrap();
        }

        assert_eq!(metrics(&pool).rpcs_completed.get(), n as u64);
        // Mock counts primer pings (2 from connect) + our 6 RPCs.
        assert_eq!(server.request_count(), 2 + n);

        // Verify round-robin: 6 RPCs split evenly across 2 channels.
        let entries = pool.inner.entries.load();
        assert_eq!(entries[0].success_count.load(Ordering::Relaxed), 3);
        assert_eq!(entries[1].success_count.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn test_in_flight_counts_open_streams() {
        let server = MockBigTableServer::start().await.unwrap();
        let registry = Registry::new();

        let pool = ChannelPool::new(
            server.endpoint(),
            PoolConfig::singleton(),
            None,
            Some(&registry),
        )
        .unwrap();
        pool.connect().await.unwrap();
        let entry = pool.inner.entries.load()[0].clone();

        let mut client = BigtableInternalClient::new(pool.clone());

        // Case 1: the stream is read to completion. The slot is released
        // when the final frame arrives, before the handle is dropped.
        let mut stream = client
            .read_rows(ReadRowsRequest::default())
            .await
            .unwrap()
            .into_inner();
        assert!(stream.message().await.unwrap().is_some());
        assert_eq!(entry.in_flight.load(Ordering::Relaxed), 1);
        assert!(stream.message().await.unwrap().is_some());
        assert_eq!(entry.in_flight.load(Ordering::Relaxed), 1);
        assert!(stream.message().await.unwrap().is_none());
        // The trailers closed the HTTP/2 stream, so the slot is released
        // even though the handle is still alive.
        assert_eq!(entry.in_flight.load(Ordering::Relaxed), 0);
        drop(stream);
        assert_eq!(entry.in_flight.load(Ordering::Relaxed), 0);

        // Case 2: the caller stops reading mid-stream. Dropping the handle
        // releases the slot.
        let mut stream = client
            .read_rows(ReadRowsRequest::default())
            .await
            .unwrap()
            .into_inner();
        assert!(stream.message().await.unwrap().is_some());
        assert_eq!(entry.in_flight.load(Ordering::Relaxed), 1);
        drop(stream);
        assert_eq!(entry.in_flight.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn test_dynamic_resize() {
        let server = MockBigTableServer::start().await.unwrap();
        let registry = Registry::new();

        // target_load = (5 + 50) / 2 = 27.5
        let config = PoolConfig {
            initial_pool_size: 1,
            min_pool_size: 1,
            max_pool_size: 10,
            max_resize_delta: 2,
            ..PoolConfig::default()
        };
        let pool = ChannelPool::new(server.endpoint(), config, None, Some(&registry)).unwrap();
        pool.connect().await.unwrap();
        assert_eq!(metrics(&pool).pool_size.get(), 1);

        let set_total_in_flight = |n: usize| {
            let entries = pool.inner.entries.load();
            // Put all load on entry[0], zero the rest.
            for (i, e) in entries.iter().enumerate() {
                e.in_flight
                    .store(if i == 0 { n } else { 0 }, Ordering::Relaxed);
            }
        };

        // Scale up: 1 channel with 100 in-flight → load=100 ≥ 50.
        // target = ceil(100/27.5) = 4, delta = 3 (uncapped). Pool: 1 → 4.
        set_total_in_flight(100);
        pool.inner.resize().await;
        assert_eq!(metrics(&pool).pool_size.get(), 4);

        // Scale up again: load=200/4=50 ≥ 50.
        // target = ceil(200/27.5) = 8, delta = 4. Pool: 4 → 8.
        set_total_in_flight(200);
        pool.inner.resize().await;
        assert_eq!(metrics(&pool).pool_size.get(), 8);

        // Remove load. Scale down requires downscale_threshold (default 3)
        // consecutive low-load observations before actually shrinking, then
        // is capped by max_resize_delta=2.
        set_total_in_flight(0);

        // First two low-load observations: no shrink yet (threshold not met).
        pool.inner.resize().await;
        assert_eq!(metrics(&pool).pool_size.get(), 8);
        pool.inner.resize().await;
        assert_eq!(metrics(&pool).pool_size.get(), 8);

        // Third consecutive low-load observation crosses the threshold.
        // load=0 ≤ 5 → target=max(ceil(0/27.5),1)=1, delta=min(8-1,2)=2. Pool:
        // 8 → 6.
        pool.inner.resize().await;
        assert_eq!(metrics(&pool).pool_size.get(), 6);

        // Counter was reset after scaling. Need 3 more consecutive
        // observations. 6 → 4
        pool.inner.resize().await;
        pool.inner.resize().await;
        pool.inner.resize().await;
        assert_eq!(metrics(&pool).pool_size.get(), 4);

        // 4 → 2
        pool.inner.resize().await;
        pool.inner.resize().await;
        pool.inner.resize().await;
        assert_eq!(metrics(&pool).pool_size.get(), 2);

        // 2 → 1 (min_pool_size)
        pool.inner.resize().await;
        pool.inner.resize().await;
        pool.inner.resize().await;
        assert_eq!(metrics(&pool).pool_size.get(), 1);

        // Already at min — no change even after threshold.
        pool.inner.resize().await;
        pool.inner.resize().await;
        pool.inner.resize().await;
        assert_eq!(metrics(&pool).pool_size.get(), 1);
    }

    #[tokio::test]
    async fn test_age_refresh_caps_replacements_per_cycle() {
        let server = MockBigTableServer::start().await.unwrap();
        let registry = Registry::new();

        let config = PoolConfig {
            initial_pool_size: 4,
            min_pool_size: 1,
            max_pool_size: 10,
            refresh_age: Duration::from_millis(0),
            refresh_jitter: Duration::ZERO,
            ..PoolConfig::default()
        };
        let pool = ChannelPool::new(server.endpoint(), config, None, Some(&registry)).unwrap();
        pool.connect().await.unwrap();

        // All 4 entries are expired, but only MAX_REPLACEMENTS_PER_CYCLE=2
        // replaced per cycle.
        pool.inner.refresh().await;
        assert_eq!(metrics(&pool).channels_replaced.get(), 2);

        // Second cycle replaces the remaining 2.
        pool.inner.refresh().await;
        assert_eq!(metrics(&pool).channels_replaced.get(), 4);
    }
}
