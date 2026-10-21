// Copyright (c) Mysten Labs, Inc.
// Modifications Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! An adaptive pool of gRPC connections to Bigtable.
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
//!   minutes to prevent failures caused by Google's server-side connection
//!   termination policy (roughly 1 hour).

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
use http::{Request, Response};
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
use tracing::{info, warn};

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

/// Sizing and maintenance parameters of the Connection Pool.
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

    /// Sets the number of channels opened on connect.
    pub fn initial_pool_size(mut self, value: impl TryInto<NonZeroUsize>) -> Result<Self> {
        self.initial_pool_size = non_zero(value, "initial_pool_size")?;
        Ok(self)
    }

    /// Sets the lower bound the pool never shrinks below.
    pub fn min_pool_size(mut self, value: impl TryInto<NonZeroUsize>) -> Result<Self> {
        self.min_pool_size = non_zero(value, "min_pool_size")?;
        Ok(self)
    }

    /// Sets the upper bound the pool never grows above.
    pub fn max_pool_size(mut self, value: impl TryInto<NonZeroUsize>) -> Result<Self> {
        self.max_pool_size = non_zero(value, "max_pool_size")?;
        Ok(self)
    }

    /// Sets the average in-flight RPCs per channel below which the pool
    /// shrinks. Zero means the pool only shrinks when fully idle.
    pub fn min_rpcs_per_channel(mut self, value: usize) -> Self {
        self.min_rpcs_per_channel = value;
        self
    }

    /// Sets the average in-flight RPCs per channel at or above which the pool
    /// grows.
    pub fn max_rpcs_per_channel(mut self, value: impl TryInto<NonZeroUsize>) -> Result<Self> {
        self.max_rpcs_per_channel = non_zero(value, "max_rpcs_per_channel")?;
        Ok(self)
    }

    /// Sets the maximum channels removed in one shrink step.
    pub fn max_resize_delta(mut self, value: impl TryInto<NonZeroUsize>) -> Result<Self> {
        self.max_resize_delta = non_zero(value, "max_resize_delta")?;
        Ok(self)
    }

    /// Sets the consecutive low-load cycles required before shrinking.
    pub fn downscale_threshold(mut self, value: impl TryInto<NonZeroUsize>) -> Result<Self> {
        self.downscale_threshold = non_zero(value, "downscale_threshold")?;
        Ok(self)
    }

    /// Sets the period of the background refresh/resize loop. A zero
    /// duration is rejected.
    pub fn maintenance_interval(mut self, value: Duration) -> Result<Self> {
        if value.is_zero() {
            return Err(BigTableClientError::InvalidPoolConfig(
                "`maintenance_interval` must be greater than zero".into(),
            ));
        }
        self.maintenance_interval = value;
        Ok(self)
    }

    /// Sets the age after which a channel is replaced.
    ///
    /// A zero duration causes constant reconnection and is only useful in
    /// tests. Keep this well above the maintenance interval in production.
    pub fn refresh_age(mut self, value: Duration) -> Self {
        self.refresh_age = value;
        self
    }

    /// Sets the random extra delay added to `refresh_age` per channel. Zero
    /// disables jitter.
    pub fn refresh_jitter(mut self, value: Duration) -> Self {
        self.refresh_jitter = value;
        self
    }
}

/// Converts a setter argument to `usize`, rejecting zero with a
/// [`BigTableClientError::InvalidPoolConfig`] naming the offending field.
fn non_zero(value: impl TryInto<NonZeroUsize>, field: &str) -> Result<usize> {
    value.try_into().map(NonZeroUsize::get).map_err(|_| {
        BigTableClientError::InvalidPoolConfig(format!("`{field}` must be greater than zero"))
    })
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            initial_pool_size: 10,
            min_pool_size: 1,
            max_pool_size: 200,
            min_rpcs_per_channel: 5,
            max_rpcs_per_channel: 50,
            max_resize_delta: 2,
            downscale_threshold: 3,
            maintenance_interval: Duration::from_secs(60),
            // GFE forcibly disconnects channels after ~60 minutes for load
            // balancing. Refresh at 45m + up to 5m jitter to replace channels
            // before reaping.
            refresh_age: Duration::from_secs(45 * 60),
            refresh_jitter: Duration::from_secs(5 * 60),
        }
    }
}

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
        let pool = Self::new(endpoint, config, primer, registry);
        pool.connect().await?;
        pool.spawn_background_tasks();
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

    /// Spawn the background maintenance loop that periodically refreshes
    /// aged-out channels and resizes the pool based on load.
    fn spawn_background_tasks(&self) {
        let weak = Arc::downgrade(&self.inner);
        let period = self.inner.config.maintenance_interval;
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(period);
            interval.tick().await; // first tick completes immediately
            loop {
                interval.tick().await;
                let Some(inner) = weak.upgrade() else {
                    return;
                };
                inner.refresh().await;
                inner.resize().await;
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
                    errors, successes, "channel errors since last maintenance cycle"
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
            Duration::from_millis(
                rand::thread_rng().gen_range(0..refresh_jitter.as_millis() as u64),
            )
        };
        Instant::now() + refresh_age + jitter
    }

    /// Opens, primes and wraps a new channel. This is the single place every
    /// pool entry is created, whether on connect, age refresh or scale-up.
    ///
    /// A connection failure is returned to the caller. A priming failure is
    /// only logged, since the channel still works and merely pays the warm-up
    /// cost on its first real request.
    async fn create_primed_entry(&self) -> Result<PoolEntry> {
        let channel = self.endpoint.connect().await?;
        if let Some(primer) = self.primer.as_deref()
            && let Err(e) = primer.prime(&channel).await
        {
            warn!(error = %e, "channel priming failed (non-fatal)");
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

// Entries are wrapped in Arc and held by both the pool's entry list and any
// in-flight RPCs (via Service::call). When an entry is replaced or removed
// from the pool, in-flight RPCs keep their Arc<PoolEntry> alive until the
// response completes with no explicit drain needed.
struct PoolEntry {
    channel: Channel,
    refresh_at: Instant,
    in_flight: AtomicUsize,
    // Per-channel counters reset each maintenance cycle. Error count triggers
    // a warn log; success count is useful for verifying round-robin
    // distribution in tests. Neither counter influences selection or
    // replacement decisions.
    success_count: AtomicUsize,
    error_count: AtomicUsize,
}

/// Decrements `in_flight` when dropped, ensuring the count stays accurate
/// even if the RPC future is cancelled (e.g. client disconnect).
struct InFlightGuard {
    entry: Arc<PoolEntry>,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.entry.in_flight.fetch_sub(1, Ordering::Relaxed);
    }
}

impl Service<Request<Body>> for ChannelPool {
    type Response = Response<Body>;
    type Error = BigTableClientError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        if let Some((_, ref mut channel)) = self.reserved {
            return channel.poll_ready(cx).map_err(Into::into);
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
        let (entry, mut channel) = self.reserved.take().expect("called before poll_ready");
        let metrics = self.inner.metrics.clone();

        Box::pin(async move {
            let _guard = InFlightGuard {
                entry: entry.clone(),
            };
            let result = channel.call(request).await;
            if let Some(m) = &metrics {
                m.rpcs_completed.inc();
            }
            match &result {
                Ok(_) => {
                    entry.success_count.fetch_add(1, Ordering::Relaxed);
                }
                Err(_) => {
                    entry.error_count.fetch_add(1, Ordering::Relaxed);
                }
            }
            Ok(result?)
        })
    }
}

#[cfg(test)]
mod tests {
    use prometheus::Registry;

    use super::*;
    use crate::{
        mock_server::MockBigtableServer,
        proto::bigtable::v2::{
            PingAndWarmRequest, bigtable_client::BigtableClient as BigtableInternalClient,
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

    #[test]
    fn test_config_setters_reject_zero() {
        let config = PoolConfig::default()
            .initial_pool_size(4)
            .unwrap()
            .min_pool_size(2)
            .unwrap()
            .max_pool_size(NonZeroUsize::new(8).unwrap())
            .unwrap()
            .min_rpcs_per_channel(0);
        assert_eq!(config.initial_pool_size, 4);
        assert_eq!(config.min_pool_size, 2);
        assert_eq!(config.max_pool_size, 8);
        assert_eq!(config.min_rpcs_per_channel, 0);

        let err = PoolConfig::default().initial_pool_size(0).unwrap_err();
        assert!(
            matches!(err, BigTableClientError::InvalidPoolConfig(ref m) if m.contains("initial_pool_size")),
            "unexpected error: {err}"
        );
        assert!(PoolConfig::default().max_resize_delta(0).is_err());
        assert!(PoolConfig::default().downscale_threshold(0).is_err());
        assert!(
            PoolConfig::default()
                .maintenance_interval(Duration::ZERO)
                .is_err()
        );

        let config = PoolConfig::default()
            .maintenance_interval(Duration::from_secs(5))
            .unwrap()
            .refresh_age(Duration::from_secs(1))
            .refresh_jitter(Duration::ZERO);
        assert_eq!(config.maintenance_interval, Duration::from_secs(5));
        assert_eq!(config.refresh_age, Duration::from_secs(1));
        assert_eq!(config.refresh_jitter, Duration::ZERO);
    }

    #[tokio::test]
    async fn test_pool_creation() {
        let server = MockBigtableServer::start().await.unwrap();
        let registry = Registry::new();

        let config = PoolConfig {
            initial_pool_size: 4,
            min_pool_size: 1,
            max_pool_size: 10,
            ..PoolConfig::default()
        };
        let pool = ChannelPool::new(server.endpoint(), config, None, Some(&registry));
        pool.connect().await.unwrap();

        assert_eq!(metrics(&pool).pool_size.get(), 4);
    }

    #[tokio::test]
    async fn test_pool_creation_with_primer() {
        let server = MockBigtableServer::start().await.unwrap();
        let registry = Registry::new();

        let config = PoolConfig {
            initial_pool_size: 3,
            min_pool_size: 1,
            max_pool_size: 10,
            ..PoolConfig::default()
        };
        let pool = ChannelPool::new(
            server.endpoint(),
            config,
            Some(Box::new(TestPrimer)),
            Some(&registry),
        );
        pool.connect().await.unwrap();

        assert_eq!(metrics(&pool).pool_size.get(), 3);
    }

    #[tokio::test]
    async fn test_round_robin_rpcs() {
        let server = MockBigtableServer::start().await.unwrap();
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
        );
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
    async fn test_dynamic_resize() {
        let server = MockBigtableServer::start().await.unwrap();
        let registry = Registry::new();

        // target_load = (5 + 50) / 2 = 27
        let config = PoolConfig {
            initial_pool_size: 1,
            min_pool_size: 1,
            max_pool_size: 10,
            max_resize_delta: 2,
            ..PoolConfig::default()
        };
        let pool = ChannelPool::new(server.endpoint(), config, None, Some(&registry));
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
        // target = ceil(100/27) = 4, delta = 3 (uncapped). Pool: 1 → 4.
        set_total_in_flight(100);
        pool.inner.resize().await;
        assert_eq!(metrics(&pool).pool_size.get(), 4);

        // Scale up again: load=200/4=50 ≥ 50.
        // target = ceil(200/27) = 8, delta = 4. Pool: 4 → 8.
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
        // load=0 ≤ 5 → target=max(ceil(0/27),1)=1, delta=min(8-1,2)=2. Pool:
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
    async fn test_age_refresh_replaces_old_channels() {
        let server = MockBigtableServer::start().await.unwrap();
        let registry = Registry::new();

        let config = PoolConfig {
            initial_pool_size: 2,
            min_pool_size: 1,
            max_pool_size: 10,
            refresh_age: Duration::from_millis(0),
            refresh_jitter: Duration::ZERO,
            ..PoolConfig::default()
        };
        let pool = ChannelPool::new(server.endpoint(), config, None, Some(&registry));
        pool.connect().await.unwrap();

        pool.inner.refresh().await;

        // Both entries expired, both replaced (within
        // MAX_REPLACEMENTS_PER_CYCLE=2).
        assert_eq!(metrics(&pool).channels_replaced.get(), 2);
    }

    #[tokio::test]
    async fn test_age_refresh_caps_replacements_per_cycle() {
        let server = MockBigtableServer::start().await.unwrap();
        let registry = Registry::new();

        let config = PoolConfig {
            initial_pool_size: 4,
            min_pool_size: 1,
            max_pool_size: 10,
            refresh_age: Duration::from_millis(0),
            refresh_jitter: Duration::ZERO,
            ..PoolConfig::default()
        };
        let pool = ChannelPool::new(server.endpoint(), config, None, Some(&registry));
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
