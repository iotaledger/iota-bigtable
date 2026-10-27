// Throwaway smoke test against a real BigTable instance. Not committed.
// Read-only: range scans only.
//
// env RUST_LOG=iota_bigtable=info GOOGLE_APPLICATION_CREDENTIALS=...
// BT_INSTANCE=devnet \   cargo run --example pool_smoke
//
// Optional: BT_TABLE (checkpoints), BT_CF (iota), BT_CONCURRENCY (200),
// BT_BURST_SECS (25), BT_ROWS (10).

use std::time::{Duration, Instant};

use iota_bigtable::{BigTableClient, PoolConfig};
use prometheus::Registry;

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn dump(registry: &Registry) {
    for mf in registry.gather() {
        if mf.name().starts_with("bt_pool") {
            for m in mf.get_metric() {
                let v = if mf.get_field_type() == prometheus::proto::MetricType::GAUGE {
                    m.get_gauge().value()
                } else {
                    m.get_counter().value()
                };
                println!("  {} = {}", mf.name(), v);
            }
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let instance = env_or("BT_INSTANCE", "devnet");
    let table = env_or("BT_TABLE", "checkpoints");
    let cf = env_or("BT_CF", "iota");
    let concurrency: usize = env_or("BT_CONCURRENCY", "200").parse()?;
    let burst_secs: u64 = env_or("BT_BURST_SECS", "25").parse()?;
    let rows: usize = env_or("BT_ROWS", "10").parse()?;
    let registry = Registry::new();

    // Small pool, fast maintenance, short channel life so resize and refresh
    // both show up within a couple of minutes.
    let config = PoolConfig::new()
        .initial_pool_size(4)?
        .min_pool_size(2)?
        .max_pool_size(20)?
        .maintenance_interval(Duration::from_secs(10))?
        .refresh_age(Duration::from_secs(30))
        .refresh_jitter(Duration::from_secs(5));

    let t = Instant::now();
    let client =
        BigTableClient::new_remote(instance, true, None, "smoke", cf, &registry, config).await?;
    println!("connected + primed in {:?}", t.elapsed());
    dump(&registry);

    let mut c = client.clone();
    let got = c.range_scan(&table, None, None, rows, false, None).await?;
    println!("single scan of {table}: {} rows", got.len());

    println!("burst: {concurrency} concurrent scans, repeated for {burst_secs}s");
    let t = Instant::now();
    let (mut ok, mut err, mut rounds) = (0usize, 0usize, 0usize);
    while t.elapsed() < Duration::from_secs(burst_secs) {
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..concurrency {
            let mut c = client.clone();
            let table = table.clone();
            tasks.spawn(async move {
                c.range_scan(&table, None, None, rows, false, None)
                    .await
                    .map(|r| r.len())
            });
        }
        while let Some(r) = tasks.join_next().await {
            match r? {
                Ok(_) => ok += 1,
                Err(e) => {
                    err += 1;
                    if err <= 5 {
                        eprintln!("  scan error: {e}");
                    }
                }
            }
        }
        rounds += 1;
    }
    println!(
        "burst done in {:?}: rounds={rounds} ok={ok} err={err}",
        t.elapsed()
    );
    dump(&registry);

    println!("idle for 90s: expect shrink to 2 after 3 low-load ticks, refresh at ~30s age");
    for i in 1..=9 {
        tokio::time::sleep(Duration::from_secs(10)).await;
        println!("t+{}s", i * 10);
        dump(&registry);
    }
    Ok(())
}
