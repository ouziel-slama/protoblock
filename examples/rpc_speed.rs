use std::env;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{ensure, Context, Result};
use bitcoin::Block;
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};
use protoblock::{
    AsyncRpcClient, BlockProtocol, FetcherConfig, FetcherConfigBuilder, ProtocolFuture,
    ProtocolPreProcessFuture, Runner,
};
use tokio::task::JoinHandle;
use tokio::time::sleep;

const DEFAULT_RPC_URL: &str = "http://localhost:8332";
const DEFAULT_RPC_USER: &str = "rpc";
const DEFAULT_RPC_PASSWORD: &str = "rpc";
const DEFAULT_START_HEIGHT: u64 = 0;
const DEFAULT_THREAD_COUNT: usize = 4;
const DEFAULT_MAX_BATCH_MB: usize = 10;
const DEFAULT_QUEUE_MB: usize = 4_096;
const DEFAULT_REORG_WINDOW: usize = 12;
const DEFAULT_LOG_DIRECTIVE: &str = "warn";
const DEFAULT_MAX_REQUEST_BODY_MB: usize = 20;
const DEFAULT_MAX_RESPONSE_BODY_MB: usize = 20;

#[tokio::main]
async fn main() -> Result<()> {
    init_example_tracing();

    let args = ExampleArgs::from_env()?;
    let raw_bar = build_progress_bar();
    raw_bar.println(format!(
        "Starting RPC speed test at height {} with {} workers",
        args.start_height, args.thread_count
    ));

    let config = args.to_fetcher_config()?;
    let rpc_client = Arc::new(AsyncRpcClient::from_config(&config)?);
    let progress = Arc::new(TipAwareProgress::new(raw_bar.clone(), args.start_height));
    let stats = Arc::new(Mutex::new(RunStats::new(args.start_height)));
    initialize_tip_length(&progress, &rpc_client, args.start_height).await?;
    let tip_handle = spawn_tip_refresh(rpc_client.clone(), progress.clone());

    let protocol = RpcSpeedProtocol::new(progress.clone(), stats.clone());
    let mut runner = Runner::new(config, protocol);

    let run_result = runner.run_until_ctrl_c().await;
    tip_handle.abort();
    match run_result {
        Ok(()) => {
            progress.finish_with_message("stopped by Ctrl-C");
            if let Ok(stats) = stats.lock() {
                print_summary(progress.bar(), &stats);
            }
        }
        Err(err) => {
            progress.finish_with_message("pipeline aborted");
            if let Ok(stats) = stats.lock() {
                print_summary(progress.bar(), &stats);
            }
            return Err(err);
        }
    }

    Ok(())
}

fn init_example_tracing() {
    if env::var_os("RUST_LOG").is_none() {
        env::set_var("RUST_LOG", DEFAULT_LOG_DIRECTIVE);
    }
    protoblock::init_tracing();
}

fn build_progress_bar() -> ProgressBar {
    let bar = ProgressBar::with_draw_target(Some(1), ProgressDrawTarget::stdout_with_hz(12));
    let style = ProgressStyle::with_template(
        "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} blocks ({per_sec:.2} blk/s) ETA {eta_precise}",
    )
    .expect("valid progress bar template")
    .progress_chars("=>-");
    bar.set_style(style);
    bar.enable_steady_tick(Duration::from_millis(120));
    bar
}

fn print_summary(progress: &ProgressBar, stats: &RunStats) {
    let seconds = stats.elapsed().as_secs_f64();
    let rate = if seconds > 0.0 {
        stats.processed as f64 / seconds
    } else {
        0.0
    };

    progress.println(format!(
        "Processed {} blocks ({} -> {}) in {:.2}s [{:.2} blocks/s]",
        stats.processed, stats.start_height, stats.last_height, seconds, rate
    ));
}

struct ExampleArgs {
    rpc_url: String,
    rpc_user: String,
    rpc_password: String,
    start_height: u64,
    thread_count: usize,
    max_batch_size_mb: usize,
    queue_max_size_mb: usize,
    reorg_window_size: usize,
    max_request_body_mb: usize,
    max_response_body_mb: usize,
}

impl ExampleArgs {
    fn from_env() -> Result<Self> {
        let rpc_url = read_env_or_default("PROTOBLOCK_RPC_URL", DEFAULT_RPC_URL);
        let rpc_user = read_env_or_default("PROTOBLOCK_RPC_USER", DEFAULT_RPC_USER);
        let rpc_password = read_env_or_default("PROTOBLOCK_RPC_PASSWORD", DEFAULT_RPC_PASSWORD);
        let start_height =
            parse_env_with_default::<u64>("PROTOBLOCK_START_HEIGHT", DEFAULT_START_HEIGHT)?;
        let thread_count =
            parse_env_with_default::<usize>("PROTOBLOCK_THREAD_COUNT", DEFAULT_THREAD_COUNT)?;
        let max_batch_size_mb =
            parse_env_with_default::<usize>("PROTOBLOCK_MAX_BATCH_MB", DEFAULT_MAX_BATCH_MB)?;
        let queue_max_size_mb =
            parse_env_with_default::<usize>("PROTOBLOCK_QUEUE_MB", DEFAULT_QUEUE_MB)?;
        let reorg_window_size =
            parse_env_with_default::<usize>("PROTOBLOCK_REORG_WINDOW", DEFAULT_REORG_WINDOW)?;
        let max_request_body_mb = parse_env_with_default::<usize>(
            "PROTOBLOCK_MAX_REQUEST_MB",
            DEFAULT_MAX_REQUEST_BODY_MB,
        )?;
        let max_response_body_mb = parse_env_with_default::<usize>(
            "PROTOBLOCK_MAX_RESPONSE_MB",
            DEFAULT_MAX_RESPONSE_BODY_MB,
        )?;

        ensure!(
            thread_count > 0,
            "PROTOBLOCK_THREAD_COUNT must be greater than 0"
        );
        ensure!(
            max_batch_size_mb > 0,
            "PROTOBLOCK_MAX_BATCH_MB must be greater than 0"
        );
        ensure!(
            queue_max_size_mb > 0,
            "PROTOBLOCK_QUEUE_MB must be greater than 0"
        );
        ensure!(
            reorg_window_size > 0,
            "PROTOBLOCK_REORG_WINDOW must be greater than 0"
        );
        ensure!(
            max_request_body_mb > 0,
            "PROTOBLOCK_MAX_REQUEST_MB must be greater than 0"
        );
        ensure!(
            max_response_body_mb > 0,
            "PROTOBLOCK_MAX_RESPONSE_MB must be greater than 0"
        );

        Ok(Self {
            rpc_url,
            rpc_user,
            rpc_password,
            start_height,
            thread_count,
            max_batch_size_mb,
            queue_max_size_mb,
            reorg_window_size,
            max_request_body_mb,
            max_response_body_mb,
        })
    }

    fn to_fetcher_config(&self) -> Result<FetcherConfig> {
        let request_bytes = self.max_request_body_mb.saturating_mul(1024 * 1024);
        let response_bytes = self.max_response_body_mb.saturating_mul(1024 * 1024);

        FetcherConfigBuilder::default()
            .rpc_url(self.rpc_url.clone())
            .rpc_user(self.rpc_user.clone())
            .rpc_password(self.rpc_password.clone())
            .thread_count(self.thread_count)
            .max_batch_size_mb(self.max_batch_size_mb)
            .queue_max_size_mb(self.queue_max_size_mb)
            .reorg_window_size(self.reorg_window_size)
            .start_height(self.start_height)
            .rpc_max_request_body_bytes(request_bytes)
            .rpc_max_response_body_bytes(response_bytes)
            .build()
    }
}

fn read_env_or_default(key: &str, default: &str) -> String {
    match env::var(key) {
        Ok(value) if !value.trim().is_empty() => value,
        _ => default.to_string(),
    }
}

fn parse_env_with_default<T>(key: &str, default: T) -> Result<T>
where
    T: FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    match env::var(key) {
        Ok(value) => value
            .parse::<T>()
            .with_context(|| format!("failed to parse {key}='{value}'")),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(err) => Err(err).with_context(|| format!("failed to read {key}")),
    }
}

struct RpcSpeedProtocol {
    progress: Arc<TipAwareProgress>,
    stats: Arc<Mutex<RunStats>>,
}

impl RpcSpeedProtocol {
    fn new(progress: Arc<TipAwareProgress>, stats: Arc<Mutex<RunStats>>) -> Self {
        Self { progress, stats }
    }
}

impl BlockProtocol for RpcSpeedProtocol {
    type PreProcessed = ();

    fn pre_process(
        &self,
        _block: Block,
        _height: u64,
    ) -> ProtocolPreProcessFuture<Self::PreProcessed> {
        Box::pin(async { Ok(()) })
    }

    fn process<'a>(&'a mut self, _: Self::PreProcessed, height: u64) -> ProtocolFuture<'a> {
        let processed = {
            let mut stats = self
                .stats
                .lock()
                .expect("stats mutex poisoned in rpc speed protocol");
            stats.record(height);
            stats.processed
        };

        self.progress.record_height(processed, height);
        Box::pin(async { Ok(()) })
    }

    fn rollback<'a>(&'a mut self, _block_height: u64) -> ProtocolFuture<'a> {
        Box::pin(async { Ok(()) })
    }

    fn shutdown<'a>(&'a mut self) -> ProtocolFuture<'a> {
        Box::pin(async { Ok(()) })
    }
}

struct RunStats {
    start_height: u64,
    processed: u64,
    last_height: u64,
    started_at: Instant,
}

impl RunStats {
    fn new(start_height: u64) -> Self {
        Self {
            start_height,
            processed: 0,
            last_height: start_height.saturating_sub(1),
            started_at: Instant::now(),
        }
    }

    fn record(&mut self, height: u64) {
        self.processed = self.processed.saturating_add(1);
        self.last_height = height;
    }

    fn elapsed(&self) -> Duration {
        self.started_at.elapsed()
    }
}

struct TipAwareProgress {
    start_height: u64,
    tip_height: AtomicU64,
    bar: ProgressBar,
}

impl TipAwareProgress {
    fn new(bar: ProgressBar, start_height: u64) -> Self {
        bar.set_length(1);
        bar.set_position(0);
        Self {
            start_height,
            tip_height: AtomicU64::new(start_height),
            bar,
        }
    }

    fn update_tip(&self, tip: u64) {
        self.tip_height.store(tip, Ordering::SeqCst);
        let len = tip
            .saturating_sub(self.start_height)
            .saturating_add(1)
            .max(1);
        self.bar.set_length(len);
    }

    fn record_height(&self, processed: u64, height: u64) {
        let current_len = self.bar.length().unwrap_or(0);
        if processed > current_len {
            self.bar.set_length(processed);
        }
        self.bar.set_position(processed);
        self.bar.set_message(format!("height {}", height));
    }

    fn finish_with_message(&self, message: &str) {
        self.bar.finish_with_message(message.to_string());
    }

    fn println(&self, message: impl AsRef<str>) {
        self.bar.println(message.as_ref());
    }

    fn bar(&self) -> &ProgressBar {
        &self.bar
    }
}

async fn initialize_tip_length(
    progress: &Arc<TipAwareProgress>,
    rpc_client: &Arc<AsyncRpcClient>,
    fallback_height: u64,
) -> Result<()> {
    match rpc_client.get_blockchain_tip().await {
        Ok(tip) => progress.update_tip(tip),
        Err(err) => {
            progress.println(format!(
                "Failed to fetch blockchain tip; defaulting to start height (error: {err})"
            ));
            progress.update_tip(fallback_height);
        }
    }
    Ok(())
}

fn spawn_tip_refresh(
    rpc_client: Arc<AsyncRpcClient>,
    progress: Arc<TipAwareProgress>,
) -> JoinHandle<()> {
    const TIP_REFRESH_INTERVAL_SECS: u64 = 5;
    tokio::spawn(async move {
        loop {
            match rpc_client.get_blockchain_tip().await {
                Ok(tip) => progress.update_tip(tip),
                Err(err) => progress.println(format!("tip refresh failed: {err}")),
            }
            sleep(Duration::from_secs(TIP_REFRESH_INTERVAL_SECS)).await;
        }
    })
}
