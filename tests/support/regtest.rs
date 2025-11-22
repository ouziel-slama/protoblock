use std::net::TcpListener as StdTcpListener;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use jsonrpsee::core::client::ClientT;
use jsonrpsee::http_client::{HeaderMap, HeaderValue, HttpClient, HttpClientBuilder};
use jsonrpsee::rpc_params;
use serde_json::Value;
use tokio::task;
use tokio::time::sleep;

pub struct RegtestNode {
    _datadir: tempfile::TempDir,
    child: Option<Child>,
    rpc_url: String,
    rpc_user: String,
    rpc_password: String,
    rpc_client: HttpClient,
    wallet_client: Option<HttpClient>,
    wallet_name: String,
}

impl RegtestNode {
    pub async fn start() -> Result<Self> {
        let datadir = tempfile::tempdir().context("failed to create regtest datadir")?;
        let rpc_port = pick_available_port().context("failed to reserve RPC port")?;
        let p2p_port = pick_available_port().context("failed to reserve p2p port")?;
        let rpc_user = format!("user{rpc_port}");
        let rpc_password = format!("pass{rpc_port}");

        let mut command = Command::new("bitcoind");
        command
            .arg("-regtest")
            .arg("-server=1")
            .arg("-listen=0")
            .arg("-discover=0")
            .arg("-dnsseed=0")
            .arg("-fallbackfee=0.0002")
            .arg("-txindex=1")
            .arg(format!("-port={p2p_port}"))
            .arg(format!("-rpcport={rpc_port}"))
            .arg("-rpcbind=127.0.0.1")
            .arg("-rpcallowip=127.0.0.1")
            .arg(format!("-rpcuser={rpc_user}"))
            .arg(format!("-rpcpassword={rpc_password}"))
            .arg(format!("-datadir={}", datadir.path().display()))
            .stdout(Stdio::null())
            .stderr(Stdio::null());

        let child = command.spawn().context("failed to start bitcoind")?;
        let rpc_url = format!("http://127.0.0.1:{rpc_port}");
        let rpc_client = build_client(&rpc_url, &rpc_user, &rpc_password)
            .context("failed to build base RPC client")?;

        let mut node = Self {
            _datadir: datadir,
            child: Some(child),
            rpc_url,
            rpc_user,
            rpc_password,
            rpc_client,
            wallet_client: None,
            wallet_name: "itest-wallet".to_owned(),
        };

        node.wait_for_ready().await?;
        node.ensure_wallet().await?;

        Ok(node)
    }

    pub fn rpc_url(&self) -> &str {
        &self.rpc_url
    }

    pub fn rpc_user(&self) -> &str {
        &self.rpc_user
    }

    pub fn rpc_password(&self) -> &str {
        &self.rpc_password
    }

    pub async fn mine_blocks(&self, count: u64) -> Result<()> {
        let wallet = self.wallet()?;
        let address: String = wallet
            .request("getnewaddress", rpc_params!["integration-tests", "bech32"])
            .await
            .context("failed to allocate new address")?;
        let _: Vec<String> = wallet
            .request("generatetoaddress", rpc_params![count, address])
            .await
            .context("failed to mine regtest blocks")?;
        Ok(())
    }

    pub async fn invalidate_tip(&self, depth: usize) -> Result<()> {
        for _ in 0..depth {
            let tip: String = self
                .rpc_client
                .request("getbestblockhash", rpc_params![])
                .await
                .context("failed to query best block hash")?;
            let _: Value = self
                .rpc_client
                .request("invalidateblock", rpc_params![tip])
                .await
                .context("failed to invalidate block")?;
        }
        Ok(())
    }

    pub async fn best_height(&self) -> Result<u64> {
        self.rpc_client
            .request("getblockcount", rpc_params![])
            .await
            .context("failed to fetch block count")
    }

    pub async fn shutdown(mut self) -> Result<()> {
        if let Some(mut child) = self.child.take() {
            let _ = self
                .rpc_client
                .request::<Value, _>("stop", rpc_params![])
                .await;
            let status = task::spawn_blocking(move || child.wait())
                .await
                .context("failed to join bitcoind")?;
            status.context("bitcoind wait error")?;
        }
        Ok(())
    }

    fn wallet(&self) -> Result<&HttpClient> {
        self.wallet_client
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("wallet client not initialized"))
    }

    async fn wait_for_ready(&mut self) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if Instant::now() > deadline {
                bail!("bitcoind RPC did not become ready in time");
            }

            match self
                .rpc_client
                .request::<u64, _>("getblockcount", rpc_params![])
                .await
            {
                Ok(_) => return Ok(()),
                Err(_) => {
                    if let Some(child) = self.child.as_mut() {
                        if let Some(status) =
                            child.try_wait().context("failed to poll bitcoind status")?
                        {
                            bail!("bitcoind exited early with status {status}");
                        }
                    }
                    sleep(Duration::from_millis(200)).await;
                }
            }
        }
    }

    async fn ensure_wallet(&mut self) -> Result<()> {
        match self
            .rpc_client
            .request::<Value, _>("createwallet", rpc_params![self.wallet_name.clone()])
            .await
        {
            Ok(_) => {}
            Err(err) => {
                if !err.to_string().contains("already exists") {
                    return Err(err).context("failed to create regtest wallet");
                }
            }
        }

        let wallet_url = format!("{}/wallet/{}", self.rpc_url, self.wallet_name);
        self.wallet_client = Some(
            build_client(&wallet_url, &self.rpc_user, &self.rpc_password)
                .context("failed to build wallet client")?,
        );

        Ok(())
    }
}

impl Drop for RegtestNode {
    fn drop(&mut self) {
        if let Some(child) = self.child.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn build_client(endpoint: &str, user: &str, password: &str) -> Result<HttpClient> {
    let headers = auth_headers(user, password)?;
    HttpClientBuilder::default()
        .set_headers(headers)
        .build(endpoint)
        .map_err(|err| anyhow::anyhow!("failed to construct RPC client: {err}"))
}

fn auth_headers(user: &str, password: &str) -> Result<HeaderMap> {
    let mut headers = HeaderMap::new();
    let credentials = format!("{user}:{password}");
    let encoded = BASE64_STANDARD.encode(credentials);
    let value = HeaderValue::from_str(&format!("Basic {encoded}"))
        .context("failed to encode Authorization header")?;
    headers.insert("Authorization", value);
    Ok(headers)
}

fn pick_available_port() -> Result<u16> {
    let socket = StdTcpListener::bind("127.0.0.1:0").context("failed to bind temporary socket")?;
    let port = socket
        .local_addr()
        .context("failed to read temporary socket addr")?
        .port();
    Ok(port)
}
