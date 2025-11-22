use std::{
    collections::HashMap,
    convert::Infallible,
    str::FromStr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, RwLock,
    },
};

use anyhow::{anyhow, bail, Context, Result};
use bitcoin::blockdata::block::{Header as BlockHeader, Version};
use bitcoin::hashes::Hash;
use bitcoin::pow::CompactTarget;
use bitcoin::{consensus, Block, BlockHash, TxMerkleNode};
use hyper::service::{make_service_fn, service_fn};
use hyper::{body, Body, Method, Request, Response, Server, StatusCode};
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

#[derive(Clone)]
pub struct MockChain {
    inner: Arc<RwLock<MockChainInner>>,
    tip_limit: Arc<AtomicU64>,
    epoch: Arc<AtomicU64>,
}

#[derive(Clone)]
struct BlockData {
    height: u64,
    hash: String,
    hex: String,
}

struct MockChainInner {
    by_height: HashMap<u64, BlockData>,
    by_hash: HashMap<String, BlockData>,
}

impl MockChain {
    pub fn new(length: u64) -> Self {
        let mut by_height = HashMap::new();
        let mut by_hash = HashMap::new();
        let mut previous = BlockHash::from_slice(&[0u8; 32]).expect("zero hash should deserialize");

        for height in 0..length {
            let (hash, hex) = build_block(height, previous, 0);
            let data = BlockData {
                height,
                hash: hash.to_string(),
                hex,
            };
            by_hash.insert(data.hash.clone(), data.clone());
            by_height.insert(height, data);
            previous = hash;
        }

        Self {
            inner: Arc::new(RwLock::new(MockChainInner { by_height, by_hash })),
            tip_limit: Arc::new(AtomicU64::new(length.saturating_sub(1))),
            epoch: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn best_height(&self) -> u64 {
        self.tip_limit.load(Ordering::SeqCst)
    }

    fn hash_for_height(&self, height: u64) -> Option<String> {
        if height > self.tip_limit.load(Ordering::SeqCst) {
            return None;
        }
        let inner = self.inner.read().expect("mock chain poisoned");
        inner.by_height.get(&height).map(|data| data.hash.clone())
    }

    fn hex_for_hash(&self, hash: &str) -> Option<String> {
        let limit = self.tip_limit.load(Ordering::SeqCst);
        let inner = self.inner.read().expect("mock chain poisoned");
        inner.by_hash.get(hash).and_then(|data| {
            if data.height > limit {
                None
            } else {
                Some(data.hex.clone())
            }
        })
    }

    pub fn max_height(&self) -> u64 {
        self.inner
            .read()
            .expect("mock chain poisoned")
            .by_height
            .keys()
            .copied()
            .max()
            .unwrap_or(0)
    }

    pub fn corrupt_block_hex(&self, height: u64, hex: impl Into<String>) -> Result<()> {
        let mut inner = self.inner.write().expect("mock chain poisoned");
        let data = inner
            .by_height
            .get_mut(&height)
            .ok_or_else(|| anyhow!("cannot corrupt unknown height {height}"))?;
        let new_hex = hex.into();
        let hash = data.hash.clone();
        data.hex = new_hex.clone();
        if let Some(entry) = inner.by_hash.get_mut(&hash) {
            entry.hex = new_hex;
        }
        Ok(())
    }

    pub fn set_tip_limit(&self, limit: u64) {
        let clamped = limit.min(self.max_height());
        self.tip_limit.store(clamped, Ordering::SeqCst);
    }

    pub fn advance_tip_by(&self, delta: u64) -> u64 {
        if delta == 0 {
            return self.tip_limit.load(Ordering::SeqCst);
        }

        let max_height = self.max_height();
        loop {
            let current = self.tip_limit.load(Ordering::SeqCst);
            if current >= max_height {
                return max_height;
            }
            let next = current.saturating_add(delta).min(max_height);
            match self
                .tip_limit
                .compare_exchange(current, next, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => return next,
                Err(_) => continue,
            }
        }
    }

    pub fn force_reorg(&self, fork_height: u64, new_suffix_len: u64) -> Result<()> {
        if new_suffix_len == 0 {
            bail!("new_suffix_len must be greater than zero");
        }

        let mut inner = self.inner.write().expect("mock chain poisoned");
        let salt = self.epoch.fetch_add(1, Ordering::SeqCst).saturating_add(1);
        let parent_hash = if fork_height == 0 {
            BlockHash::from_slice(&[0u8; 32]).expect("zero hash should deserialize")
        } else {
            inner
                .by_height
                .get(&fork_height)
                .ok_or_else(|| anyhow!("cannot reorg: missing fork height {fork_height}"))
                .and_then(|data| {
                    BlockHash::from_str(&data.hash)
                        .map_err(|err| anyhow!("stored hash invalid: {err}"))
                })?
        };

        let heights_to_remove: Vec<u64> = inner
            .by_height
            .keys()
            .copied()
            .filter(|height| *height > fork_height)
            .collect();
        for height in heights_to_remove {
            if let Some(old) = inner.by_height.remove(&height) {
                inner.by_hash.remove(&old.hash);
            }
        }

        let mut previous = parent_hash;
        for offset in 1..=new_suffix_len {
            let height = fork_height.saturating_add(offset);
            let (hash, hex) = build_block(height, previous, salt);
            let data = BlockData {
                height,
                hash: hash.to_string(),
                hex,
            };
            inner.by_hash.insert(data.hash.clone(), data.clone());
            inner.by_height.insert(height, data);
            previous = hash;
        }

        let new_max = fork_height.saturating_add(new_suffix_len);
        let current_limit = self.tip_limit.load(Ordering::SeqCst);
        if current_limit > new_max {
            self.tip_limit.store(new_max, Ordering::SeqCst);
        }

        Ok(())
    }
}

fn build_block(height: u64, prev_hash: BlockHash, salt: u64) -> (BlockHash, String) {
    let mut merkle_bytes = [0u8; 32];
    merkle_bytes[..8].copy_from_slice(&height.to_le_bytes());
    merkle_bytes[8..16].copy_from_slice(&salt.to_le_bytes());
    let merkle_root = TxMerkleNode::from_slice(&merkle_bytes).expect("valid merkle root bytes");

    let header = BlockHeader {
        version: Version::from_consensus(1),
        prev_blockhash: prev_hash,
        merkle_root,
        time: 1 + height as u32 + salt as u32,
        bits: CompactTarget::from_consensus(0x207f_ffff),
        nonce: height as u32 ^ salt as u32,
    };

    let block = Block {
        header,
        txdata: Vec::new(),
    };
    let hash = block.block_hash();
    let hex = hex::encode(consensus::serialize(&block));

    (hash, hex)
}

pub struct MockRpcServer {
    url: String,
    shutdown: Option<oneshot::Sender<()>>,
    handle: Option<JoinHandle<()>>,
}

impl MockRpcServer {
    pub async fn start(chain: MockChain) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .context("failed to bind mock RPC listener")?;
        let addr = listener
            .local_addr()
            .context("failed to read mock listener address")?;
        let std_listener = listener
            .into_std()
            .context("failed to convert mock listener")?;
        std_listener
            .set_nonblocking(true)
            .context("failed to set mock listener non-blocking")?;

        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let make_service = make_service_fn(move |_| {
            let chain = chain.clone();
            async move { Ok::<_, Infallible>(service_fn(move |req| serve_request(chain.clone(), req))) }
        });

        let server = Server::from_tcp(std_listener)
            .context("failed to build mock HTTP server")?
            .serve(make_service);
        let graceful = server.with_graceful_shutdown(async {
            let _ = shutdown_rx.await;
        });

        let handle = tokio::spawn(async move {
            if let Err(err) = graceful.await {
                eprintln!("mock RPC server stopped: {err}");
            }
        });

        Ok(Self {
            url: format!("http://{}", addr),
            shutdown: Some(shutdown_tx),
            handle: Some(handle),
        })
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.handle.take() {
            let _ = handle.await;
        }
    }
}

async fn serve_request(chain: MockChain, req: Request<Body>) -> Result<Response<Body>, Infallible> {
    if req.method() != Method::POST {
        let mut response = Response::new(Body::from("Unsupported method"));
        *response.status_mut() = StatusCode::METHOD_NOT_ALLOWED;
        return Ok(response);
    }

    let bytes = match body::to_bytes(req.into_body()).await {
        Ok(bytes) => bytes,
        Err(err) => {
            let mut response = Response::new(Body::from(format!("failed to read body: {err}")));
            *response.status_mut() = StatusCode::BAD_REQUEST;
            return Ok(response);
        }
    };

    let payload: Value = match serde_json::from_slice(&bytes) {
        Ok(value) => value,
        Err(err) => {
            let mut response = Response::new(Body::from(format!("invalid JSON payload: {err}")));
            *response.status_mut() = StatusCode::BAD_REQUEST;
            return Ok(response);
        }
    };

    let response_value = if payload.is_array() {
        Value::Array(
            payload
                .as_array()
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .map(|call| handle_call(&chain, call))
                .collect(),
        )
    } else {
        handle_call(&chain, payload)
    };

    let mut response = Response::new(Body::from(response_value.to_string()));
    response.headers_mut().insert(
        hyper::header::CONTENT_TYPE,
        hyper::header::HeaderValue::from_static("application/json"),
    );
    Ok(response)
}

fn handle_call(chain: &MockChain, call: Value) -> Value {
    let id = call.get("id").cloned().unwrap_or(Value::Null);
    let method = call
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let params = call
        .get("params")
        .cloned()
        .unwrap_or_else(|| Value::Array(Vec::new()));

    match method.as_str() {
        "getblockcount" => success(id, json!(chain.best_height())),
        "getblockhash" => {
            let height = params
                .as_array()
                .and_then(|arr| arr.first())
                .and_then(Value::as_u64);
            match height.and_then(|h| chain.hash_for_height(h)) {
                Some(hash) => success(id, Value::String(hash)),
                None => error(id, -8, "Block height out of range"),
            }
        }
        "getblock" => {
            let hash = params
                .as_array()
                .and_then(|arr| arr.first())
                .and_then(Value::as_str)
                .map(|value| value.to_string());
            match hash.and_then(|h| chain.hex_for_hash(&h)) {
                Some(hex) => success(id, Value::String(hex)),
                None => error(id, -5, "Block not found"),
            }
        }
        _ => error(id, -32601, format!("unknown method {method}")),
    }
}

fn success(id: Value, result: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "result": result,
        "id": id,
    })
}

fn error(id: Value, code: i64, message: impl Into<String>) -> Value {
    json!({
        "jsonrpc": "2.0",
        "error": {
            "code": code,
            "message": message.into(),
        },
        "id": id,
    })
}
