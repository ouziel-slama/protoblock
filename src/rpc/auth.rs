//! HTTP authentication helpers for wiring Bitcoin Core credentials into the
//! underlying `jsonrpsee` client builder.

use anyhow::{Context, Result};
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use jsonrpsee::http_client::{HeaderMap, HeaderValue};

pub(crate) fn build_auth_headers(user: &str, password: &str) -> Result<HeaderMap> {
    let mut headers = HeaderMap::new();
    let credentials = format!("{user}:{password}");
    let encoded = BASE64_STANDARD.encode(credentials);
    let value = HeaderValue::from_str(&format!("Basic {encoded}"))
        .context("failed to build Authorization header")?;
    headers.insert("Authorization", value);
    Ok(headers)
}
