//! Utility helpers for working with raw Bitcoin block data returned by RPC
//! calls (hex decoding plus hash extraction).

use anyhow::{Context, Result};
use bitcoin::{consensus, Block, BlockHash};

/// Decodes a hexadecimal string into a Bitcoin [`Block`].
///
/// The input string is trimmed before decoding. Returns an error if the hex is invalid
/// or the deserialization fails.
pub fn hex_to_block(hex: &str) -> Result<Block> {
    let bytes = hex::decode(hex.trim()).context("invalid block hex")?;
    consensus::deserialize::<Block>(&bytes).context("failed to deserialize block bytes")
}

/// Extracts the current block hash and previous block hash from a [`Block`].
///
/// Returns a tuple of `(block_hash, previous_hash)`.
pub fn extract_hashes(block: &Block) -> (BlockHash, BlockHash) {
    (block.block_hash(), block.header.prev_blockhash)
}

#[cfg(test)]
mod tests {
    use super::*;

    const GENESIS_BLOCK_HEX: &str = "0100000000000000000000000000000000000000000000000000000000000000000000003ba3edfd7a7b12b27ac72c3e67768f617fc81bc3888a51323a9fb8aa4b1e5e4a29ab5f49ffff001d1dac2b7c0101000000010000000000000000000000000000000000000000000000000000000000000000ffffffff4d04ffff001d0104455468652054696d65732030332f4a616e2f32303039204368616e63656c6c6f72206f6e206272696e6b206f66207365636f6e64206261696c6f757420666f722062616e6b73ffffffff0100f2052a01000000434104678afdb0fe5548271967f1a67130b7105cd6a828e03909a67962e0ea1f61deb649f6bc3f4cef38c4f35504e51ec112de5c384df7ba0b8d578a4c702b6bf11d5fac00000000";

    #[test]
    fn decodes_block_from_hex() {
        let block = hex_to_block(GENESIS_BLOCK_HEX).expect("genesis block must decode");
        assert_eq!(block.txdata.len(), 1);
        assert_eq!(
            block.block_hash().to_string(),
            "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
        );
    }

    #[test]
    fn extracts_block_hashes() {
        let block = hex_to_block(GENESIS_BLOCK_HEX).expect("genesis block must decode");
        let (current, previous) = extract_hashes(&block);
        assert_eq!(
            current.to_string(),
            "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
        );
        assert_eq!(
            previous.to_string(),
            "0000000000000000000000000000000000000000000000000000000000000000"
        );
    }
}
