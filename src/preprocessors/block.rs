use crate::preprocessors::sized_queue::QueueByteSize;
use bitcoin::hashes::Hash;
use bitcoin::BlockHash;

/// Wrapper type that carries pre-processed data alongside block metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreProcessedBlock<T> {
    height: u64,
    block_hash: BlockHash,
    previous_hash: BlockHash,
    data: T,
}

impl<T> PreProcessedBlock<T> {
    pub fn new(height: u64, block_hash: BlockHash, previous_hash: BlockHash, data: T) -> Self {
        Self {
            height,
            block_hash,
            previous_hash,
            data,
        }
    }

    pub fn height(&self) -> u64 {
        self.height
    }

    pub fn block_hash(&self) -> &BlockHash {
        &self.block_hash
    }

    pub fn previous_hash(&self) -> &BlockHash {
        &self.previous_hash
    }

    pub fn data(&self) -> &T {
        &self.data
    }

    pub fn into_inner(self) -> T {
        self.data
    }

    pub fn map<U, F>(self, f: F) -> PreProcessedBlock<U>
    where
        F: FnOnce(T) -> U,
    {
        PreProcessedBlock {
            height: self.height,
            block_hash: self.block_hash,
            previous_hash: self.previous_hash,
            data: f(self.data),
        }
    }
}

impl<T: QueueByteSize> PreProcessedBlock<T> {
    /// Returns the estimated number of bytes retained in the queue when this block is enqueued.
    ///
    /// The calculation includes the block metadata kept alongside the pre-processed payload so the
    /// queue budget aligns with the actual memory footprint visible to `OrderedBlockQueue`.
    pub fn queue_bytes(&self) -> usize {
        const HASH_BYTES: usize = BlockHash::LEN;
        let metadata_bytes =
            core::mem::size_of_val(&self.height).saturating_add(HASH_BYTES.saturating_mul(2));
        metadata_bytes.saturating_add(self.data.queue_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::hashes::Hash;

    fn dummy_hash(seed: u8) -> BlockHash {
        let mut bytes = [0u8; 32];
        bytes[0] = seed;
        BlockHash::from_slice(&bytes).expect("valid hash")
    }

    #[test]
    fn map_transforms_data_but_keeps_metadata() {
        let block = PreProcessedBlock::new(42, dummy_hash(1), dummy_hash(2), 10u32);
        let mapped = block.map(|value| value.to_string());

        assert_eq!(mapped.height(), 42);
        assert_eq!(mapped.block_hash(), &dummy_hash(1));
        assert_eq!(mapped.previous_hash(), &dummy_hash(2));
        assert_eq!(mapped.data(), "10");
    }
}
