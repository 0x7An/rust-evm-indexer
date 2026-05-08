use super::{
    DomainError, DomainResult,
    chain::{BlockNumber, BlockRef},
    source::SourceId,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checkpoint {
    pub source_id: SourceId,
    pub processed: BlockRef,
    pub finalized_block: BlockNumber,
}

impl Checkpoint {
    pub fn new(
        source_id: SourceId,
        processed: BlockRef,
        finalized_block: BlockNumber,
    ) -> DomainResult<Self> {
        if processed.number > finalized_block {
            return Err(DomainError::InvalidCheckpoint {
                processed: processed.number,
                finalized: finalized_block,
            });
        }

        Ok(Self {
            source_id,
            processed,
            finalized_block,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::chain::{BlockHash, ChainId};

    fn block_ref(number: u64) -> BlockRef {
        BlockRef::new(
            ChainId::new(1),
            BlockNumber::new(number),
            BlockHash::new(format!("0x{}", "22".repeat(32))).unwrap(),
        )
    }

    #[test]
    fn checkpoint_rejects_processed_block_newer_than_finalized_block() {
        let err = Checkpoint::new(
            SourceId::new("source-1").unwrap(),
            block_ref(10),
            BlockNumber::new(9),
        )
        .unwrap_err();

        assert_eq!(
            err,
            DomainError::InvalidCheckpoint {
                processed: BlockNumber::new(10),
                finalized: BlockNumber::new(9),
            }
        );
    }
}
