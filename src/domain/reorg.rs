use super::{
    DomainResult,
    chain::{BlockHash, BlockRange, ChainId, NonEmptyId},
    job::JobId,
    source::SourceId,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ReorgEventId(NonEmptyId);

impl ReorgEventId {
    pub fn new(value: impl Into<String>) -> DomainResult<Self> {
        NonEmptyId::new("reorg event id", value).map(Self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReorgEvent {
    pub id: ReorgEventId,
    pub source_id: SourceId,
    pub chain_id: ChainId,
    pub range: BlockRange,
    pub expected_block_hash: Option<BlockHash>,
    pub actual_block_hash: Option<BlockHash>,
    pub replay_job_id: Option<JobId>,
}

impl ReorgEvent {
    pub fn new(
        id: ReorgEventId,
        source_id: SourceId,
        chain_id: ChainId,
        range: BlockRange,
        expected_block_hash: Option<BlockHash>,
        actual_block_hash: Option<BlockHash>,
        replay_job_id: Option<JobId>,
    ) -> Self {
        Self {
            id,
            source_id,
            chain_id,
            range,
            expected_block_hash,
            actual_block_hash,
            replay_job_id,
        }
    }
}
