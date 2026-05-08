use std::{fmt, str::FromStr};

use super::{
    DomainError, DomainResult,
    chain::{BlockRange, ChainId, NonEmptyId},
    source::SourceId,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct JobId(NonEmptyId);

impl JobId {
    pub fn new(value: impl Into<String>) -> DomainResult<Self> {
        NonEmptyId::new("job id", value).map(Self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobType {
    IngestRange,
    BackfillRange,
    ReplayRange,
    VerifyReorg,
    RepairCheckpoint,
}

impl JobType {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::IngestRange => "INGEST_RANGE",
            Self::BackfillRange => "BACKFILL_RANGE",
            Self::ReplayRange => "REPLAY_RANGE",
            Self::VerifyReorg => "VERIFY_REORG",
            Self::RepairCheckpoint => "REPAIR_CHECKPOINT",
        }
    }
}

impl fmt::Display for JobType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for JobType {
    type Err = DomainError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "INGEST_RANGE" => Ok(Self::IngestRange),
            "BACKFILL_RANGE" => Ok(Self::BackfillRange),
            "REPLAY_RANGE" => Ok(Self::ReplayRange),
            "VERIFY_REORG" => Ok(Self::VerifyReorg),
            "REPAIR_CHECKPOINT" => Ok(Self::RepairCheckpoint),
            _ => Err(DomainError::InvalidEnumValue {
                field: "job type",
                value: value.to_string(),
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobStatus {
    Queued,
    Leased,
    Running,
    Succeeded,
    Failed,
    DeadLettered,
    Cancelled,
}

impl JobStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Leased => "leased",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::DeadLettered => "dead_lettered",
            Self::Cancelled => "cancelled",
        }
    }
}

impl fmt::Display for JobStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for JobStatus {
    type Err = DomainError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "queued" => Ok(Self::Queued),
            "leased" => Ok(Self::Leased),
            "running" => Ok(Self::Running),
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            "dead_lettered" => Ok(Self::DeadLettered),
            "cancelled" => Ok(Self::Cancelled),
            _ => Err(DomainError::InvalidEnumValue {
                field: "job status",
                value: value.to_string(),
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IdempotencyKey(NonEmptyId);

impl IdempotencyKey {
    pub fn new(value: impl Into<String>) -> DomainResult<Self> {
        NonEmptyId::new("idempotency key", value).map(Self)
    }

    pub fn ingest(source_id: &SourceId, range: BlockRange) -> Self {
        Self(
            NonEmptyId::new(
                "idempotency key",
                format!(
                    "ingest:{}:{}:{}",
                    source_id.as_str(),
                    range.from.get(),
                    range.to.get()
                ),
            )
            .expect("generated idempotency key is non-empty"),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Job {
    pub id: JobId,
    pub job_type: JobType,
    pub status: JobStatus,
    pub source_id: Option<SourceId>,
    pub chain_id: ChainId,
    pub range: Option<BlockRange>,
    pub idempotency_key: IdempotencyKey,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::chain::BlockNumber;

    #[test]
    fn job_type_parses_required_values() {
        assert_eq!(
            "INGEST_RANGE".parse::<JobType>().unwrap(),
            JobType::IngestRange
        );
        assert!("UNKNOWN".parse::<JobType>().is_err());
    }

    #[test]
    fn job_status_parses_required_values() {
        assert_eq!("queued".parse::<JobStatus>().unwrap(), JobStatus::Queued);
        assert_eq!(
            "dead_lettered".parse::<JobStatus>().unwrap(),
            JobStatus::DeadLettered
        );
        assert!("approved".parse::<JobStatus>().is_err());
    }

    #[test]
    fn ingest_idempotency_key_is_deterministic() {
        let source_id = SourceId::new("source-1").unwrap();
        let range = BlockRange::new(BlockNumber::new(5), BlockNumber::new(10)).unwrap();

        let key = IdempotencyKey::ingest(&source_id, range);

        assert_eq!(key.0.as_str(), "ingest:source-1:5:10");
    }
}
