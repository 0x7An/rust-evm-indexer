use std::fmt;

use super::{DomainError, DomainResult, require_non_empty};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ChainId(u64);

impl ChainId {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for ChainId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlockNumber(u64);

impl BlockNumber {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

impl fmt::Display for BlockNumber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BlockHash(String);

impl BlockHash {
    pub fn new(value: impl Into<String>) -> DomainResult<Self> {
        validate_hex_hash("block hash", value.into()).map(Self)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TransactionHash(String);

impl TransactionHash {
    pub fn new(value: impl Into<String>) -> DomainResult<Self> {
        validate_hex_hash("transaction hash", value.into()).map(Self)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Address(String);

impl Address {
    pub fn new(value: impl Into<String>) -> DomainResult<Self> {
        let value = value.into();
        let normalized = value.to_ascii_lowercase();

        if normalized.len() != 42
            || !normalized.starts_with("0x")
            || !normalized[2..].chars().all(|ch| ch.is_ascii_hexdigit())
        {
            return Err(DomainError::InvalidAddress(value));
        }

        Ok(Self(normalized))
    }

    pub fn zero() -> Self {
        Self("0x0000000000000000000000000000000000000000".to_string())
    }

    pub fn is_zero(&self) -> bool {
        self.0 == Self::zero().0
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockRef {
    pub chain_id: ChainId,
    pub number: BlockNumber,
    pub hash: BlockHash,
}

impl BlockRef {
    pub fn new(chain_id: ChainId, number: BlockNumber, hash: BlockHash) -> Self {
        Self {
            chain_id,
            number,
            hash,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockRange {
    pub from: BlockNumber,
    pub to: BlockNumber,
}

impl BlockRange {
    pub fn new(from: BlockNumber, to: BlockNumber) -> DomainResult<Self> {
        if from > to {
            return Err(DomainError::InvalidRange { from, to });
        }

        Ok(Self { from, to })
    }

    pub fn contains(&self, block: BlockNumber) -> bool {
        self.from <= block && block <= self.to
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NonEmptyId(String);

impl NonEmptyId {
    pub fn new(field: &'static str, value: impl Into<String>) -> DomainResult<Self> {
        require_non_empty(field, value).map(Self)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

fn validate_hex_hash(field: &'static str, value: String) -> DomainResult<String> {
    let normalized = value.to_ascii_lowercase();

    if normalized.len() != 66
        || !normalized.starts_with("0x")
        || !normalized[2..].chars().all(|ch| ch.is_ascii_hexdigit())
    {
        return Err(DomainError::InvalidHash { field, value });
    }

    Ok(normalized)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash() -> BlockHash {
        BlockHash::new(format!("0x{}", "11".repeat(32))).unwrap()
    }

    #[test]
    fn block_range_rejects_inverted_ranges() {
        let err = BlockRange::new(BlockNumber::new(10), BlockNumber::new(9)).unwrap_err();

        assert_eq!(
            err,
            DomainError::InvalidRange {
                from: BlockNumber::new(10),
                to: BlockNumber::new(9),
            }
        );
    }

    #[test]
    fn block_range_accepts_equal_bounds() {
        let range = BlockRange::new(BlockNumber::new(10), BlockNumber::new(10)).unwrap();

        assert!(range.contains(BlockNumber::new(10)));
        assert!(!range.contains(BlockNumber::new(11)));
    }

    #[test]
    fn address_normalizes_and_detects_zero() {
        let address = Address::new("0xA000000000000000000000000000000000000000").unwrap();

        assert_eq!(
            address.as_str(),
            "0xa000000000000000000000000000000000000000"
        );
        assert!(Address::zero().is_zero());
        assert!(!address.is_zero());
    }

    #[test]
    fn hash_validation_requires_32_bytes_hex() {
        assert!(BlockHash::new("0x1234").is_err());
        assert_eq!(hash().as_str().len(), 66);
    }
}
