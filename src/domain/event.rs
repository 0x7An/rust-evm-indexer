use super::{
    DomainError, DomainResult,
    chain::{Address, BlockHash, BlockNumber, ChainId, TransactionHash},
    source::{SourceId, TokenStandard},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LogIndex(u32);

impl LogIndex {
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u32 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BatchIndex(u32);

impl BatchIndex {
    pub const fn zero() -> Self {
        Self(0)
    }

    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u32 {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TokenId(String);

impl TokenId {
    pub fn new(value: impl Into<String>) -> DomainResult<Self> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(DomainError::EmptyValue("token id"));
        }
        Ok(Self(value))
    }

    pub fn fungible() -> Self {
        Self(String::new())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenAmount(String);

impl TokenAmount {
    pub fn new(value: impl Into<String>) -> DomainResult<Self> {
        let value = value.into();
        if value.is_empty() || !value.chars().all(|ch| ch.is_ascii_digit()) {
            return Err(DomainError::InvalidAmount(value));
        }
        Ok(Self(value))
    }

    pub fn one() -> Self {
        Self("1".to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EventIdentity {
    pub chain_id: ChainId,
    pub transaction_hash: TransactionHash,
    pub log_index: LogIndex,
}

impl EventIdentity {
    pub fn new(chain_id: ChainId, transaction_hash: TransactionHash, log_index: LogIndex) -> Self {
        Self {
            chain_id,
            transaction_hash,
            log_index,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LedgerEntryIdentity {
    pub event: EventIdentity,
    pub batch_index: BatchIndex,
}

impl LedgerEntryIdentity {
    pub fn new(event: EventIdentity, batch_index: BatchIndex) -> Self {
        Self { event, batch_index }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexedEvent {
    pub identity: EventIdentity,
    pub source_id: SourceId,
    pub block_number: BlockNumber,
    pub block_hash: BlockHash,
    pub contract_address: Address,
    pub event_name: String,
    pub finalized: bool,
    pub orphaned: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MovementType {
    Mint,
    Transfer,
    Burn,
}

impl MovementType {
    pub fn from_participants(from: &Address, to: &Address) -> DomainResult<Self> {
        match (from.is_zero(), to.is_zero()) {
            (true, false) => Ok(Self::Mint),
            (false, true) => Ok(Self::Burn),
            (false, false) => Ok(Self::Transfer),
            (true, true) => Err(DomainError::InvalidLedgerEntry(
                "from and to cannot both be the zero address",
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerEntry {
    pub identity: LedgerEntryIdentity,
    pub source_id: SourceId,
    pub token_standard: TokenStandard,
    pub movement_type: MovementType,
    pub operator: Option<Address>,
    pub from: Option<Address>,
    pub to: Option<Address>,
    pub token_id: TokenId,
    pub amount: TokenAmount,
    pub block_number: BlockNumber,
    pub block_hash: BlockHash,
    pub contract_address: Address,
}

impl LedgerEntry {
    pub fn new(
        identity: LedgerEntryIdentity,
        source_id: SourceId,
        token_standard: TokenStandard,
        operator: Option<Address>,
        from: Address,
        to: Address,
        token_id: TokenId,
        amount: TokenAmount,
        block_number: BlockNumber,
        block_hash: BlockHash,
        contract_address: Address,
    ) -> DomainResult<Self> {
        let movement_type = MovementType::from_participants(&from, &to)?;
        let from = (!from.is_zero()).then_some(from);
        let to = (!to.is_zero()).then_some(to);

        Ok(Self {
            identity,
            source_id,
            token_standard,
            movement_type,
            operator,
            from,
            to,
            token_id,
            amount,
            block_number,
            block_hash,
            contract_address,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenBalance {
    pub source_id: SourceId,
    pub holder: Address,
    pub token_id: TokenId,
    pub amount: TokenAmount,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tx_hash() -> TransactionHash {
        TransactionHash::new(format!("0x{}", "33".repeat(32))).unwrap()
    }

    fn block_hash() -> BlockHash {
        BlockHash::new(format!("0x{}", "44".repeat(32))).unwrap()
    }

    fn address(byte: &str) -> Address {
        Address::new(format!("0x{}", byte.repeat(20))).unwrap()
    }

    fn identity(batch_index: u32) -> LedgerEntryIdentity {
        LedgerEntryIdentity::new(
            EventIdentity::new(ChainId::new(1), tx_hash(), LogIndex::new(7)),
            BatchIndex::new(batch_index),
        )
    }

    #[test]
    fn event_identity_uses_chain_transaction_and_log_index() {
        let a = EventIdentity::new(ChainId::new(1), tx_hash(), LogIndex::new(7));
        let b = EventIdentity::new(ChainId::new(1), tx_hash(), LogIndex::new(8));

        assert_ne!(a, b);
    }

    #[test]
    fn ledger_entry_identity_adds_batch_index() {
        let a = identity(0);
        let b = identity(1);

        assert_ne!(a, b);
    }

    #[test]
    fn ledger_entry_infers_mint_and_drops_zero_sender() {
        let entry = LedgerEntry::new(
            identity(0),
            SourceId::new("source-1").unwrap(),
            TokenStandard::Erc721,
            None,
            Address::zero(),
            address("55"),
            TokenId::new("42").unwrap(),
            TokenAmount::one(),
            BlockNumber::new(100),
            block_hash(),
            address("66"),
        )
        .unwrap();

        assert_eq!(entry.movement_type, MovementType::Mint);
        assert!(entry.from.is_none());
        assert!(entry.to.is_some());
    }

    #[test]
    fn ledger_entry_rejects_zero_to_zero_movement() {
        let err = LedgerEntry::new(
            identity(0),
            SourceId::new("source-1").unwrap(),
            TokenStandard::Erc721,
            None,
            Address::zero(),
            Address::zero(),
            TokenId::new("42").unwrap(),
            TokenAmount::one(),
            BlockNumber::new(100),
            block_hash(),
            address("66"),
        )
        .unwrap_err();

        assert_eq!(
            err,
            DomainError::InvalidLedgerEntry("from and to cannot both be the zero address")
        );
    }

    #[test]
    fn token_amount_rejects_non_decimal_values() {
        assert!(TokenAmount::new("1.5").is_err());
        assert!(TokenAmount::new("").is_err());
        assert_eq!(TokenAmount::new("0").unwrap().as_str(), "0");
    }
}
