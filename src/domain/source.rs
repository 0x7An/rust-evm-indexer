use std::{fmt, str::FromStr};

use super::{
    DomainError, DomainResult,
    chain::{Address, BlockNumber, ChainId, NonEmptyId},
    require_non_empty,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SourceId(NonEmptyId);

impl SourceId {
    pub fn new(value: impl Into<String>) -> DomainResult<Self> {
        NonEmptyId::new("source id", value).map(Self)
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenStandard {
    Auto,
    Erc20,
    Erc721,
    Erc1155,
}

impl TokenStandard {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Erc20 => "erc20",
            Self::Erc721 => "erc721",
            Self::Erc1155 => "erc1155",
        }
    }
}

impl fmt::Display for TokenStandard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for TokenStandard {
    type Err = DomainError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "auto" => Ok(Self::Auto),
            "erc20" => Ok(Self::Erc20),
            "erc721" => Ok(Self::Erc721),
            "erc1155" => Ok(Self::Erc1155),
            _ => Err(DomainError::InvalidEnumValue {
                field: "token standard",
                value: value.to_string(),
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Source {
    pub id: SourceId,
    pub chain_id: ChainId,
    pub name: String,
    pub contract_address: Address,
    pub token_standard: TokenStandard,
    pub start_block: BlockNumber,
    pub enabled: bool,
}

impl Source {
    pub fn new(
        id: SourceId,
        chain_id: ChainId,
        name: impl Into<String>,
        contract_address: Address,
        token_standard: TokenStandard,
        start_block: BlockNumber,
        enabled: bool,
    ) -> DomainResult<Self> {
        Ok(Self {
            id,
            chain_id,
            name: require_non_empty("source name", name)?,
            contract_address,
            token_standard,
            start_block,
            enabled,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn address() -> Address {
        Address::new("0x1000000000000000000000000000000000000000").unwrap()
    }

    #[test]
    fn token_standard_parses_supported_values() {
        assert_eq!(
            "auto".parse::<TokenStandard>().unwrap(),
            TokenStandard::Auto
        );
        assert_eq!(
            "ERC721".parse::<TokenStandard>().unwrap(),
            TokenStandard::Erc721
        );
        assert!("erc777".parse::<TokenStandard>().is_err());
    }

    #[test]
    fn source_requires_non_empty_name() {
        let err = Source::new(
            SourceId::new("source-1").unwrap(),
            ChainId::new(1),
            " ",
            address(),
            TokenStandard::Erc721,
            BlockNumber::new(0),
            true,
        )
        .unwrap_err();

        assert_eq!(err, DomainError::EmptyValue("source name"));
    }
}
