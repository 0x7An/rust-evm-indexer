use anyhow::{Context, Result, anyhow, bail};
use bigdecimal::num_bigint::BigUint;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha3::{Digest, Keccak256};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TokenStandard {
    Erc20,
    Erc721,
    Erc1155,
}

impl TokenStandard {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Erc20 => "erc20",
            Self::Erc721 => "erc721",
            Self::Erc1155 => "erc1155",
        }
    }
}

impl std::str::FromStr for TokenStandard {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value.to_ascii_lowercase().as_str() {
            "erc20" => Ok(Self::Erc20),
            "erc721" => Ok(Self::Erc721),
            "erc1155" => Ok(Self::Erc1155),
            _ => bail!("unsupported token standard: {value}"),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcLog {
    pub address: String,
    pub topics: Vec<String>,
    pub data: String,
    pub block_number: String,
    pub transaction_hash: String,
    #[serde(default)]
    pub transaction_index: Option<String>,
    pub log_index: String,
    pub block_hash: String,
    #[serde(skip)]
    pub block_timestamp: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedLedgerEntry {
    pub event_name: String,
    pub token_standard: TokenStandard,
    pub operator: Option<String>,
    pub from: String,
    pub to: String,
    pub token_id: String,
    pub amount: String,
    pub batch_index: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedLog {
    pub event_name: String,
    pub entries: Vec<DecodedLedgerEntry>,
}

pub fn supported_topic0_values(standard: TokenStandard) -> Vec<String> {
    match standard {
        TokenStandard::Erc20 | TokenStandard::Erc721 => {
            vec![event_topic("Transfer(address,address,uint256)")]
        }
        TokenStandard::Erc1155 => vec![
            event_topic("TransferSingle(address,address,address,uint256,uint256)"),
            event_topic("TransferBatch(address,address,address,uint256[],uint256[])"),
        ],
    }
}

pub fn decode_log(log: &RpcLog, standard: TokenStandard) -> Result<Option<DecodedLog>> {
    let Some(topic0) = log.topics.first() else {
        return Ok(None);
    };

    let topic0 = topic0.to_ascii_lowercase();
    let transfer = event_topic("Transfer(address,address,uint256)");
    let transfer_single = event_topic("TransferSingle(address,address,address,uint256,uint256)");
    let transfer_batch = event_topic("TransferBatch(address,address,address,uint256[],uint256[])");

    if topic0 == transfer && matches!(standard, TokenStandard::Erc20 | TokenStandard::Erc721) {
        return decode_transfer(log, standard).map(Some);
    }

    if topic0 == transfer_single && standard == TokenStandard::Erc1155 {
        return decode_transfer_single(log).map(Some);
    }

    if topic0 == transfer_batch && standard == TokenStandard::Erc1155 {
        return decode_transfer_batch(log).map(Some);
    }

    Ok(None)
}

pub fn event_topic(signature: &str) -> String {
    let mut hasher = Keccak256::new();
    hasher.update(signature.as_bytes());
    format!("0x{}", hex::encode(hasher.finalize()))
}

pub fn parse_hex_u64(value: &str) -> Result<u64> {
    u64::from_str_radix(strip_0x(value)?, 16).with_context(|| format!("parse hex u64: {value}"))
}

fn decode_transfer(log: &RpcLog, standard: TokenStandard) -> Result<DecodedLog> {
    let (from, to, token_id, amount) = match standard {
        TokenStandard::Erc20 => {
            require_topic_count(log, 3)?;
            let amount = decode_word_as_decimal(&word_at(&log.data, 0)?)?;
            (
                topic_address(&log.topics[1])?,
                topic_address(&log.topics[2])?,
                String::new(),
                amount,
            )
        }
        TokenStandard::Erc721 => {
            require_topic_count(log, 4)?;
            (
                topic_address(&log.topics[1])?,
                topic_address(&log.topics[2])?,
                decode_word_as_decimal(strip_0x(&log.topics[3])?)?,
                "1".to_string(),
            )
        }
        TokenStandard::Erc1155 => unreachable!("ERC-1155 transfer uses dedicated events"),
    };

    Ok(DecodedLog {
        event_name: "Transfer".to_string(),
        entries: vec![DecodedLedgerEntry {
            event_name: "Transfer".to_string(),
            token_standard: standard,
            operator: None,
            from,
            to,
            token_id,
            amount,
            batch_index: 0,
        }],
    })
}

fn decode_transfer_single(log: &RpcLog) -> Result<DecodedLog> {
    require_topic_count(log, 4)?;
    let token_id = decode_word_as_decimal(&word_at(&log.data, 0)?)?;
    let amount = decode_word_as_decimal(&word_at(&log.data, 1)?)?;

    Ok(DecodedLog {
        event_name: "TransferSingle".to_string(),
        entries: vec![DecodedLedgerEntry {
            event_name: "TransferSingle".to_string(),
            token_standard: TokenStandard::Erc1155,
            operator: Some(topic_address(&log.topics[1])?),
            from: topic_address(&log.topics[2])?,
            to: topic_address(&log.topics[3])?,
            token_id,
            amount,
            batch_index: 0,
        }],
    })
}

fn decode_transfer_batch(log: &RpcLog) -> Result<DecodedLog> {
    require_topic_count(log, 4)?;
    let data = decode_data_words(&log.data)?;
    let ids_offset =
        word_decimal_as_usize(data.first().ok_or_else(|| anyhow!("missing ids offset"))?)?;
    let amounts_offset = word_decimal_as_usize(
        data.get(1)
            .ok_or_else(|| anyhow!("missing amounts offset"))?,
    )?;

    let ids = decode_dynamic_uint_array(&data, ids_offset)?;
    let amounts = decode_dynamic_uint_array(&data, amounts_offset)?;
    if ids.len() != amounts.len() {
        bail!(
            "TransferBatch ids/amounts length mismatch: {} != {}",
            ids.len(),
            amounts.len()
        );
    }

    let operator = topic_address(&log.topics[1])?;
    let from = topic_address(&log.topics[2])?;
    let to = topic_address(&log.topics[3])?;
    let entries = ids
        .into_iter()
        .zip(amounts)
        .enumerate()
        .map(|(index, (token_id, amount))| DecodedLedgerEntry {
            event_name: "TransferBatch".to_string(),
            token_standard: TokenStandard::Erc1155,
            operator: Some(operator.clone()),
            from: from.clone(),
            to: to.clone(),
            token_id,
            amount,
            batch_index: index as i32,
        })
        .collect();

    Ok(DecodedLog {
        event_name: "TransferBatch".to_string(),
        entries,
    })
}

fn require_topic_count(log: &RpcLog, expected: usize) -> Result<()> {
    if log.topics.len() != expected {
        bail!(
            "expected {expected} topics for {}, got {}",
            log.transaction_hash,
            log.topics.len()
        );
    }
    Ok(())
}

fn topic_address(topic: &str) -> Result<String> {
    let topic = strip_0x(topic)?;
    if topic.len() != 64 {
        bail!("topic must be 32 bytes");
    }
    Ok(format!("0x{}", &topic[24..64]).to_ascii_lowercase())
}

fn word_at(data: &str, index: usize) -> Result<String> {
    let data = strip_0x(data)?;
    let start = index * 64;
    let end = start + 64;
    if data.len() < end {
        bail!("data does not contain word {index}");
    }
    Ok(data[start..end].to_string())
}

fn decode_data_words(data: &str) -> Result<Vec<String>> {
    let data = strip_0x(data)?;
    if data.len() % 64 != 0 {
        bail!("ABI data length must be a multiple of 32 bytes");
    }
    Ok(data
        .as_bytes()
        .chunks(64)
        .map(|chunk| std::str::from_utf8(chunk).expect("hex is utf8").to_string())
        .collect())
}

fn decode_dynamic_uint_array(words: &[String], offset_bytes: usize) -> Result<Vec<String>> {
    if offset_bytes % 32 != 0 {
        bail!("dynamic array offset must be 32-byte aligned");
    }
    let start = offset_bytes / 32;
    let len_word = words
        .get(start)
        .ok_or_else(|| anyhow!("dynamic array length out of bounds"))?;
    let len = word_decimal_as_usize(len_word)?;

    (0..len)
        .map(|index| {
            words
                .get(start + 1 + index)
                .ok_or_else(|| anyhow!("dynamic array item out of bounds"))
                .and_then(|word| decode_word_as_decimal(word))
        })
        .collect()
}

fn word_decimal_as_usize(word: &str) -> Result<usize> {
    let value = decode_word_as_decimal(word)?;
    value
        .parse::<usize>()
        .with_context(|| format!("parse ABI word as usize: {value}"))
}

fn decode_word_as_decimal(word: &str) -> Result<String> {
    let word = strip_optional_0x(word);
    if word.len() != 64 {
        bail!("ABI word must be 32 bytes");
    }
    let value =
        BigUint::parse_bytes(word.as_bytes(), 16).ok_or_else(|| anyhow!("invalid uint256 word"))?;
    Ok(value.to_string())
}

fn strip_0x(value: &str) -> Result<&str> {
    value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
        .ok_or_else(|| anyhow!("hex value must start with 0x: {value}"))
}

fn strip_optional_0x(value: &str) -> &str {
    value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
        .unwrap_or(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topic_address_word(byte: &str) -> String {
        format!("0x{}{}", "00".repeat(12), byte.repeat(20))
    }

    #[test]
    fn computes_standard_event_topics() {
        assert_eq!(
            event_topic("Transfer(address,address,uint256)"),
            "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
        );
    }

    #[test]
    fn decodes_erc721_transfer() {
        let log = RpcLog {
            address: "0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d".to_string(),
            topics: vec![
                event_topic("Transfer(address,address,uint256)"),
                topic_address_word("11"),
                topic_address_word("22"),
                format!("0x{:064x}", 42),
            ],
            data: "0x".to_string(),
            block_number: "0x1".to_string(),
            transaction_hash: format!("0x{}", "33".repeat(32)),
            transaction_index: Some("0x0".to_string()),
            log_index: "0x0".to_string(),
            block_hash: format!("0x{}", "44".repeat(32)),
            block_timestamp: None,
        };

        let decoded = decode_log(&log, TokenStandard::Erc721).unwrap().unwrap();
        assert_eq!(decoded.entries.len(), 1);
        assert_eq!(decoded.entries[0].token_id, "42");
        assert_eq!(decoded.entries[0].amount, "1");
    }
}
