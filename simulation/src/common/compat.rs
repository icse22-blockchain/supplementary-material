use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use web3::types::{Bytes, Index, Transaction as Web3Transaction, H160, H256, U256, U64};

// source: https://github.com/tomusdrw/rust-web3/blob/9e80f896bb49605079202fa53b6dc1dfd2430c32/src/types/transaction.rs
#[derive(Debug, Default, Clone, PartialEq, Deserialize, Serialize)]
pub struct Transaction {
    /// Hash
    pub hash: H256,
    /// Nonce
    pub nonce: U256,
    /// Block hash. None when pending.
    #[serde(rename = "blockHash")]
    pub block_hash: Option<H256>,
    /// Block number. None when pending.
    #[serde(rename = "blockNumber")]
    pub block_number: Option<U64>,
    /// Transaction Index. None when pending.
    #[serde(rename = "transactionIndex")]
    pub transaction_index: Option<Index>,
    /// Sender
    pub from: H160,
    /// Recipient (None when contract creation)
    pub to: Option<H160>,
    /// Transfered value
    pub value: U256,
    /// Gas Price
    #[serde(rename = "gasPrice")]
    pub gas_price: U256,
    /// Gas amount
    pub gas: U256,
    /// Input data
    pub input: Bytes,
    /// Raw transaction data
    #[serde(default)]
    pub raw: Option<Bytes>,
}

impl TryFrom<Web3Transaction> for Transaction {
    type Error = &'static str;

    fn try_from(tx: Web3Transaction) -> Result<Self, Self::Error> {
        let from = match tx.from {
            Some(f) => f,
            None => return Err("tx.from should not be empty"),
        };

        Ok(Transaction {
            hash: tx.hash,
            nonce: tx.nonce,
            block_hash: tx.block_hash,
            block_number: tx.block_number,
            transaction_index: tx.transaction_index,
            from,
            to: tx.to,
            value: tx.value,
            gas_price: tx.gas_price,
            gas: tx.gas,
            input: tx.input,
            raw: tx.raw,
        })
    }
}
