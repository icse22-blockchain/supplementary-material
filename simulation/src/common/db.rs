use crate::compat::Transaction as DbTransaction;
use crate::rpc;
use crate::transaction_info::{parse_accesses, parse_tx_hash, TransactionInfo};
use rocksdb::{Error, Options, SliceTransform, DB};
use std::collections::HashMap;
use std::convert::TryInto;
use web3::types::{Transaction as Web3Transaction, TransactionReceipt, U256};

pub fn open_traces(path: &str) -> DB {
    let prefix_extractor = SliceTransform::create_fixed_prefix(8);

    let mut opts = Options::default();
    opts.create_if_missing(false);
    opts.set_prefix_extractor(prefix_extractor);

    DB::open_for_read_only(&opts, path, /* error_if_log_file_exist: */ true)
        .expect("db open should succeed")
}

// note: this will get tx infos in the wrong order!
pub fn tx_infos_deprecated(db: &DB, block: u64) -> Vec<TransactionInfo> {
    let prefix = format!("{:0>8}", block);
    let iter = db.prefix_iterator(prefix.as_bytes());

    iter.status().unwrap();

    iter.map(|(key, value)| {
        let key = std::str::from_utf8(&*key).expect("key read is valid string");
        let value = std::str::from_utf8(&*value).expect("value read is valid string");

        TransactionInfo {
            tx_hash: parse_tx_hash(key).to_owned(),
            accesses: parse_accesses(value).to_owned(),
        }
    })
    .collect()
}

pub fn tx_infos(db: &DB, block: u64, infos: &Vec<rpc::TxInfo>) -> Vec<TransactionInfo> {
    let prefix = format!("{:0>8}", block);
    let iter = db.prefix_iterator(prefix.as_bytes());

    iter.status().unwrap();

    // get results from db
    let mut txs_unordered: HashMap<String, TransactionInfo> = iter
        .map(|(key, value)| {
            let key = std::str::from_utf8(&*key).expect("key read is valid string");
            let value = std::str::from_utf8(&*value).expect("value read is valid string");

            let tx_hash = parse_tx_hash(key).to_owned();

            let tx = TransactionInfo {
                tx_hash: tx_hash.clone(),
                accesses: parse_accesses(value).to_owned(),
            };

            (tx_hash, tx)
        })
        .collect();

    // order results based on `infos`
    assert_eq!(infos.len(), txs_unordered.len());
    let mut res = vec![];

    for hash in infos.iter().map(|i| i.hash) {
        let tx = txs_unordered
            .remove(&format!("{:?}", hash))
            .expect("hash should exist");

        res.push(tx);
    }

    res
}

pub struct RpcDb {
    db: DB,
}

impl RpcDb {
    pub fn open(path: &str) -> Result<RpcDb, Error> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = DB::open(&opts, path)?;
        Ok(RpcDb { db })
    }

    pub fn open_for_read_only(path: &str) -> Result<RpcDb, Error> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = DB::open_for_read_only(&opts, path, /* error_if_log_file_exist: */ true)?;
        Ok(RpcDb { db })
    }

    fn txs_key(block: u64) -> Vec<u8> {
        format!("{:0>8}-txs", block).as_bytes().to_vec()
    }

    fn receipts_key(block: u64) -> Vec<u8> {
        format!("{:0>8}-rec", block).as_bytes().to_vec()
    }

    pub fn put_txs(&mut self, block: u64, txs: Vec<Web3Transaction>) -> Result<(), Error> {
        let txs: Vec<DbTransaction> = txs
            .into_iter()
            .map(|tx| tx.try_into().expect("tx conversion failed"))
            .collect();

        let key = RpcDb::txs_key(block);
        let value = rmp_serde::to_vec(&txs).expect("serialize should succeed");
        self.db.put(key, value)
    }

    pub fn get_txs(&self, block: u64) -> Result<Option<Vec<DbTransaction>>, Error> {
        let key = RpcDb::txs_key(block);

        match self.db.get(&key)? {
            None => Ok(None),
            Some(raw) => Ok(Some(rmp_serde::from_slice(&raw[..]).unwrap())),
        }
    }

    pub fn tx_infos(&self, block: u64) -> Result<Option<Vec<rpc::TxInfo>>, Error> {
        let txs = match self.get_txs(block)? {
            None => return Ok(None),
            Some(txs) => txs,
        };

        let infos = txs
            .into_iter()
            .map(|tx| rpc::TxInfo {
                hash: tx.hash,
                from: tx.from,
                to: tx.to,
                gas_limit: tx.gas,
            })
            .collect();

        Ok(Some(infos))
    }

    pub fn put_receipts(
        &mut self,
        block: u64,
        receipts: Vec<TransactionReceipt>,
    ) -> Result<(), Error> {
        let key = RpcDb::receipts_key(block);
        let value = rmp_serde::to_vec(&receipts).expect("serialize should succeed");
        self.db.put(key, value)
    }

    pub fn get_receipts(&self, block: u64) -> Result<Option<Vec<TransactionReceipt>>, Error> {
        let key = RpcDb::receipts_key(block);

        match self.db.get(&key)? {
            None => Ok(None),
            Some(raw) => Ok(Some(rmp_serde::from_slice(&raw[..]).unwrap())),
        }
    }

    pub fn gas_used(&self, block: u64) -> Result<Option<Vec<U256>>, Error> {
        let receipts = match self.get_receipts(block)? {
            None => return Ok(None),
            Some(rs) => rs,
        };

        let gas = receipts
            .into_iter()
            .map(|r| r.gas_used.expect("gas_used exists"))
            .collect();

        Ok(Some(gas))
    }
}

#[rustfmt::skip]
#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use serial_test::serial;

    const BLOCK: u64 = 5232800;
    const DB_PATH: &str = "experiment/db/_rpc_db";

    fn db() -> Result<RpcDb, Error>  {
        let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        d.push(DB_PATH);
        let path = d.into_os_string().into_string().unwrap();
        RpcDb::open(&path)
    }

    #[test]
    #[serial]
    fn test_block_txs() {
        let db = db().expect("can open db");

        let txs = db.get_txs(BLOCK)
            .expect("query succeeds")
            .expect("block exists");

        assert_eq!(txs.len(), 88);

        assert_eq!(txs[0].hash, "c68501cb5b3eda0fe845c30864a5657c9cf71d492c78f24b92e3d12986ae705b".parse().unwrap());
        assert_eq!(txs[0].to, Some("9ff305d9f7692a9b45e4f9bce8be4e98992eddde".parse().unwrap()));
        assert_eq!(txs[0].gas, U256::from(21_000));

        assert_eq!(txs[1].hash, "f9f3be29b6c70e3032a57e141c8374cc7cfb2b4006a640d680411af024b06179".parse().unwrap());
        assert_eq!(txs[1].to, Some("179631c363eef2cfec04f2354476f7b407ed031d".parse().unwrap()));
        assert_eq!(txs[1].gas, U256::from(150_000));

       // ...

        assert_eq!(txs[87].hash, "76da1628c18e0096ed0a24d37adf110ee39fea04c8bed36dfa7065c016f5d4d3".parse().unwrap());
        assert_eq!(txs[87].to, Some("d2f81cd7a20d60c0d558496c7169a20968389b40".parse().unwrap()));
        assert_eq!(txs[87].gas, U256::from(36_195));
    }

    #[test]
    #[serial]
    fn test_tx_infos() {
        let db = db().expect("can open db");

        let infos = db.tx_infos(BLOCK)
            .expect("query succeeds")
            .expect("block exists");

        assert_eq!(infos.len(), 88);

        assert_eq!(infos[0].hash, "c68501cb5b3eda0fe845c30864a5657c9cf71d492c78f24b92e3d12986ae705b".parse().unwrap());
        assert_eq!(infos[0].to, Some("9ff305d9f7692a9b45e4f9bce8be4e98992eddde".parse().unwrap()));
        assert_eq!(infos[0].gas_limit, U256::from(21_000));

        assert_eq!(infos[1].hash, "f9f3be29b6c70e3032a57e141c8374cc7cfb2b4006a640d680411af024b06179".parse().unwrap());
        assert_eq!(infos[1].to, Some("179631c363eef2cfec04f2354476f7b407ed031d".parse().unwrap()));
        assert_eq!(infos[1].gas_limit, U256::from(150_000));

        // ...

        assert_eq!(infos[87].hash, "76da1628c18e0096ed0a24d37adf110ee39fea04c8bed36dfa7065c016f5d4d3".parse().unwrap());
        assert_eq!(infos[87].to, Some("d2f81cd7a20d60c0d558496c7169a20968389b40".parse().unwrap()));
        assert_eq!(infos[87].gas_limit, U256::from(36_195));
    }

    #[test]
    #[serial]
    fn test_get_receipts() {
        let db = db().expect("can open db");

        let receipts = db.get_receipts(BLOCK)
            .expect("query succeeds")
            .expect("block exists");

        assert_eq!(receipts.len(), 88);

        assert_eq!(receipts[0].transaction_hash, "c68501cb5b3eda0fe845c30864a5657c9cf71d492c78f24b92e3d12986ae705b".parse().unwrap());
        assert_eq!(receipts[0].gas_used, Some(U256::from(21_000)));

        assert_eq!(receipts[1].transaction_hash, "f9f3be29b6c70e3032a57e141c8374cc7cfb2b4006a640d680411af024b06179".parse().unwrap());
        assert_eq!(receipts[1].gas_used, Some(U256::from(21_000)));

        // ...

        assert_eq!(receipts[87].transaction_hash, "76da1628c18e0096ed0a24d37adf110ee39fea04c8bed36dfa7065c016f5d4d3".parse().unwrap());
        assert_eq!(receipts[87].gas_used, Some(U256::from(24_130)));
    }

    #[test]
    #[serial]
    fn test_gas_used() {
        let db = db().expect("can open db");

        let gas_used = db.gas_used(BLOCK)
            .expect("query succeeds")
            .expect("block exists");

        assert_eq!(gas_used.len(), 88);
        assert_eq!(gas_used[0], U256::from(21_000));
        assert_eq!(gas_used[1], U256::from(21_000));
        // ...
        assert_eq!(gas_used[87], U256::from(24_130));
    }
}
