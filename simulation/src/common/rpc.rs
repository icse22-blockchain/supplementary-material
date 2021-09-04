use futures::{stream, StreamExt};
use tokio::task::JoinError;
use web3::{
    transports,
    types::{BlockId, BlockNumber, Transaction, TransactionReceipt, H160, H256, U256},
    Transport, Web3 as Web3Generic,
};

type Web3 = Web3Generic<transports::Http>;

pub struct TxInfo {
    pub hash: H256,
    pub from: H160,
    pub to: Option<H160>,
    pub gas_limit: U256,
}

// retrieve tx gas using `eth_getTransactionReceipt`
pub async fn gas_used(web3: &Web3, tx_hash: &str) -> Result<Option<U256>, web3::Error> {
    let tx_hash = tx_hash
        .trim_start_matches("0x")
        .parse()
        .expect("Unable to parse tx-hash");

    let gas = web3
        .eth()
        .transaction_receipt(tx_hash)
        .await?
        .and_then(|tx| tx.gas_used);

    Ok(gas)
}

// retrieve block tx gases using `eth_getTransactionReceipt`
pub async fn gas_used_parallel(
    web3: &Web3,
    hashes: impl Iterator<Item = String>,
) -> Result<Vec<U256>, JoinError> {
    // create async tasks, one for each tx hash
    let tasks = hashes.map(|tx| {
        // clone so that we can move into async block
        // this should not be expensive
        let web3 = web3.clone();

        tokio::spawn(async move {
            match gas_used(&web3, &tx[..]).await {
                Err(e) => panic!("Failed to retrieve gas for {}: {}", tx, e),
                Ok(None) => panic!("Failed to retrieve gas for {}: None", tx),
                Ok(Some(g)) => g,
            }
        })
    });

    stream::iter(tasks)
        .buffered(4) // execute in parallel in batches of 4
        .collect::<Vec<_>>()
        .await // wait for all requests to complete
        .into_iter()
        .collect() // convert Vec<Result<_>> to Result<Vec<_>>
}

pub async fn block_receipts_parity(
    web3: &Web3,
    block: u64,
) -> web3::Result<Vec<TransactionReceipt>> {
    // convert block number to hex
    let block = format!("0x{:x}", block).into();

    // call RPC
    let raw = web3
        .transport()
        .execute("parity_getBlockReceipts", vec![block])
        .await?;

    // parse response
    Ok(serde_json::from_value::<Vec<TransactionReceipt>>(raw)?)
}

// retrieve block tx gases using `parity_getBlockReceipts`
// this should be faster than `gas_parallel`
pub async fn gas_parity(web3: &Web3, block: u64) -> web3::Result<Vec<U256>> {
    let gas = block_receipts_parity(web3, block)
        .await?
        .into_iter()
        .map(|r| r.gas_used.expect("Receipt should contain `gas_used`"))
        .collect();

    Ok(gas)
}

pub fn gas_parity_parallel<'a>(
    web3: &'a Web3,
    blocks: impl Iterator<Item = u64> + 'a,
) -> impl stream::Stream<Item = Vec<U256>> + 'a {
    // create async tasks, one for each tx hash
    let tasks = blocks.map(move |b| {
        // clone so that we can move into async block
        // this should not be expensive
        let web3 = web3.clone();

        tokio::spawn(async move {
            match gas_parity(&web3, b).await {
                Err(e) => panic!("Failed to retrieve gas for {}: {}", b, e),
                Ok(g) => g,
            }
        })
    });

    stream::iter(tasks)
        .buffered(4) // execute in parallel in batches of 4
        .map(|x| x.expect("RPC should succeed"))
}

// TODO
pub async fn block_txs(web3: &Web3, num: u64) -> Result<Option<Vec<Transaction>>, web3::Error> {
    let block = BlockId::Number(BlockNumber::Number(num.into()));

    let raw = web3
        .eth()
        .block_with_txs(block)
        .await?
        .map(|b| b.transactions);

    Ok(raw)
}

// TODO
pub async fn tx_infos(web3: &Web3, num: u64) -> Result<Option<Vec<TxInfo>>, web3::Error> {
    let infos = block_txs(web3, num).await?.map(|txs| {
        txs.iter()
            .map(|tx| TxInfo {
                hash: tx.hash,
                from: tx.from.expect("tx.from is not empty"),
                to: tx.to,
                gas_limit: tx.gas,
            })
            .collect::<Vec<_>>()
    });

    Ok(infos)
}

pub fn tx_infos_parallel<'a>(
    web3: &'a Web3,
    blocks: impl Iterator<Item = u64> + 'a,
) -> impl stream::Stream<Item = Vec<TxInfo>> + 'a {
    // create async tasks, one for each tx hash
    let tasks = blocks.map(move |b| {
        // clone so that we can move into async block
        // this should not be expensive
        let web3 = web3.clone();

        tokio::spawn(async move {
            match tx_infos(&web3, b).await {
                Err(e) => panic!("Failed to retrieve transactions for {}: {}", b, e),
                Ok(None) => panic!("Block {} not found", b),
                Ok(Some(receivers)) => receivers,
            }
        })
    });

    stream::iter(tasks)
        .buffered(4) // execute in parallel in batches of 4
        .map(|x| x.expect("RPC should succeed"))
}

#[rustfmt::skip]
#[cfg(test)]
mod tests {
    use super::*;

    const BLOCK1: u64 = 5232800;
    const BLOCK2: u64 = 5232802;

    // const NODE_URL: &str = "https://mainnet.infura.io/v3/c15ab95c12d441d19702cb4a0d1313e7";
    const NODE_URL: &str = "http://localhost:8545";

    fn web3() -> Result<web3::Web3<transports::Http>, web3::Error>  {
        let transport = web3::transports::Http::new(NODE_URL)?;
        Ok(web3::Web3::new(transport))
    }

    // eth_getTransactionReceipt
    #[tokio::test]
    async fn test_gas_used() {
        let web3 = web3().expect("can instantiate web3");

        let gas_used = gas_used(&web3, "0xdb9a7dda261eb09fe3d1c3d4cdd00fe93693fa4a932ed6cfa51a5b6696f71c92")
            .await
            .expect("query succeeds")
            .expect("receipt exists");

        assert_eq!(gas_used, U256::from(52_249));
    }

    // eth_getTransactionReceipt
    #[tokio::test]
    async fn test_gas_used_parallel() {
        let web3 = web3().expect("can instantiate web3");

        let hashes = vec![
            "0xdb9a7dda261eb09fe3d1c3d4cdd00fe93693fa4a932ed6cfa51a5b6696f71c92".to_string(),
            "0xc68501cb5b3eda0fe845c30864a5657c9cf71d492c78f24b92e3d12986ae705b".to_string(),
            "0x92376a0a83608f630e4ef678de54f437d3fd41cd0165e8799ca782e42eb308cc".to_string(),
        ];

        let gas_used = gas_used_parallel(&web3, hashes.into_iter())
            .await
            .expect("queries succeed");

        assert_eq!(gas_used.len(), 3);
        assert_eq!(gas_used[0], U256::from(52_249));
        assert_eq!(gas_used[1], U256::from(21_000));
        assert_eq!(gas_used[2], U256::from(898_344));
    }

    // parity_getBlockReceipts
    #[tokio::test]
    async fn test_block_receipts_parity() {
        let web3 = web3().expect("can instantiate web3");
        let receipts = block_receipts_parity(&web3, BLOCK1).await.expect("query succeeds");

        assert_eq!(receipts.len(), 88);

        assert_eq!(receipts[0].transaction_hash, "c68501cb5b3eda0fe845c30864a5657c9cf71d492c78f24b92e3d12986ae705b".parse().unwrap());
        assert_eq!(receipts[0].gas_used, Some(U256::from(21_000)));

        assert_eq!(receipts[1].transaction_hash, "f9f3be29b6c70e3032a57e141c8374cc7cfb2b4006a640d680411af024b06179".parse().unwrap());
        assert_eq!(receipts[1].gas_used, Some(U256::from(21_000)));

        // ...

        assert_eq!(receipts[87].transaction_hash, "76da1628c18e0096ed0a24d37adf110ee39fea04c8bed36dfa7065c016f5d4d3".parse().unwrap());
        assert_eq!(receipts[87].gas_used, Some(U256::from(24_130)));
    }

    // parity_getBlockReceipts
    #[tokio::test]
    async fn test_gas_parity() {
        let web3 = web3().expect("can instantiate web3");
        let gas_used = gas_parity(&web3, BLOCK1).await.expect("query succeeds");

        assert_eq!(gas_used.len(), 88);
        assert_eq!(gas_used[0], U256::from(21_000));
        assert_eq!(gas_used[1], U256::from(21_000));
        // ...
        assert_eq!(gas_used[87], U256::from(24_130));
    }

    // parity_getBlockReceipts
    #[tokio::test]
    async fn test_gas_parity_parallel() {
        let web3 = web3().expect("can instantiate web3");
        let blocks = vec![BLOCK1, BLOCK2];

        let mut stream = gas_parity_parallel(&web3, blocks.into_iter());

        match stream.next().await {
            None => assert!(false),
            Some(gas_used) => assert_eq!(gas_used.len(), 88),
        }

        match stream.next().await {
            None => assert!(false),
            Some(gas_used) => assert_eq!(gas_used.len(), 47),
        }

        assert!(stream.next().await.is_none());
    }

    // eth_getBlockByNumber
    #[tokio::test]
    async fn test_block_txs() {
        let web3 = web3().expect("can instantiate web3");

        let txs = block_txs(&web3, BLOCK1)
            .await
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

    // eth_getBlockByNumber
    #[tokio::test]
    async fn test_tx_infos() {
        let web3 = web3().expect("can instantiate web3");

        let infos = tx_infos(&web3, BLOCK1)
            .await
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

    // eth_getBlockByNumber
    #[tokio::test]
    async fn test_tx_infos_parallel() {
        let web3 = web3().expect("can instantiate web3");
        let blocks = vec![BLOCK1, BLOCK2];

        let mut stream = tx_infos_parallel(&web3, blocks.into_iter());

        match stream.next().await {
            None => assert!(false),
            Some(infos) => assert_eq!(infos.len(), 88),
        }

        match stream.next().await {
            None => assert!(false),
            Some(infos) => assert_eq!(infos.len(), 47),
        }

        assert!(stream.next().await.is_none());
    }
}
