extern crate web3;

use common::*;
use futures::{future, stream, FutureExt, StreamExt};
use web3::{transports, Web3};

const NODE_URL: &str = "http://localhost:8545";
// const NODE_URL: &str = "https://mainnet.infura.io/v3/c15ab95c12d441d19702cb4a0d1313e7";

const DB_PATH: &str = "_rpc_db";

#[tokio::main]
async fn main() -> web3::Result<()> {
    let web3 = Web3::new(transports::Http::new(NODE_URL)?);
    let mut db = db::RpcDb::open(DB_PATH).expect("db open succeeds");

    let from: u64 = 5232800;
    let to: u64 = 5232802;

    let mut queries = stream::iter(from..=to)
        .map(|block| {
            let web3_clone = web3.clone();

            let txs_task = tokio::spawn(async move {
                rpc::block_txs(&web3_clone, block)
                    .await
                    .expect("query succeeds")
                    .expect("block exists")
            });

            let web3_clone = web3.clone();

            let rec_task = tokio::spawn(async move {
                rpc::block_receipts_parity(&web3_clone, block)
                    .await
                    .expect("query succeeds")
            });

            future::join3(txs_task, rec_task, future::ready(block)).map(|(txs, receipts, block)| {
                (txs.expect("future OK"), receipts.expect("future OK"), block)
            })
        })
        .buffered(10);

    while let Some((txs, receipts, block)) = queries.next().await {
        println!("{} query OK", block);
        db.put_receipts(block, receipts.clone()).expect("put OK");
        db.put_txs(block, txs.clone()).expect("put OK");
        println!("{} put OK", block);
    }

    Ok(())
}
