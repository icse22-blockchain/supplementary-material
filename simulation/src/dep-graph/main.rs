extern crate regex;
extern crate rocksdb;
extern crate web3;

use common::*;

use crate::transaction_info::{Access, AccessMode, Target, TransactionInfo};

use futures::{stream, StreamExt};
use rocksdb::DB;
use std::collections::HashMap;
use std::env;
use web3::types::U256;

fn is_wr_conflict(first: &TransactionInfo, second: &TransactionInfo) -> bool {
    for acc in second
        .accesses
        .iter()
        .filter(|a| a.mode == AccessMode::Read)
    {
        if let Target::Storage(addr, entry) = &acc.target {
            if first.accesses.contains(&Access::storage_write(addr, entry)) {
                return true;
            }
        }
    }

    false
}

fn find_longest_rec(
    start_from: usize,
    depends_on: &HashMap<usize, Vec<usize>>,
    gas: &Vec<U256>,
    memo: &mut HashMap<usize, (U256, Vec<usize>)>,
) -> (U256, Vec<usize>) {
    if let Some((cost, chain)) = memo.get(&start_from) {
        return (cost.clone(), chain.clone());
    }

    let res = match depends_on.get(&start_from) {
        None => (gas[start_from], vec![start_from]),
        Some(preds) => {
            let mut largest_cost = U256::from(0);
            let mut longest_chain = vec![];

            for predecessor in preds {
                let (mut cost, chain) = find_longest_rec(*predecessor, depends_on, gas, memo);
                cost += gas[start_from];

                if cost > largest_cost {
                    largest_cost = cost;
                    longest_chain = chain;
                }
            }

            longest_chain.push(start_from);
            (largest_cost, longest_chain)
        }
    };

    memo.insert(start_from, res.clone());
    res
}

fn find_longest(depends_on: &HashMap<usize, Vec<usize>>, gas: &Vec<U256>) -> (U256, Vec<usize>) {
    let mut largest_cost = U256::from(0);
    let mut longest_chain = vec![];

    let mut memo = HashMap::new();

    for tx in 0..gas.len() {
        let (cost, chain) = find_longest_rec(tx, depends_on, gas, &mut memo);
        // println!("longest from {}: {:?}, {:?}", tx, cost, chain);

        if cost > largest_cost {
            largest_cost = cost;
            longest_chain = chain;
        }
    }

    (largest_cost, longest_chain)
}

async fn xxx(db: &DB, from: u64, to: u64) {
    let rpc_db = db::RpcDb::open_for_read_only("./_rpc_db").expect("db open succeeds");

    println!("block;speedup_bound;bottleneck_chain;bottleneck_cost");

    let others = stream::iter(from..=to).map(|block| {
        let gas = rpc_db
            .gas_used(block)
            .expect("get from db succeeds")
            .expect("block exists in db");

        let info = rpc_db
            .tx_infos(block)
            .expect("get from db succeeds")
            .expect("block exists in db");

        (gas, info)
    });

    let blocks = stream::iter(from..=to);
    let mut it = blocks.zip(others);

    while let Some((block, (gas, info))) = it.next().await {
        let txs = db::tx_infos(&db, block, &info);

        let num_txs = txs.len();

        if num_txs == 0 {
            println!("{:?};{:?};{:?};{:?}", block, 0, Vec::<usize>::new(), 0);
            continue;
        }

        let mut depends_on = HashMap::new();

        // for tx in 0..num_txs {
        //     println!("tx-{} ({}) gas: {}", tx, txs[tx].tx_hash, gas[tx]);
        // }

        for first in 0..(num_txs - 1) {
            for second in (first + 1)..num_txs {
                if is_wr_conflict(&txs[first], &txs[second]) {
                    depends_on.entry(second).or_insert(vec![]).push(first);
                }
            }
        }

        // println!("depends_on = {:#?}", depends_on);

        let (cost, chain) = find_longest(&depends_on, &gas);
        let serial = gas.iter().fold(U256::from(0), |acc, item| acc + item);
        // println!("speedup bound = {:?} (chain = {:?}, cost = {:?})", , chain, cost);
        let speedup_bound = (serial.as_u64() as f64) / (cost.as_u64() as f64);

        println!("{:?};{:?};{:?};{:?}", block, speedup_bound, chain, cost);
    }
}

#[tokio::main]
async fn main() -> web3::Result<()> {
    // let transport = web3::transports::Http::new("http://localhost:8545")?;
    // let web3 = web3::Web3::new(transport);

    // parse args
    let args: Vec<String> = env::args().collect();

    if args.len() != 4 {
        println!("Usage: dep-graph [db-path:str] [from-block:int] [to-block:int]");
        return Ok(());
    }

    let path = &args[1][..];

    let from = args[2]
        .parse::<u64>()
        .expect("from-block should be a number");

    let to = args[3].parse::<u64>().expect("to-block should be a number");

    // open db
    let db = db::open_traces(path);

    // check range
    let latest_raw = db
        .get(b"latest")
        .expect("get latest should succeed")
        .expect("latest should exist");

    let latest = std::str::from_utf8(&latest_raw[..])
        .expect("parse to string succeed")
        .parse::<u64>()
        .expect("parse to int should succees");

    if to > latest {
        println!("Latest header in trace db: #{}", latest);
        return Ok(());
    }

    // // process
    // match mode {
    //     "pairwise" => process_pairwise(&db, from..=to, output),
    //     "aborts" => occ_detailed_stats(&db, &web3, from, to, output).await,
    //     _ => {
    //         println!("mode should be one of: pairwise, aborts");
    //         return Ok(());
    //     }
    // }

    xxx(&db, from, to).await;

    Ok(())
}
