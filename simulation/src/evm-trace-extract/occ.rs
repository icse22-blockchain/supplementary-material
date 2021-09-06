use crate::rpc;
use crate::transaction_info::{Access, AccessMode, Target, TransactionInfo};
use std::cmp::{min, Reverse};
use std::collections::{BinaryHeap, HashSet};
use std::convert::TryFrom;
use web3::types::U256;

// Estimate number of aborts (due to conflicts) in block.
// The actual number can be lower if we process transactions in batches,
//      e.g. with batches of size 2, tx-1's write will not affect tx-3's read.
// The actual number can also be higher because the same transaction could be aborted multiple times,
//      e.g. with batch [tx-1, tx-2, tx-3], tx-2's abort will make tx-3 abort as well,
//      then in the next batch [tx-2, tx-3, tx-4] tx-3 might be aborted again if it reads a slot written by tx-2.
pub fn num_conflicts(txs: &Vec<TransactionInfo>) -> u64 {
    let mut num_aborted = 0;

    // keep track of which storage entries were written
    let mut storages = HashSet::new();

    for tx in txs {
        let TransactionInfo { accesses, .. } = tx;

        // check for conflicts without committing changes
        // a conflict is when tx-b reads a storage entry written by tx-a
        for acc in accesses.iter().filter(|a| a.mode == AccessMode::Read) {
            if let Target::Storage(addr, entry) = &acc.target {
                if storages.contains(&(addr, entry)) {
                    num_aborted += 1;
                    break;
                }
            }
        }

        // commit changes
        for acc in accesses.iter().filter(|a| a.mode == AccessMode::Write) {
            if let Target::Storage(addr, entry) = &acc.target {
                storages.insert((addr, entry));
            }
        }
    }

    num_aborted
}

// First execute all transaction in parallel (infinite threads), then re-execute aborted ones serially.
// Note that this is inaccuare in practice: if tx-1 and tx-3 succeed and tx-2 aborts, we will have to
// re-execute both tx-2 and tx-3, as tx-2's new storage access patterns might make tx-3 abort this time.
#[allow(dead_code)]
pub fn parallel_then_serial(txs: &Vec<TransactionInfo>, gas: &Vec<U256>) -> U256 {
    assert_eq!(txs.len(), gas.len());

    // keep track of which storage entries were written
    let mut storages = HashSet::new();

    // parallel gas cost is the cost of parallel execution (max gas cost)
    // + sum of gas costs for aborted txs
    let parallel_cost = gas.iter().max().cloned().unwrap_or(U256::from(0));
    let mut serial_cost = U256::from(0);

    for (id, tx) in txs.iter().enumerate() {
        let TransactionInfo { accesses, .. } = tx;

        // check for conflicts without committing changes
        // a conflict is when tx-b reads a storage entry written by tx-a
        for acc in accesses.iter().filter(|a| a.mode == AccessMode::Read) {
            if let Target::Storage(addr, entry) = &acc.target {
                if storages.contains(&(addr, entry)) {
                    serial_cost += gas[id];
                    break;
                }
            }
        }

        // commit changes
        for acc in accesses.iter().filter(|a| a.mode == AccessMode::Write) {
            if let Target::Storage(addr, entry) = &acc.target {
                storages.insert((addr, entry));
            }
        }
    }

    parallel_cost + serial_cost
}

// Process transactions in fixed-size batches.
// We assume a batch's execution cost is (proportional to) the largest gas cost in that batch.
// If the n'th transaction in a batch aborts (detected on commit), we will re-execute all transactions after (and including) n.
// Note that in this scheme, we wait for all txs in a batch before starting the next one, resulting in thread under-utilization.
#[allow(dead_code)]
pub fn batches(txs: &Vec<TransactionInfo>, gas: &Vec<U256>, batch_size: usize) -> U256 {
    assert_eq!(txs.len(), gas.len());

    let mut next = min(batch_size, txs.len()); // e.g. 4
    let mut batch = (0..next).collect::<Vec<_>>(); // e.g. [0, 1, 2, 3]
    let mut cost = U256::from(0);

    loop {
        // exit condition: nothing left to process
        if batch.is_empty() {
            assert_eq!(next, txs.len());
            break;
        }

        // cost of batch is the maximum gas cost in this batch
        let cost_of_batch = batch
            .iter()
            .map(|id| gas[*id])
            .max()
            .expect("batch not empty");

        cost += cost_of_batch;

        // keep track of which storage entries were written
        // start with clear storage for each batch!
        // i.e. txs will not abort due to writes in previous batches
        let mut storages = HashSet::new();

        // process batch
        'outer: for id in batch {
            let TransactionInfo { accesses, .. } = &txs[id];

            // check for conflicts without committing changes
            // a conflict is when tx-b reads a storage entry written by tx-a
            for acc in accesses.iter().filter(|a| a.mode == AccessMode::Read) {
                if let Target::Storage(addr, entry) = &acc.target {
                    if storages.contains(&(addr, entry)) {
                        // e.g. if our batch is [0, 1, 2, 3]
                        // and we detect a conflict while committing `2`,
                        // then the next batch is [2, 3, 4, 5]
                        // because the outdated value read by `2` might affect `3`

                        next = id;
                        break 'outer;
                    }
                }
            }

            // commit updates
            for acc in accesses.iter().filter(|a| a.mode == AccessMode::Write) {
                if let Target::Storage(addr, entry) = &acc.target {
                    storages.insert((addr, entry));
                }
            }
        }

        // prepare next batch
        batch = vec![];

        while batch.len() < batch_size && next < txs.len() {
            batch.push(next);
            next += 1;
        }
    }

    cost
}

#[allow(dead_code)]
pub fn deterministic_scheduling(
    txs: &Vec<TransactionInfo>,
    gas: &Vec<U256>,
    _info: &Vec<rpc::TxInfo>,
    num_threads: usize,

    allow_check_conflicts_before_commit: bool,
) -> (U256, U256) {
    assert_eq!(txs.len(), gas.len());

    type MinHeap<T> = BinaryHeap<Reverse<T>>;

    // transaction queue: transactions waiting to be executed
    // item: <(storage-version, transaction-id)>
    // pop() always returns the lowest storage-version
    let mut tx_queue: MinHeap<(i32, usize)> =
        (0..txs.len()).map(|tx_id| Reverse((-1, tx_id))).collect();

    // transaction ready to schedule : txns have less or equal storage version than the current committed storage version
    // item: <(transaction-id, storage-version)>
    // pop() always returns the lowest transaction-id
    let mut tx_ready_to_schedule: MinHeap<(usize, i32)> = Default::default();

    // commit queue: txs that finished execution and are waiting to commit
    // item: <transaction-id, storage-version>
    // pop() always returns the lowest transaction-id
    // storage-version is the highest committed transaction-id before the tx's execution started
    let mut commit_queue: MinHeap<(usize, i32)> = Default::default();

    // next transaction-id to commit
    let mut next_to_commit = 0;

    // thread pool: information on current execution on each thread
    // item: < gas-left, transaction-id, storage-version>
    // pop() always returns the least gas-left
    let mut threads: MinHeap<(U256, usize, i32)> = Default::default();

    // overall cost of execution
    let mut cost = U256::from(0);

    #[allow(non_snake_case)]
    let mut N = 0;

    let mut num_aborts = U256::from(0);

    // just for checking RW-conflict between a to-commit txn(R) and a just-executed txn(W)
    let is_wr_conflict = |executed: usize, to_commit: usize| {
        for acc in txs[to_commit]
            .accesses
            .iter()
            .filter(|a| a.mode == AccessMode::Read)
        {
            if let Target::Storage(addr, entry) = &acc.target {
                if txs[executed]
                    .accesses
                    .contains(&Access::storage_write(addr, entry))
                {
                    log::trace!(
                        "tx-{}/tx-{} wr conflict on {}-{}",
                        executed,
                        to_commit,
                        addr,
                        entry
                    );
                    return true;
                }
            }
        }

        false
    };

    log::trace!("-----------------------------------------");

    loop {
        // ---------------- exit condition ----------------
        if next_to_commit == txs.len() {
            // we have committed all transactions
            // nothing left to execute or commit
            assert!(
                tx_queue.is_empty() && tx_ready_to_schedule.is_empty(),
                "tx queue not empty"
            );
            assert!(commit_queue.is_empty(), "commit queue not empty");

            // all threads are idle
            assert!(threads.is_empty(), "some threads are not idle");

            break;
        }

        // ---------------- scheduling ----------------
        // push prepared txs to tx_ready_to_schedule from tx_queue
        while let Some(Reverse((sv, tx_id))) = tx_queue.pop() {
            if sv > next_to_commit as i32 - 1 {
                tx_queue.push(Reverse((sv, tx_id)));
                break;
            }
            log::trace!("[{}] (tx-{}, sv({})) is ready for scheduling", N, tx_id, sv);
            tx_ready_to_schedule.push(Reverse((tx_id, sv)));
        }

        log::trace!("[{}] threads before scheduling: {:?}", N, threads);
        log::trace!("[{}] tx queue before scheduling: {:?}", N, tx_queue);
        log::trace!(
            "[{}] txs ready for scheduling: {:?}",
            N,
            tx_ready_to_schedule
        );
        log::trace!("[{}] commit queue before scheduling: {:?}", N, commit_queue);

        'schedule: loop {
            // check if there are any idle threads
            if threads.len() == num_threads {
                break 'schedule;
            }

            // get tx from tx_queue
            if let Some(Reverse((tx_id, sv))) = tx_ready_to_schedule.pop() {
                log::trace!(
                    "[{}] attempting to schedule tx-{} ({})...",
                    N,
                    tx_id,
                    &txs[tx_id].tx_hash[0..8]
                );

                // schedule on the first idle thread
                log::trace!(
                    "[{}] scheduling tx-{} ({}) to a thread",
                    N,
                    tx_id,
                    &txs[tx_id].tx_hash[0..8],
                );

                let gas_left = gas[tx_id];

                threads.push(Reverse((gas_left, tx_id, sv)));
            } else {
                break 'schedule;
            }
        }

        // ---------------- execution ----------------
        // find transaction that finishes execution next
        log::trace!("[{}] threads before execution: {:?}", N, threads);
        if let Some(Reverse((gas_step, tx_id, sv))) = threads.pop() {
            log::trace!(
                "[{}] executing tx-{} ({}) (step = {})",
                N,
                tx_id,
                &txs[tx_id].tx_hash[0..8],
                gas_step
            );
            if allow_check_conflicts_before_commit {
                let mut old_commit_queue: MinHeap<(usize, i32)> = commit_queue.clone();
                commit_queue.clear();
                while let Some(Reverse((c_id, c_sv))) = old_commit_queue.pop() {
                    if ((tx_id as i32) > c_sv) && (tx_id < c_id) && (is_wr_conflict(tx_id, c_id)) {
                        // re-schedule aborted tx
                        num_aborts += U256::from(1);
                        tx_queue.push(Reverse((c_id as i32 - 1, c_id)));
                    } else {
                        commit_queue.push(Reverse((c_id, c_sv)));
                    }
                }
            }

            // finish executing tx, update thread states
            commit_queue.push(Reverse((tx_id, sv)));
            cost += gas_step;

            threads = threads
                .into_iter()
                .map(|Reverse((gas_left, tx_id, sv))| Reverse((gas_left - gas_step, tx_id, sv)))
                .collect();
        }

        log::trace!("[{}] threads after execution: {:?}", N, threads);

        // ---------------- commit / abort ----------------

        log::trace!("[{}] commit queue before committing: {:?}", N, commit_queue);
        log::trace!(
            "[{}] next_to_commit before committing: {:?}",
            N,
            next_to_commit
        );

        while let Some(Reverse((tx_id, sv))) = commit_queue.peek() {
            log::trace!(
                "[{}] attempting to commit tx-{} ({}, sv = {})...",
                N,
                tx_id,
                &txs[*tx_id].tx_hash[0..8],
                sv
            );

            // we must commit transactions in order
            if *tx_id != next_to_commit {
                log::trace!(
                    "[{}] unable to commit tx-{} ({}): next to commit is tx-{} ({})",
                    N,
                    tx_id,
                    &txs[*tx_id].tx_hash[0..8],
                    next_to_commit,
                    &txs[next_to_commit].tx_hash[0..8]
                );
                assert!(*tx_id > next_to_commit);
                break;
            }

            let Reverse((tx_id, sv)) = commit_queue.pop().unwrap();

            // if allow check conflicts before commit, we skip the unnecessary conflicts' check below
            if allow_check_conflicts_before_commit {
                log::trace!(
                    "[{}] COMMIT tx-{} ({})",
                    N,
                    tx_id,
                    &txs[tx_id].tx_hash[0..8]
                );
                next_to_commit += 1;
                continue;
            }

            // check all potentially conflicting transactions
            // e.g. if tx-3 was executed with sv = -1, it means that it cannot see writes by tx-0, tx-1, tx-2
            let conflict_from = usize::try_from(sv + 1).expect("sv + 1 should be non-negative");
            let conflict_to = tx_id;

            let accesses = &txs[tx_id].accesses;
            let mut aborted = false;

            'outer: for prev_tx in conflict_from..conflict_to {
                let concurrent = &txs[prev_tx].accesses;

                for acc in accesses.iter().filter(|a| a.mode == AccessMode::Read) {
                    if let Target::Storage(addr, entry) = &acc.target {
                        if concurrent.contains(&Access::storage_write(addr, entry)) {
                            log::trace!("[{}] wr conflict between tx-{} ({}) [committed] and tx-{} ({}) [to be committed], ABORT tx-{}", N, prev_tx, &txs[prev_tx].tx_hash[0..8], tx_id, &txs[tx_id].tx_hash[0..8], tx_id);
                            aborted = true;
                            break 'outer;
                        }
                    }
                }
            }

            // commit transaction
            if !aborted {
                log::trace!(
                    "[{}] COMMIT tx-{} ({})",
                    N,
                    tx_id,
                    &txs[tx_id].tx_hash[0..8]
                );
                next_to_commit += 1;
                continue;
            }

            // re-schedule aborted tx
            num_aborts += U256::from(1);
            tx_queue.push(Reverse((tx_id as i32 - 1, tx_id)));
        }

        N += 1;
        log::trace!("[{}] cost so far: {}", N, cost);
    }
    (cost, num_aborts)
}

#[allow(dead_code)]
pub fn thread_pool(
    txs: &Vec<TransactionInfo>,
    gas: &Vec<U256>,
    _info: &Vec<rpc::TxInfo>,
    num_threads: usize,

    allow_ignore_slots: bool,
    allow_avoid_conflicts_during_scheduling: bool,
    allow_read_from_uncommitted: bool,
) -> U256 {
    assert_eq!(txs.len(), gas.len());

    let mut ignored_slots: HashSet<&str> = Default::default();
    ignored_slots.insert("0x06012c8cf97bead5deae237070f9587f8e7a266d-0x000000000000000000000000000000000000000000000000000000000000000f");
    ignored_slots.insert("0x06012c8cf97bead5deae237070f9587f8e7a266d-0x0000000000000000000000000000000000000000000000000000000000000006");
    ignored_slots.insert("0x06012c8cf97bead5deae237070f9587f8e7a266d-0xc56c286245a85e4048e082d091c57ede29ec05df707b458fe836e199193ff182");

    type MinHeap<T> = BinaryHeap<Reverse<T>>;

    // transaction queue: transactions waiting to be executed
    // item: <transaction-id>
    // pop() always returns the lowest transaction-id
    let mut tx_queue: MinHeap<usize> = (0..txs.len()).map(Reverse).collect();

    // commit queue: txs that finished execution and are waiting to commit
    // item: <transaction-id, storage-version, seen-set>
    // pop() always returns the lowest transaction-id
    // storage-version is the highest committed transaction-id before the tx's execution started
    let mut commit_queue: MinHeap<(usize, i32, Vec<usize>)> = Default::default();

    // next transaction-id to commit
    let mut next_to_commit = 0;

    // thread pool: information on current execution on each thread
    // item: <transaction-id, gas-left, storage-version, seen-set> or None if idle
    let mut threads: Vec<Option<(usize, U256, i32, Vec<usize>)>> = vec![None; num_threads];

    // overall cost of execution
    let mut cost = U256::from(0);

    #[allow(non_snake_case)]
    let mut N = 0;

    let is_wr_conflict = |running: usize, to_schedule: usize| {
        for acc in txs[to_schedule]
            .accesses
            .iter()
            .filter(|a| a.mode == AccessMode::Read)
        {
            if let Target::Storage(addr, entry) = &acc.target {
                if txs[running]
                    .accesses
                    .contains(&Access::storage_write(addr, entry))
                {
                    log::trace!(
                        "tx-{}/tx-{} wr conflict on {}-{}",
                        running,
                        to_schedule,
                        addr,
                        entry
                    );
                    return true;
                }
            }
        }

        false
    };

    log::trace!("-----------------------------------------");

    loop {
        // ---------------- exit condition ----------------
        if next_to_commit == txs.len() {
            // we have committed all transactions
            // nothing left to execute or commit
            assert!(tx_queue.is_empty(), "tx queue not empty");
            assert!(commit_queue.is_empty(), "commit queue not empty");

            // all threads are idle
            assert!(
                threads.iter().all(|opt| opt.is_none()),
                "some threads are not idle"
            );

            break;
        }

        // ---------------- scheduling ----------------
        log::trace!("");
        log::trace!("[{}] threads before scheduling: {:?}", N, threads);
        log::trace!("[{}] tx queue before scheduling: {:?}", N, tx_queue);
        log::trace!("[{}] commit queue before scheduling: {:?}", N, commit_queue);

        let mut reinsert = HashSet::new();

        'schedule: loop {
            // check if there are any idle threads
            if !threads.iter().any(Option::is_none) {
                break 'schedule;
            }

            // get tx from tx_queue
            let tx_id = match tx_queue.pop() {
                Some(Reverse(id)) => id,
                None => break,
            };

            log::trace!(
                "[{}] attempting to schedule tx-{} ({})...",
                N,
                tx_id,
                &txs[tx_id].tx_hash[0..8]
            );

            if allow_avoid_conflicts_during_scheduling {
                // check executed txs for conflicts
                for Reverse((executed_tx, _, _)) in &commit_queue {
                    // case 1:
                    // e.g., tx-3 is waiting to be committed, we're scheduling tx-5
                    //       tx-5 reads from tx-3 => do not run yet
                    if *executed_tx < tx_id && is_wr_conflict(*executed_tx, tx_id) {
                        log::trace!("[{}] wr conflict between tx-{} [executed] and tx-{} [to be scheduled], postponing tx-{}", N, *executed_tx, tx_id, tx_id);
                        reinsert.insert(tx_id);
                        continue 'schedule;
                    }

                    // case 2:
                    // e.g., tx-5 is waiting to be committed, we're scheduling tx-3
                    //       tx-5 reads from tx-3 => tx-5 should be invalidated
                    // TODO
                }

                // check running txs for conflicts
                for thread_id in 0..threads.len() {
                    let (running_tx, ..) = match &threads[thread_id] {
                        None => continue,
                        Some(x) => x,
                    };

                    assert!(tx_id != *running_tx);

                    // case 1:
                    // e.g., tx-3 is running, we're scheduling tx-5
                    //       tx-5 reads from tx-3 => do not run yet
                    if *running_tx < tx_id && is_wr_conflict(*running_tx, tx_id) {
                        log::trace!("[{}] wr conflict between tx-{} ({}) [running] and tx-{} ({}) [to be scheduled], postponing tx-{}", N, *running_tx, &txs[*running_tx].tx_hash[0..8], tx_id, &txs[tx_id].tx_hash[0..8], tx_id);
                        reinsert.insert(tx_id);
                        continue 'schedule;
                    }
                    // case 2:
                    // e.g., tx-5 is running, we're scheduling tx-3
                    //       tx-5 reads from tx-3 => replace tx-5 with tx-3
                    else if tx_id < *running_tx && is_wr_conflict(tx_id, *running_tx) {
                        log::trace!("[{}] wr conflict between tx-{} ({}) [running] and tx-{} ({}) [to be scheduled], replacing tx-{} with tx-{}", N, *running_tx, &txs[*running_tx].tx_hash[0..8], tx_id, &txs[tx_id].tx_hash[0..8], *running_tx, tx_id);

                        reinsert.insert(*running_tx);

                        // overwrite
                        let gas_left = gas[tx_id];
                        let sv = next_to_commit as i32 - 1;

                        let seen = commit_queue
                            .iter()
                            .map(|Reverse((id, ..))| id)
                            .cloned()
                            .filter(|id| *id < tx_id)
                            .collect::<Vec<usize>>();

                        threads[thread_id] = Some((tx_id, gas_left, sv, seen));

                        continue 'schedule;
                    }
                }
            }

            // schedule on the first idle thread
            for thread_id in 0..threads.len() {
                if threads[thread_id].is_none() {
                    log::trace!(
                        "[{}] scheduling tx-{} ({}) on thread-{}",
                        N,
                        tx_id,
                        &txs[tx_id].tx_hash[0..8],
                        thread_id
                    );

                    let gas_left = gas[tx_id];
                    let sv = next_to_commit as i32 - 1;

                    let seen = commit_queue
                        .iter()
                        .map(|Reverse((id, ..))| id)
                        .cloned()
                        .filter(|id| *id < tx_id)
                        .collect::<Vec<usize>>();

                    threads[thread_id] = Some((tx_id, gas_left, sv, seen));
                    continue 'schedule;
                }
            }

            assert!(false, "should find idle thread");
        }

        // reschedule txs for later
        if !reinsert.is_empty() {
            log::trace!("[{}] reinserting {:?} into tx queue", N, reinsert);
        }

        for tx_id in reinsert {
            tx_queue.push(Reverse(tx_id));
        }

        log::trace!("[{}] threads after scheduling: {:?}", N, threads);

        // ---------------- execution ----------------
        // find transaction that finishes execution next
        let (thread_id, (tx_id, gas_step, sv, seen)) = threads
            .iter()
            .enumerate()
            .filter(|(_, opt)| opt.is_some())
            .map(|(id, opt)| (id, opt.clone().unwrap()))
            .min_by_key(|(_, (_, gas_left, _, _))| *gas_left)
            .expect("not all threads are idle");

        log::trace!(
            "[{}] executing tx-{} ({}) on thread-{} (step = {})",
            N,
            tx_id,
            &txs[tx_id].tx_hash[0..8],
            thread_id,
            gas_step
        );

        // finish executing tx, update thread states
        threads[thread_id] = None;
        commit_queue.push(Reverse((tx_id, sv, seen)));
        cost += gas_step;

        for thread_id in 0..threads.len() {
            if let Some((_, gas_left, _, _)) = &mut threads[thread_id] {
                *gas_left -= gas_step;
            }
        }

        log::trace!("[{}] threads after execution: {:?}", N, threads);

        // ---------------- commit / abort ----------------
        log::trace!("[{}] commit queue before committing: {:?}", N, commit_queue);
        log::trace!(
            "[{}] next_to_commit before committing: {:?}",
            N,
            next_to_commit
        );

        while let Some(Reverse((tx_id, sv, _))) = commit_queue.peek() {
            log::trace!(
                "[{}] attempting to commit tx-{} ({}, sv = {})...",
                N,
                tx_id,
                &txs[*tx_id].tx_hash[0..8],
                sv
            );

            // we must commit transactions in order
            if *tx_id != next_to_commit {
                log::trace!(
                    "[{}] unable to commit tx-{} ({}): next to commit is tx-{} ({})",
                    N,
                    tx_id,
                    &txs[*tx_id].tx_hash[0..8],
                    next_to_commit,
                    &txs[next_to_commit].tx_hash[0..8]
                );
                assert!(*tx_id > next_to_commit);
                break;
            }

            let Reverse((tx_id, sv, seen)) = commit_queue.pop().unwrap();

            // check all potentially conflicting transactions
            // e.g. if tx-3 was executed with sv = -1, it means that it cannot see writes by tx-0, tx-1, tx-2
            let conflict_from = usize::try_from(sv + 1).expect("sv + 1 should be non-negative");
            let conflict_to = tx_id;

            let accesses = &txs[tx_id].accesses;
            let mut aborted = false;

            'outer: for prev_tx in conflict_from..conflict_to {
                let concurrent = &txs[prev_tx].accesses;

                for acc in accesses.iter().filter(|a| a.mode == AccessMode::Read) {
                    if let Target::Storage(addr, entry) = &acc.target {
                        if concurrent.contains(&Access::storage_write(addr, entry)) {
                            if allow_ignore_slots
                                && ignored_slots.contains(&format!("{}-{}", addr, entry)[..])
                            {
                                continue;
                            }

                            if allow_read_from_uncommitted && seen.contains(&prev_tx) {
                                log::trace!("[{}] not aborting: tx-{} ({}) [to be committed] has seen tx-{} ({}) [then uncommitted]", N, tx_id, &txs[tx_id].tx_hash[0..8], prev_tx, &txs[prev_tx].tx_hash[0..8]);
                                continue;
                            }

                            log::trace!("[{}] wr conflict between tx-{} ({}) [committed] and tx-{} ({}) [to be committed], ABORT tx-{}", N, prev_tx, &txs[prev_tx].tx_hash[0..8], tx_id, &txs[tx_id].tx_hash[0..8], tx_id);
                            aborted = true;
                            break 'outer;
                        }
                    }
                }
            }

            // commit transaction
            if !aborted {
                log::trace!(
                    "[{}] COMMIT tx-{} ({})",
                    N,
                    tx_id,
                    &txs[tx_id].tx_hash[0..8]
                );
                next_to_commit += 1;
                continue;
            }

            // re-schedule aborted tx
            tx_queue.push(Reverse(tx_id));
        }

        N += 1;
        log::trace!("[{}] cost so far: {}", N, cost);
    }

    log::trace!("-----------------------------------------");

    cost
}
