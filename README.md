## ICSE'21 Submission Supplementary Material

The modified OpenEthereum client can be found here: https://github.com/icse22-blockchain/openethereum

The simulation script is in `./simulation`. You can build it using `cargo build --release`. The main code is in the `thread_pool` and `deterministic_scheduling` functions in `evm-trace-extract/occ.rs`, and in `evm-trace-extract/depgraph.rs`.

Data:
- `bottleneck-analysis-batch-1.csv`, `bottleneck-analysis-batch-30.csv`: Information about block or block-batch bottleneck transaction chains.
- `occ-with-det-aborts-batch-1-blocks-4833999-5692235.csv`, `occ-with-det-aborts-batch-30-blocks-5431999-5692218.csv`: Runtime costs using OCC vs deterministic scheduling.
- `partitioned-counters-batch-10-blocks-4833999-5692235.csv`: Runtime costs using partitioned counters.