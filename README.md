# <h1 align="center">exex-indexer</h1>

**A real-time Ethereum indexer that writes on-chain data to a postgres database.
Reth's Execution Extensions (ExEx) framework is used for efficient real-time block notifications and processing.
Datasets are meant to be curated and shifted-left in your data pipeline as much as possible.**

[![Build](https://github.com/gibz104/exex-indexer-priv/actions/workflows/build.yml/badge.svg)](https://github.com/gibz104/exex-indexer-priv/actions/workflows/build.yml)

# Datasets

- Datasets are defined in the `/datasets` directory (one file per dataset)
- These dataset files contain all the logic to parse the on-chain data either by calling the internal node's EthApi, 
  reading from Reth's MDBX database, or simply looping through blocks, logs, or receipts provided by the ExEx.
- Individual datasets can be turned on/off in `config.yaml` prior to starting the node
- Specific Uniswap pool addresses can be specified in `config.yaml` for the volume & tvl datasets

| Dataset                 | Postgres Table Name         | Description                                        |
|-------------------------|-----------------------------|----------------------------------------------------|
| Headers                 | headers                     | Block headers from committed blocks                |
| Transactions            | transactions                | Transactions from committed blocks                 |
| Logs                    | logs                        | Event logs from transaction receipts               |
| Traces                  | traces                      | Block traces for committed transactions            |
| ERC-20 Transfers        | erc20_transfers             | ERC-20 transfers from event logs                   |
| ERC-20 Metadata         | erc20_metadata              | ERC-20 metadata for newly created ERC-20 contracts |
| Native Transfers        | native_transfers            | Native ETH transfers from block traces             |
| Contracts               | contracts                   | New contract creations from block traces           |
| Uniswap v2 Pools        | uni_v2_pools                | New Uniswap v2 pool creations and their metadata   |
| Uniswap v3 Pools        | uni_v3_pools                | New Uniswap v3 pool creations and their metadata   |
| Uniswap v4 Pools        | uni_v4_pools                | New Uniswap v4 pool creations and their metadata   |
| Uniswap v2 Volume & TVL | uni_v2_pools_volume_and_tvl | Volume & TVL for predefined Uniswap v2 pools       |
| Uniswap v3 Volume & TVL | uni_v3_pools_volume_and_tvl | Volume & TVL for predefined Uniswap v3 pools       |

# Setup

1. Ensure you have Rust and Cargo installed
2. Clone this repository
3. Set the `DATABASE_URL` environment variable to your Postgres connection string
    * example: `postgresql://username:password@host:port/database`
4. Build and run the project with `cargo run`

