# Ethereum Indexer with Reth ExEx

This project is an Ethereum indexer that writes block headers and MEV-boost relay data to a remote DuckDB instance. It leverages Reth's Executable Extensions (ExEx) framework to efficiently process and store Ethereum blockchain data.

## Features

- Indexes Ethereum block headers
- Captures MEV-boost relay data (winning payloads and proposing relays)
- Writes data to a remote DuckDB instance
- Utilizes Reth's ExEx framework for seamless integration with Reth node

## How It Works

The indexer follows the Ethereum chain head and processes new chain state notifications:

1. Reverts headers when necessary
2. Inserts new block headers
3. Fetches and stores MEV-boost data for each block

## Key Components

- `init`: Initializes the ExEx and creates necessary DuckDB tables
- `indexer_exex`: Main ExEx function that processes chain state notifications
- `write_headers`: Writes block header data to DuckDB
- `write_mevboost_data`: Fetches and writes MEV-boost data to DuckDB
- `rbuilder_types.rs`: MEV-Boost handler for fetching relay data (Flashbots rbuilder)

## Setup

1. Ensure you have Rust and Cargo installed
2. Clone this repository
3. Set the `DB_ENDPOINT` environment variable to your DuckDB instance URL
4. Build and run the project with `cargo run`

## Acknowledgements

This project is made possible by Reth's Executable Extensions (ExEx) framework, which simplifies the creation of Ethereum indexers and other blockchain-related tools.