# <h1 align="center">exex-indexer</h1>

**A real-time Ethereum indexer that writes on-chain data and off-chain mev-boost relay data to a postgres database.
Reth's Execution Extensions (ExEx) framework is used for efficient real-time block notifications and processing.**

[![Build](https://github.com/gibz104/exex-indexer/actions/workflows/build.yml/badge.svg)](https://github.com/gibz104/exex-indexer/actions/workflows/build.yml)

# Datasets
| Dataset           | Source               | Rust Struct           | Postgres Table    |
|-------------------|----------------------|-----------------------|-------------------|
| Headers           | On-chain             | HeadersEvent          | headers           |
| Transactions      | On-chain             | TransactionsEvent     | transactions      |
| Logs              | On-chain             | LogsEvent             | logs              |
| Traces            | On-chain             | TracesEvent           | traces            |
| Uncles            | On-chain             | OmmersEvent           | ommers            |
| Withdrawals       | On-chain             | WithdrawalsEvent      | withdrawals       |
| Builder Bids      | Off-chain (mevboost) | BuilderBidsEvent      | builder_bids      |
| Proposer Payloads | Off-chain (mevboost) | ProposerPayloadsEvent | proposer_payloads |
| ERC-20 Transfers  | Derived (logs)       | Erc20TransfersEvent   | erc20_transfers   |
| Native Transfers  | Derived (traces)     | NativeTransfersEvent  | native_transfers  |
| Contracts         | Derived (traces)     | ContractsEvent        | contracts         |


# Setup

1. Ensure you have Rust and Cargo installed
2. Clone this repository
3. Set the `DATABASE_URL` environment variable to your Postgres connection string
    * example: `postgresql://username:password@host:port/database`
4. Build and run the project with `cargo run`

# Key components

- `main.rs`: Initializes the ExEx and processes chain state notifications
- `indexer.rs`: Component that stores `ProcessingEvents` and runs them for each block
- `db.rs`: Postgres table-create DDL statements that get run once, at node startup
- `events.rs`: This is where various `ProcessingEvents` are stored as well as the wrapper `Event` enum
- `relay.rs`: Handler for fetching mev-boost relay data
- `config.rs`: Reads in the project configs
- `utils.rs`: Various utility functions used throughout the codebase

# How it works
The trait `ProcessingEvent` is setup to run each time a new block is committed.  `ProcessingEvent.process()` receives,
or retrieves, data and writes it to Postgres each time a new block is committed.  `ProcessingEvent.revert()` deletes
data from postgres each time a new block is reverted.  Multiple processing events are already setup (`HeaderEvent`,
`BuilderBidsEvent`, etc.) that correspond one-to-one to postgres tables.  However, processing events can also be setup
where one event writes to multiple postgres tables or multiple events write to the same postgres table.  At the end
of the day, each `ProcessingEvent` setup is compute you are defining for when a block is either commited or reverted
to the chain.  In a way this is just an orchestration system where the trigger is a new Ethereum block (12 seconds) and
the tasks/jobs that are being triggered are the compute defined in each `ProcessingEvent` implementation.

When a new exex commit notification is received, its chain state is added to a tokio mpsc queue.  A tokio task reads
from this mpsc queue and concurrently runs each `ProcessingEvent` with the provided chain state.  The next block's
processing will not begin until all current block's processing events have completed.  This allows a build-up of
blocks in the queue to be possible -- this would happen when a block takes longer than 12 seconds to finish its
processing events.  Most processing events are very fast to read/retrieve the data as well as writing it to postgres;
however, getting data from mev-boost relays requires making external http calls to each running relay and these calls
can take a longer time to run.  Having the mpsc queue, and allowing a build-up/queue of processing events, gives this
indexer flexibility in the types of processing it can support.

# How to add a new `ProcessingEvent`
1. Create a new struct that implements `ProcessingEvent`
2. Add this new struct to the `Event` enum
3. Add this new struct to `config.yaml` and set it to `true`
* Make sure any postgres tables the new processing event interacts with has its create ddl defined in `db.rs`
* This newly defined processing event/struct will now run each block

# Database schema
- `Headers` - Canonical block headers
- `Transactions` - Canonical block transactions
- `Logs` - Smart contract log events emitted from canonical blocks
- `Traces` - Transaction traces for canonical blocks (does not include pending transaction traces)
- `Uncles` - Uncle block headers
- `Withdrawals` - Validator withdrawals from the beacon chain that were processed in canonical blocks
- `Builder Bids` - Block builder header submissions to mev-boost relays; contains block bid values and timestamps (in ms) relay received the bid
- `Proposer Payloads` - Block header mev-boost relays sent to block proposer
- `ERC-20 Transfers` - ERC-20 transfers between addresses, based on emitted contract log events
- `Native Transfers` - Native ETH transfers as determined by transaction traces (regular transfers and internal calls with value)
- `Contracts` - Deployed contracts (and their code) based on transaction trace "create" actions 

![img.png](assets/img.png)
