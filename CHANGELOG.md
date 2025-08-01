# Changelog

All notable changes to this project will be documented in this file.

## [0.3.0] - 2025-07-31

### Changed

- Updated Reth from `v1.1.5` to `v1.6.0`

- Replaced deprecated `SealedBlockWithSenders` with `RecoveredBlock`

- Refactored ExEx and EthApi usage to follow best practices in Reth v1.6.0

- Refactored trait bounds and usage of `TransactionRequest`

### Removed

- Most helper functions in `utils.rs` to create eth_api and trace_api have been
  removed since Reth now makes it easier to do this out-of-the-box with their `NodeAddOns`

## [0.2.0] - 2025-04-07

### Added

- Added this `CHANGELOG.md` file to track any future changes made to this project.

- Added function in `utils.rs` for creating an EthApi instance against the
  currently running ExEx Reth node. This is made available within the dataset
  processing functions so that Eth API calls can be made directly into the
  node rather than making external http calls.

- Added new Uniswap datasets that index new pool creations (uni v2, v3, v4) as well
  as datasets that include basic pool data like volume, prices, tvl, and number
  of trades (uni v2, v3)

- Datasets added: `ERC-20 Metadata`, `Uniswap v2 Pools`, `Uniswap v3 Pools`, 
  `Uniswap v4 Pools`, `Uniswap v2 Volume & TVL`, `Uniswap v3 Volume & TVL`.

### Changed

- Fully refactored data pipeline to no longer use mpsc channels. Channels were
  needed before because extracting mev-boost data could take longer than 12
  seconds to extract, and the channel was used as a back flow to track blocks
  that need to be processed. ExExs are best when execution can be completed within
  a single slot (12 seconds), which we can now accomplish since we are only indexing
  on-chain data and no longer making external http calls to mev-boost relays.

- Rather than having to create a new `ProcessingEvent` struct for each dataset, we
  instead moved in favor of creating individual processing functions where one 
  function exists for each dataset.  These processing functions are now defined in
  the `/datasets` directory.  The goal is to make defining the logic for each dataset
  as simple as possible.

- Seperated the logic of writing data to the db and the logic required to parse the
  on-chain dataset. We centralized all database writing activities to the `db_writer.rs`
  file, which now contains a macro `record_values` which can be used throughout the
  codebase for consistent database writing and type conversion.  This allows the 
  processing functions defined in `/datasets` to be much cleaner and no longer requires
  users to have knowledge on writing to the database when creating a new dataset.

- Table definitions are now defined in the `table_definitions.rs` file as Rust types 
  and are used when creating table ddl and indexes on startup. This replaces the 
  old table ddl represented as strings.

- Replaced manually defined event signatures with sol! macros.

### Removed

- Remove ability to index off-chain mev-boost data. All code related to making
  external http calls and their respective rate limiting has also been removed
  as it was only used for extracting mev-boost data. Long-term direction is to
  only index on-chain data accessible directly within Reth. Mev-boost data can
  be obtained via Xatu instead (https://github.com/ethpandaops/xatu).

- Datasets removed: `Uncles`, `Withdrawals`, `Builder Bids`, and `Proposer Payloads`

## [0.1.0] - 2024-11-04

### Added

- Initial release, including indexing for both on-chain data and off-chain mev-boost data.