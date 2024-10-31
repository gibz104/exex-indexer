use alloy_primitives::{Address, BlockHash, hex};
use async_trait::async_trait;
use chrono::Utc;
use reth_primitives::{
    SealedBlockWithSenders,
    TransactionSigned,
    Receipt,
    Header,
    Withdrawals,
    Log,
};
use tokio_postgres::{Client, types::Type, binary_copy::BinaryCopyInWriter};
use reth_tracing::tracing::warn;
use crate::relay::{RELAYS, RelayClient};
use crate::utils::sanitize_bytes;
use std::sync::Arc;
use tokio::sync::Semaphore;
use futures_util::{pin_mut, future::join_all};
use lazy_static::lazy_static;
use primitive_types::{H256, U256};

lazy_static! {
    /// Transfer event signature hash
    static ref ERC20_TRANSFER_TOPIC: H256 = H256(
        <[u8; 32]>::try_from(hex::decode("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")
            .expect("Static transfer topic decoding failed")).unwrap()
    );
}

pub struct ProcessingResult {
    pub records_written: usize,
}

#[async_trait]
pub trait ProcessingEvent: Send + Sync {
    fn name(&self) -> &'static str;

    /// Process the block data
    async fn process(&self, block_data: &(SealedBlockWithSenders, Vec<Option<Receipt>>), client: &Arc<Client>) -> eyre::Result<ProcessingResult>;

    /// Revert the processed data for given block numbers
    async fn revert(&self, block_numbers: &[i64], client: &Arc<Client>) -> eyre::Result<()>;
}

#[derive(Clone)]
pub struct HeaderEvent;

#[async_trait]
impl ProcessingEvent for HeaderEvent {
    fn name(&self) -> &'static str {
        "HeaderEvent"
    }

    async fn process(&self, block_data: &(SealedBlockWithSenders, Vec<Option<Receipt>>), client: &Arc<Client>) -> eyre::Result<ProcessingResult> {
        let block = &block_data.0;
        let header: &Header = block.block.header.header();
        let hash: BlockHash = block.block.header.hash();

        let sanitized_extra_data = sanitize_bytes(header.extra_data.as_ref());

        let result = client.execute(
            "INSERT INTO headers (block_number, timestamp, parent_hash, hash, beneficiary, difficulty, gas_limit, gas_used, base_fee_per_gas, blob_gas_used, excess_blob_gas, extra_data, updated_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
            &[
                &(header.number as i64),
                &(header.timestamp as i64),
                &header.parent_hash.to_string(),
                &hash.to_string(),
                &header.beneficiary.to_string(),
                &header.difficulty.to_string(),
                &(header.gas_limit as i64),
                &(header.gas_used as i64),
                &header.base_fee_per_gas.map(|fee| fee as i64),
                &header.blob_gas_used.map(|gas| gas as i64),
                &header.excess_blob_gas.map(|gas| gas as i64),
                &sanitized_extra_data,
                &Utc::now(),
            ],
        ).await?;

        Ok(ProcessingResult { records_written: result as usize })
    }

    async fn revert(&self, block_numbers: &[i64], client: &Arc<Client>) -> eyre::Result<()> {
        client.execute(
            "DELETE FROM headers WHERE block_number = ANY($1::bigint[])",
            &[&block_numbers]
        ).await?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct TransactionsEvent;

#[async_trait]
impl ProcessingEvent for TransactionsEvent {
    fn name(&self) -> &'static str {
        "TransactionsEvent"
    }

    async fn process(&self, block_data: &(SealedBlockWithSenders, Vec<Option<Receipt>>), client: &Arc<Client>) -> eyre::Result<ProcessingResult> {
        let block = &block_data.0;
        let receipts = &block_data.1;
        let transactions: Vec<TransactionSigned> = block.block.body.transactions.to_vec();

        // Initialize the COPY command
        let sink = client
            .copy_in("COPY transactions (
            block_number,
            tx_hash,
            chain_id,
            from_addr,
            to_addr,
            nonce,
            max_fee_per_gas,
            max_priority_fee_per_gas,
            gas_limit,
            gas_used,
            value,
            input,
            tx_type,
            access_list,
            authorization_list,
            is_dynamic_fee,
            blob_versioned_hashes,
            max_fee_per_blob_gas,
            blob_gas_used,
            log_count,
            success,
            updated_at
        ) FROM STDIN BINARY")
            .await?;

        let writer = BinaryCopyInWriter::new(
            sink,
            &[
                Type::INT8,      // block_number
                Type::TEXT,      // tx_hash
                Type::INT8,      // chain_id
                Type::TEXT,      // from_addr
                Type::TEXT,      // to_addr
                Type::INT8,      // nonce
                Type::INT8,      // max_fee_per_gas
                Type::INT8,      // max_priority_fee_per_gas
                Type::INT8,      // gas_limit
                Type::INT8,      // gas_used
                Type::TEXT,      // value
                Type::TEXT,      // input
                Type::TEXT,      // tx_type
                Type::TEXT,      // access_list
                Type::TEXT,      // authorization_list
                Type::BOOL,      // is_dynamic_fee
                Type::TEXT,      // blob_versioned_hashes
                Type::INT8,      // max_fee_per_blob_gas
                Type::INT8,      // blob_gas_used
                Type::INT8,      // log_count
                Type::BOOL,      // success
                Type::TIMESTAMPTZ, // updated_at
            ],
        );
        pin_mut!(writer);

        let mut previous_cumulative_gas = 0u64;

        for (tx, receipt) in transactions.into_iter().zip(receipts.iter()) {
            // Calculate individual gas used for this transaction
            let gas_used = receipt.as_ref().map(|r| {
                let individual_gas = r.cumulative_gas_used - previous_cumulative_gas;
                previous_cumulative_gas = r.cumulative_gas_used;
                individual_gas as i64
            });

            // Convert AccessList to JSON string
            let access_list_str = tx.access_list()
                .map(|list| serde_json::to_string(&list).unwrap_or_default());

            // Convert AuthorizationList to JSON string
            let auth_list_str = tx.authorization_list()
                .map(|list| serde_json::to_string(&list).unwrap_or_default());

            // Convert blob versioned hashes to string
            let blob_hashes_str = tx.blob_versioned_hashes()
                .map(|hashes| hashes.iter()
                    .map(|hash| hash.to_string())
                    .collect::<Vec<_>>()
                    .join(","));

            writer
                .as_mut()
                .write(&[
                    &(block.block.header.header().number as i64),
                    &tx.hash().to_string(),
                    &tx.chain_id().map(|id| id as i64),
                    &tx.recover_signer_unchecked().map(|addr| addr.to_string()),
                    &tx.to().map(|addr| addr.to_string()),
                    &(tx.nonce() as i64),
                    &(tx.max_fee_per_gas() as i64),
                    &tx.max_priority_fee_per_gas().map(|gas| gas as i64),
                    &(tx.gas_limit() as i64),
                    &gas_used,
                    &tx.value().to_string(),
                    &tx.input().to_string(),
                    &format!("{:?}", tx.tx_type()),
                    &access_list_str,
                    &auth_list_str,
                    &tx.is_dynamic_fee(),
                    &blob_hashes_str,
                    &tx.max_fee_per_blob_gas().map(|gas| gas as i64),
                    &tx.blob_gas_used().map(|gas| gas as i64),
                    &receipt.as_ref().map(|r| r.logs.len() as i64).unwrap_or(0),
                    &receipt.as_ref().map(|r| r.success).unwrap_or(false),
                    &Utc::now(),
                ])
                .await?;
        }

        let rows_affected = writer.finish().await?;

        Ok(ProcessingResult { records_written: rows_affected as usize })
    }

    async fn revert(&self, block_numbers: &[i64], client: &Arc<Client>) -> eyre::Result<()> {
        client.execute(
            "DELETE FROM transactions WHERE block_number = ANY($1::bigint[])",
            &[&block_numbers]
        ).await?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct LogsEvent;

#[async_trait]
impl ProcessingEvent for LogsEvent {
    fn name(&self) -> &'static str {
        "LogsEvent"
    }

    async fn process(&self, block_data: &(SealedBlockWithSenders, Vec<Option<Receipt>>), client: &Arc<Client>) -> eyre::Result<ProcessingResult> {
        let block = &block_data.0;
        let receipts = &block_data.1;
        let block_number = block.block.header.header().number;
        let block_hash = block.block.header.hash();

        // Initialize the COPY command
        let sink = client
            .copy_in("COPY logs (
                block_number,
                block_hash,
                transaction_index,
                log_index,
                transaction_hash,
                address,
                topic0,
                topic1,
                topic2,
                topic3,
                data,
                n_data_bytes,
                chain_id,
                updated_at
            ) FROM STDIN BINARY")
            .await?;

        let writer = BinaryCopyInWriter::new(
            sink,
            &[
                Type::INT8,      // block_number
                Type::TEXT,      // block_hash
                Type::INT8,      // transaction_index
                Type::INT8,      // log_index
                Type::TEXT,      // transaction_hash
                Type::TEXT,      // address
                Type::TEXT,      // topic0
                Type::TEXT,      // topic1
                Type::TEXT,      // topic2
                Type::TEXT,      // topic3
                Type::TEXT,      // data
                Type::INT8,      // n_data_bytes
                Type::INT8,      // chain_id
                Type::TIMESTAMPTZ, // updated_at
            ],
        );
        pin_mut!(writer);

        let mut total_logs = 0;

        // Iterate through transactions and their receipts
        for (tx_idx, (tx, receipt)) in block.block.body.transactions.iter().zip(receipts.iter()).enumerate() {
            if let Some(receipt) = receipt {
                // Process each log in the receipt
                for (log_idx, log) in receipt.logs.iter().enumerate() {
                    let topics = log.topics();

                    writer
                        .as_mut()
                        .write(&[
                            &(block_number as i64),
                            &block_hash.to_string(),
                            &(tx_idx as i64),
                            &(log_idx as i64),
                            &tx.hash().to_string(),
                            &log.address.to_string(),
                            &topics.get(0).map(|t| t.to_string()),
                            &topics.get(1).map(|t| t.to_string()),
                            &topics.get(2).map(|t| t.to_string()),
                            &topics.get(3).map(|t| t.to_string()),
                            &hex::encode(&log.data.data),
                            &(log.data.data.len() as i64),
                            &tx.chain_id().map(|id| id as i64),
                            &Utc::now(),
                        ])
                        .await?;

                    total_logs += 1;
                }
            }
        }

        let rows_affected = writer.finish().await?;

        Ok(ProcessingResult { records_written: rows_affected as usize })
    }

    async fn revert(&self, block_numbers: &[i64], client: &Arc<Client>) -> eyre::Result<()> {
        client.execute(
            "DELETE FROM logs WHERE block_number = ANY($1::bigint[])",
            &[&block_numbers]
        ).await?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct BuilderBidsEvent;

#[async_trait]
impl ProcessingEvent for BuilderBidsEvent {
    fn name(&self) -> &'static str {
        "BuilderBidsEvent"
    }

    async fn process(&self, block_data: &(SealedBlockWithSenders, Vec<Option<Receipt>>), client: &Arc<Client>) -> eyre::Result<ProcessingResult> {
        let block = &block_data.0;
        let block_number = block.block.header.header().number;
        let semaphore = Arc::new(Semaphore::new(10));

        let relay_futures = RELAYS.iter().map(|relay| {
            let semaphore = Arc::clone(&semaphore);
            let (relay_id, relay_client) = RelayClient::from_known_relay(relay.clone());
            async move {
                let _permit = semaphore.acquire().await.unwrap();
                let result = relay_client.get_builder_block_received(100, block_number).await;
                (relay_id, result)
            }
        });

        let results = join_all(relay_futures).await;

        let mut all_blocks = Vec::new();
        let mut relay_errors = std::collections::HashMap::new();

        for (relay_id, result) in results {
            match result {
                Ok(Some(blocks)) => {
                    all_blocks.extend(blocks.into_iter().map(|block| (relay_id.clone(), block)));
                }
                Ok(None) => {}
                Err(err) => {
                    relay_errors.insert(relay_id, err);
                }
            }
        }

        let sink = client
            .copy_in("COPY builder_bids (block_number, relay_id, slot, parent_hash, block_hash, builder_pubkey, proposer_pubkey, proposer_fee_recipient, gas_limit, gas_used, value, num_tx, timestamp, timestamp_ms, optimistic_submission, updated_at) FROM STDIN BINARY")
            .await?;

        let writer = BinaryCopyInWriter::new(
            sink,
            &[
                Type::INT8,
                Type::TEXT,
                Type::INT8,
                Type::TEXT,
                Type::TEXT,
                Type::TEXT,
                Type::TEXT,
                Type::TEXT,
                Type::INT8,
                Type::INT8,
                Type::TEXT,
                Type::INT8,
                Type::INT8,
                Type::INT8,
                Type::BOOL,
                Type::TIMESTAMPTZ,
            ],
        );
        pin_mut!(writer);

        for (relay_id, block) in all_blocks {
            writer
                .as_mut()
                .write(&[
                    &(block.block_number as i64),
                    &relay_id,
                    &(block.slot as i64),
                    &block.parent_hash.to_string(),
                    &block.block_hash.to_string(),
                    &format!("{:?}", block.builder_pubkey),
                    &format!("{:?}", block.proposer_pubkey),
                    &block.proposer_fee_recipient.to_string(),
                    &(block.gas_limit as i64),
                    &(block.gas_used as i64),
                    &block.value.to_string(),
                    &(block.num_tx as i64),
                    &(block.timestamp as i64),
                    &(block.timestamp_ms as i64),
                    &block.optimistic_submission,
                    &Utc::now(),
                ])
                .await?;
        }

        let rows_affected = writer.finish().await?;

        if !relay_errors.is_empty() {
            for (relay_id, error) in &relay_errors {
                warn!("Relay error for block {}: Relay ID: {}, Error: {:?}", block_number, relay_id, error);
            }
        }

        Ok(ProcessingResult { records_written: rows_affected as usize })
    }

    async fn revert(&self, block_numbers: &[i64], client: &Arc<Client>) -> eyre::Result<()> {
        client.execute(
            "DELETE FROM builder_bids WHERE block_number = ANY($1::bigint[])",
            &[&block_numbers]
        ).await?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct ProposerPayloadsEvent;

#[async_trait]
impl ProcessingEvent for ProposerPayloadsEvent {
    fn name(&self) -> &'static str {
        "ProposerPayloadsEvent"
    }

    async fn process(&self, block_data: &(SealedBlockWithSenders, Vec<Option<Receipt>>), client: &Arc<Client>) -> eyre::Result<ProcessingResult> {
        let block = &block_data.0;
        let block_number = block.block.header.header().number;
        let semaphore = Arc::new(Semaphore::new(10));

        let relay_futures = RELAYS.iter().map(|relay| {
            let semaphore = Arc::clone(&semaphore);
            let (relay_id, relay_client) = RelayClient::from_known_relay(relay.clone());
            async move {
                let _permit = semaphore.acquire().await.unwrap();
                let result = relay_client.get_delivered_payload(100, block_number).await;
                (relay_id, result)
            }
        });

        let results = join_all(relay_futures).await;

        let mut all_payloads = Vec::new();
        let mut relay_errors = std::collections::HashMap::new();

        for (relay_id, result) in results {
            match result {
                Ok(Some(payloads)) => {
                    all_payloads.extend(payloads.into_iter().map(|payload| (relay_id.clone(), payload)));
                }
                Ok(None) => {}
                Err(err) => {
                    relay_errors.insert(relay_id, err);
                }
            }
        }

        let sink = client
            .copy_in("COPY proposer_payloads (
                block_number,
                relay_id,
                slot,
                parent_hash,
                block_hash,
                builder_pubkey,
                proposer_pubkey,
                proposer_fee_recipient,
                gas_limit,
                gas_used,
                value,
                num_tx,
                updated_at
            ) FROM STDIN BINARY")
            .await?;

        let writer = BinaryCopyInWriter::new(
            sink,
            &[
                Type::INT8,      // block_number
                Type::TEXT,      // relay_id
                Type::INT8,      // slot
                Type::TEXT,      // parent_hash
                Type::TEXT,      // block_hash
                Type::TEXT,      // builder_pubkey
                Type::TEXT,      // proposer_pubkey
                Type::TEXT,      // proposer_fee_recipient
                Type::INT8,      // gas_limit
                Type::INT8,      // gas_used
                Type::TEXT,      // value
                Type::INT8,      // num_tx
                Type::TIMESTAMPTZ, // updated_at
            ],
        );
        pin_mut!(writer);

        for (relay_id, payload) in all_payloads {
            writer
                .as_mut()
                .write(&[
                    &(payload.block_number as i64),
                    &relay_id,
                    &(payload.slot as i64),
                    &payload.parent_hash.to_string(),
                    &payload.block_hash.to_string(),
                    &format!("{:?}", payload.builder_pubkey),
                    &format!("{:?}", payload.proposer_pubkey),
                    &payload.proposer_fee_recipient.to_string(),
                    &(payload.gas_limit as i64),
                    &(payload.gas_used as i64),
                    &payload.value.to_string(),
                    &(payload.num_tx as i64),
                    &Utc::now(),
                ])
                .await?;
        }

        let rows_affected = writer.finish().await?;

        if !relay_errors.is_empty() {
            for (relay_id, error) in &relay_errors {
                warn!("Relay error for block {}: Relay ID: {}, Error: {:?}", block_number, relay_id, error);
            }
        }

        Ok(ProcessingResult { records_written: rows_affected as usize })
    }

    async fn revert(&self, block_numbers: &[i64], client: &Arc<Client>) -> eyre::Result<()> {
        client.execute(
            "DELETE FROM proposer_payloads WHERE block_number = ANY($1::bigint[])",
            &[&block_numbers]
        ).await?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct OmmersEvent;

#[async_trait]
impl ProcessingEvent for OmmersEvent {
    fn name(&self) -> &'static str {
        "OmmersEvent"
    }

    async fn process(&self, block_data: &(SealedBlockWithSenders, Vec<Option<Receipt>>), client: &Arc<Client>) -> eyre::Result<ProcessingResult> {
        let block = &block_data.0;
        let block_number = block.block.header.header().number;
        let block_hash = block.block.header.hash();
        let ommers = &block.block.body.ommers;

        // Initialize the COPY command
        let sink = client
            .copy_in("COPY ommers (
                block_number,
                block_hash,
                ommer_index,
                parent_hash,
                ommer_hash,
                beneficiary,
                state_root,
                transactions_root,
                receipts_root,
                difficulty,
                number,
                gas_limit,
                gas_used,
                timestamp,
                mix_hash,
                nonce,
                base_fee_per_gas,
                withdrawals_root,
                blob_gas_used,
                excess_blob_gas,
                parent_beacon_block_root,
                extra_data,
                updated_at
            ) FROM STDIN BINARY")
            .await?;

        let writer = BinaryCopyInWriter::new(
            sink,
            &[
                Type::INT8,      // block_number
                Type::TEXT,      // block_hash
                Type::INT8,      // ommer_index
                Type::TEXT,      // parent_hash
                Type::TEXT,      // ommer_hash
                Type::TEXT,      // beneficiary
                Type::TEXT,      // state_root
                Type::TEXT,      // transactions_root
                Type::TEXT,      // receipts_root
                Type::TEXT,      // difficulty
                Type::INT8,      // number
                Type::INT8,      // gas_limit
                Type::INT8,      // gas_used
                Type::INT8,      // timestamp
                Type::TEXT,      // mix_hash
                Type::TEXT,      // nonce
                Type::INT8,      // base_fee_per_gas
                Type::TEXT,      // withdrawals_root
                Type::INT8,      // blob_gas_used
                Type::INT8,      // excess_blob_gas
                Type::TEXT,      // parent_beacon_block_root
                Type::TEXT,      // extra_data
                Type::TIMESTAMPTZ, // updated_at
            ],
        );
        pin_mut!(writer);

        for (idx, ommer) in ommers.iter().enumerate() {
            writer
                .as_mut()
                .write(&[
                    &(block_number as i64),
                    &block_hash.to_string(),
                    &(idx as i64),
                    &ommer.parent_hash.to_string(),
                    &ommer.ommers_hash.to_string(),
                    &ommer.beneficiary.to_string(),
                    &ommer.state_root.to_string(),
                    &ommer.transactions_root.to_string(),
                    &ommer.receipts_root.to_string(),
                    &ommer.difficulty.to_string(),
                    &(ommer.number as i64),
                    &(ommer.gas_limit as i64),
                    &(ommer.gas_used as i64),
                    &(ommer.timestamp as i64),
                    &ommer.mix_hash.to_string(),
                    &format!("{:?}", ommer.nonce),
                    &ommer.base_fee_per_gas.map(|fee| fee as i64),
                    &ommer.withdrawals_root.as_ref().map(|root| root.to_string()),
                    &ommer.blob_gas_used.map(|gas| gas as i64),
                    &ommer.excess_blob_gas.map(|gas| gas as i64),
                    &ommer.parent_beacon_block_root.as_ref().map(|root| root.to_string()),
                    &hex::encode(&ommer.extra_data),
                    &Utc::now(),
                ])
                .await?;
        }

        let rows_affected = writer.finish().await?;

        Ok(ProcessingResult { records_written: rows_affected as usize })
    }

    async fn revert(&self, block_numbers: &[i64], client: &Arc<Client>) -> eyre::Result<()> {
        client.execute(
            "DELETE FROM ommers WHERE block_number = ANY($1::bigint[])",
            &[&block_numbers]
        ).await?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct WithdrawalsEvent;

#[async_trait]
impl ProcessingEvent for WithdrawalsEvent {
    fn name(&self) -> &'static str {
        "WithdrawalsEvent"
    }

    async fn process(&self, block_data: &(SealedBlockWithSenders, Vec<Option<Receipt>>), client: &Arc<Client>) -> eyre::Result<ProcessingResult> {
        let block = &block_data.0;
        let block_number = block.block.header.header().number;
        let block_hash = block.block.header.hash();

        // Get withdrawals if they exist
        let withdrawals: Withdrawals = match &block.block.body.withdrawals {
            Some(withdrawals) => withdrawals.clone(),
            None => return Ok(ProcessingResult { records_written: 0 }),
        };

        // Initialize the COPY command
        let sink = client
            .copy_in("COPY withdrawals (
                block_number,
                block_hash,
                withdrawal_index,
                validator_index,
                address,
                amount_gwei,
                amount_wei,
                updated_at
            ) FROM STDIN BINARY")
            .await?;

        let writer = BinaryCopyInWriter::new(
            sink,
            &[
                Type::INT8,      // block_number
                Type::TEXT,      // block_hash
                Type::INT8,      // withdrawal_index
                Type::INT8,      // validator_index
                Type::TEXT,      // address
                Type::INT8,      // amount_gwei
                Type::TEXT,      // amount_wei
                Type::TIMESTAMPTZ, // updated_at
            ],
        );
        pin_mut!(writer);

        for withdrawal in withdrawals.into_inner() {
            writer
                .as_mut()
                .write(&[
                    &(block_number as i64),
                    &block_hash.to_string(),
                    &(withdrawal.index as i64),
                    &(withdrawal.validator_index as i64),
                    &withdrawal.address.to_string(),
                    &(withdrawal.amount as i64),
                    &withdrawal.amount_wei().to_string(),
                    &Utc::now(),
                ])
                .await?;
        }

        let rows_affected = writer.finish().await?;

        Ok(ProcessingResult { records_written: rows_affected as usize })
    }

    async fn revert(&self, block_numbers: &[i64], client: &Arc<Client>) -> eyre::Result<()> {
        client.execute(
            "DELETE FROM withdrawals WHERE block_number = ANY($1::bigint[])",
            &[&block_numbers]
        ).await?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct Erc20TransfersEvent;

impl Erc20TransfersEvent {
    fn is_transfer_event(log: &Log) -> bool {
        // Check all conditions that must be true for a valid ERC20 transfer:
        // 1. First topic must match Transfer event signature
        // 2. Must have exactly 3 topics (event signature + from + to)
        // 3. Data must be exactly 32 bytes (uint256 value)
        log.topics().len() == 3 &&
            log.data.data.len() == 32 &&
            log.topics().get(0).map_or(false, |topic| topic == ERC20_TRANSFER_TOPIC.as_ref())
    }

    fn parse_transfer_event(log: &Log) -> Option<(Address, Address, String)> {
        // Topics[0] is the event signature
        // Topics[1] is the from address
        // Topics[2] is the to address
        let from_address = Address::from_slice(&log.topics()[1].as_slice()[12..]);
        let to_address = Address::from_slice(&log.topics()[2].as_slice()[12..]);

        // Convert value to string
        let value = U256::from_big_endian(&log.data.data).to_string();

        Some((from_address, to_address, value))
    }
}

#[async_trait]
impl ProcessingEvent for Erc20TransfersEvent {
    fn name(&self) -> &'static str {
        "Erc20TransfersEvent"
    }

    async fn process(&self, block_data: &(SealedBlockWithSenders, Vec<Option<Receipt>>), client: &Arc<Client>) -> eyre::Result<ProcessingResult> {
        let block = &block_data.0;
        let receipts = &block_data.1;
        let block_number = block.block.header.header().number;
        let block_hash = block.block.header.hash();

        // Initialize the COPY command
        let sink = client
            .copy_in("COPY erc20_transfers (
                block_number,
                block_hash,
                transaction_index,
                log_index,
                transaction_hash,
                erc20,
                from_address,
                to_address,
                value,
                chain_id,
                updated_at
            ) FROM STDIN BINARY")
            .await?;

        let writer = BinaryCopyInWriter::new(
            sink,
            &[
                Type::INT8,        // block_number
                Type::TEXT,        // block_hash
                Type::INT4,        // transaction_index
                Type::INT4,        // log_index
                Type::TEXT,        // transaction_hash
                Type::TEXT,        // erc20
                Type::TEXT,        // from_address
                Type::TEXT,        // to_address
                Type::TEXT,        // value
                Type::INT8,        // chain_id
                Type::TIMESTAMPTZ, // updated_at
            ],
        );
        pin_mut!(writer);

        let mut records_written = 0;

        // Iterate through transactions and their receipts
        for (tx_idx, (tx, receipt)) in block.block.body.transactions.iter().zip(receipts.iter()).enumerate() {
            if let Some(receipt) = receipt {
                // Process each log in the receipt
                for (log_idx, log) in receipt.logs.iter().enumerate() {
                    if Self::is_transfer_event(log) {
                        if let Some((from_address, to_address, value)) = Self::parse_transfer_event(log) {
                            writer
                                .as_mut()
                                .write(&[
                                    &(block_number as i64),
                                    &block_hash.to_string(),
                                    &(tx_idx as i32),
                                    &(log_idx as i32),
                                    &tx.hash().to_string(),
                                    &log.address.to_string(),  // ERC20 contract address
                                    &from_address.to_checksum(Some(1)),
                                    &to_address.to_checksum(Some(1)),
                                    &value,
                                    &tx.chain_id().map(|id| id as i64),
                                    &Utc::now(),
                                ])
                                .await?;

                            records_written += 1;
                        }
                    }
                }
            }
        }

        let rows_affected = writer.finish().await?;
        assert_eq!(records_written, rows_affected as usize);

        Ok(ProcessingResult { records_written })
    }

    async fn revert(&self, block_numbers: &[i64], client: &Arc<Client>) -> eyre::Result<()> {
        client.execute(
            "DELETE FROM erc20_transfers WHERE block_number = ANY($1::bigint[])",
            &[&block_numbers]
        ).await?;
        Ok(())
    }
}

// Event enum
#[derive(Clone)]
pub enum Event {
    Header(HeaderEvent),
    BuilderBids(BuilderBidsEvent),
    Transactions(TransactionsEvent),
    ProposerPayloads(ProposerPayloadsEvent),
    Logs(LogsEvent),
    Ommers(OmmersEvent),
    Withdrawals(WithdrawalsEvent),
    Erc20Transfers(Erc20TransfersEvent),
}

impl Event {
    pub fn name(&self) -> &'static str {
        match self {
            Event::Header(e) => e.name(),
            Event::BuilderBids(e) => e.name(),
            Event::Transactions(e) => e.name(),
            Event::ProposerPayloads(e) => e.name(),
            Event::Logs(e) => e.name(),
            Event::Ommers(e) => e.name(),
            Event::Withdrawals(e) => e.name(),
            Event::Erc20Transfers(e) => e.name(),
        }
    }

    pub async fn process(&self, block_data: &(SealedBlockWithSenders, Vec<Option<Receipt>>), client: &Arc<Client>) -> eyre::Result<ProcessingResult> {
        match self {
            Event::Header(e) => e.process(block_data, client).await,
            Event::BuilderBids(e) => e.process(block_data, client).await,
            Event::Transactions(e) => e.process(block_data, client).await,
            Event::ProposerPayloads(e) => e.process(block_data, client).await,
            Event::Logs(e) => e.process(block_data, client).await,
            Event::Ommers(e) => e.process(block_data, client).await,
            Event::Withdrawals(e) => e.process(block_data, client).await,
            Event::Erc20Transfers(e) => e.process(block_data, client).await,
        }
    }

    pub async fn revert(&self, block_numbers: &[i64], client: &Arc<Client>) -> eyre::Result<()> {
        match self {
            Event::Header(e) => e.revert(block_numbers, client).await,
            Event::BuilderBids(e) => e.revert(block_numbers, client).await,
            Event::Transactions(e) => e.revert(block_numbers, client).await,
            Event::ProposerPayloads(e) => e.revert(block_numbers, client).await,
            Event::Logs(e) => e.revert(block_numbers, client).await,
            Event::Ommers(e) => e.revert(block_numbers, client).await,
            Event::Withdrawals(e) => e.revert(block_numbers, client).await,
            Event::Erc20Transfers(e) => e.revert(block_numbers, client).await,
        }
    }

    pub fn all() -> Vec<Event> {
        vec![
            Event::Header(HeaderEvent),
            Event::BuilderBids(BuilderBidsEvent),
            Event::Transactions(TransactionsEvent),
            Event::ProposerPayloads(ProposerPayloadsEvent),
            Event::Logs(LogsEvent),
            Event::Ommers(OmmersEvent),
            Event::Withdrawals(WithdrawalsEvent),
            Event::Erc20Transfers(Erc20TransfersEvent),
        ]
    }
}