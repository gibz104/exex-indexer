use crate::record_values;
use crate::db_writer::DbWriter;
use alloy_consensus::transaction::SignerRecoverable;  // This allows us to use `tx.recover_signer()`
use chrono::Utc;
use eyre::Result;
use reth_node_api::FullNodeComponents;
use reth_rpc_eth_api::helpers::FullEthApi;
use alloy_rpc_types_eth::TransactionTrait;  // This trait allows us to call methods directly on tx like access_list()
use crate::indexer::{ProcessingComponents, EthereumBlockData};

pub async fn process_transactions<Node: FullNodeComponents, EthApi: FullEthApi>(
    block_data: &EthereumBlockData,
    _components: ProcessingComponents<Node, EthApi>,
    writer: &mut DbWriter,
) -> Result<()> {
    let block = &block_data.0;
    let receipts = &block_data.1;
    let block_number = block.num_hash().number;

    // Process each transaction
    let mut previous_cumulative_gas = 0;
    for (tx, receipt) in block.body().transactions.iter().zip(receipts.iter()) {
        let success = receipt.success;
        let log_count = receipt.logs.len() as i64;
        
        // Calculate individual gas used for this transaction
        let individual_gas = receipt.cumulative_gas_used - previous_cumulative_gas;
        previous_cumulative_gas = receipt.cumulative_gas_used;
        let gas_used = individual_gas as i64;

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
            .write_record(record_values![
                block_number as i64,
                tx.hash(),
                tx.chain_id().map(|id| id as i64),
                tx.recover_signer().unwrap_or_default(),
                tx.to(),
                tx.nonce() as i64,
                tx.max_fee_per_gas() as i64,
                tx.max_priority_fee_per_gas().map(|fee| fee as i64),
                tx.gas_limit() as i64,
                gas_used,
                tx.value(),
                tx.input(),
                format!("{:?}", tx.tx_type()),
                access_list_str,
                auth_list_str,
                tx.is_dynamic_fee(),
                blob_hashes_str,
                tx.max_fee_per_blob_gas().map(|fee| fee as i64),
                tx.blob_gas_used().map(|gas| gas as i64),
                log_count,
                success,
                Utc::now(),
            ])
            .await?;
    }

    Ok(())
}
