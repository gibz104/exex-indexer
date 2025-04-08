use crate::record_values;
use crate::db_writer::DbWriter;
use alloy::primitives::hex;
use alloy_rpc_types_eth::TransactionTrait;
use reth_primitives::{SealedBlockWithSenders, Receipt};
use chrono::Utc;
use eyre::Result;
use reth_node_api::FullNodeComponents;
use crate::indexer::ProcessingComponents;

pub async fn process_logs<Node: FullNodeComponents>(
    block_data: &(SealedBlockWithSenders, Vec<Option<Receipt>>),
    _components: ProcessingComponents<Node>,
    writer: &mut DbWriter,
) -> Result<()> {
    let block = &block_data.0;
    let receipts = &block_data.1;
    let block_number = block.block.header.header().number;
    let block_hash = block.block.header.hash();

    // Process each transaction's logs
    for (tx_idx, (tx, receipt)) in block.body().transactions.iter().zip(receipts.iter()).enumerate() {
        if let Some(receipt) = receipt {
            for (log_idx, log) in receipt.logs.iter().enumerate() {
                let topics = log.topics();
                writer
                    .write_record(record_values![
                        block_number as i64,
                        block_hash,
                        tx_idx as i64,
                        log_idx as i64,
                        tx.hash(),
                        log.address,
                        topics.get(0),
                        topics.get(1),
                        topics.get(2),
                        topics.get(3),
                        hex::encode(&log.data.data),
                        log.data.data.len() as i64,
                        tx.chain_id().map(|id| id as i64),
                        Utc::now(),
                    ])
                    .await?;
            }
        }
    }

    Ok(())
}
