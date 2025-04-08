use crate::record_values;
use crate::db_writer::DbWriter;
use alloy::{sol, sol_types::SolEvent};
use reth_primitives::{SealedBlockWithSenders, Receipt};
use alloy_rpc_types_eth::TransactionTrait;
use chrono::Utc;
use eyre::Result;
use reth_node_api::FullNodeComponents;
use tracing::debug;
use crate::indexer::ProcessingComponents;

sol! {
    event Transfer(
        address indexed from,
        address indexed to,
        uint256 value
    );
}

pub async fn process_erc20_transfers<Node: FullNodeComponents>(
    block_data: &(SealedBlockWithSenders, Vec<Option<Receipt>>),
    _components: ProcessingComponents<Node>,
    writer: &mut DbWriter,
) -> Result<()> {
    let block = &block_data.0;
    let receipts = &block_data.1;
    let block_number = block.block.header.header().number;
    let block_hash = block.block.header.hash();

    // Iterate through transactions and their receipts
    for (tx_idx, (tx, receipt)) in block.transactions().into_iter().zip(receipts).enumerate() {
        if let Some(receipt) = receipt {
            for (log_idx, log) in receipt.logs.iter().enumerate() {
                // First check if this log matches our Transfer event signature
                if log.topics().get(0) != Some(&Transfer::SIGNATURE_HASH) {
                    continue;
                }

                match Transfer::decode_raw_log(log.topics(), &log.data.data, true) {
                    Ok(transfer) => {
                        writer
                            .write_record(record_values![
                            block_number as i64,
                            block_hash,
                            tx_idx as i32,
                            log_idx as i32,
                            tx.hash(),
                            log.address,  // ERC20 contract address
                            transfer.from,
                            transfer.to,
                            transfer.value,
                            tx.chain_id().map(|id| id as i64),
                            Utc::now(),
                        ])
                            .await?;
                    }
                    Err(e) => {
                        debug!("Failed to decode transfer event: {:?}", e);
                    }
                }
            }
        }
    }

    Ok(())
}