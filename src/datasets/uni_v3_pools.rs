use crate::record_values;
use crate::db_writer::DbWriter;
use alloy::{sol, sol_types::SolEvent, primitives::{address, Address}};
use alloy_consensus::BlockHeader;
use reth_primitives::{SealedBlockWithSenders, Receipt};
use chrono::Utc;
use eyre::Result;
use reth_node_api::FullNodeComponents;
use tracing::debug;
use crate::indexer::ProcessingComponents;

const UNIV3_FACTORY_CONTRACT_ADDRESS: Address = address!("0x1F98431c8aD98523631AE4a59f267346ea31F984");

sol! {
    event PoolCreated(
        address indexed token0,
        address indexed token1,
        uint24 indexed fee,
        int24 tickSpacing,
        address pool
    );
}

pub async fn process_uni_v3_pools<Node: FullNodeComponents>(
    block_data: &(SealedBlockWithSenders, Vec<Option<Receipt>>),
    _components: ProcessingComponents<Node>,
    writer: &mut DbWriter,
) -> Result<()> {
    let block = &block_data.0;
    let receipts = &block_data.1;
    let block_number = block.block.header().number();

    // Iterate through transactions and their receipts
    for (tx_idx, (tx, receipt)) in block.body().transactions.iter().zip(receipts.iter()).enumerate() {
        if let Some(receipt) = receipt {
            // Process each log in the receipt
            for (log_idx, log) in receipt.logs.iter().enumerate() {
                // Filter on univ3 contract address
                if log.address == UNIV3_FACTORY_CONTRACT_ADDRESS {
                    // First check if this log matches our event signature
                    if log.topics().get(0) != Some(&PoolCreated::SIGNATURE_HASH) {
                        continue;
                    }

                    match PoolCreated::decode_raw_log(log.topics(), &log.data.data, true) {
                        Ok(create) => {
                            writer
                                .write_record(record_values![
                                block_number as i64,
                                tx.hash(),
                                tx_idx as i64,
                                log_idx as i64,
                                log.address,
                                create.token0,
                                create.token1,
                                create.fee.to_string(),
                                create.tickSpacing.to_string(),
                                create.pool,
                                Utc::now(),
                            ])
                                .await?;
                        }
                        Err(e) => {
                            debug!("Failed to decode univ3 pool creation event: {:?}", e);
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

