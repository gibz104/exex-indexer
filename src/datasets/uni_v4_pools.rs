use crate::record_values;
use crate::indexer::{ProcessingComponents, EthereumBlockData};
use crate::db_writer::DbWriter;
use alloy::{sol, sol_types::SolEvent, primitives::{address, Address}};
use reth_node_api::FullNodeComponents;
use eyre::Result;
use chrono::Utc;
use reth_rpc_eth_api::helpers::FullEthApi;
use tracing::debug;

const UNIV4_FACTORY_CONTRACT_ADDRESS: Address = address!("0x000000000004444c5dc75cB358380D2e3dE08A90");

sol! {
    event Initialize(
        bytes32 indexed id,
        address indexed currency0,
        address indexed currency1,
        uint24 fee,
        int24 tickSpacing,
        address hooks,
        uint160 sqrtPriceX96,
        int24 tick
    );
}

pub async fn process_uni_v4_pools<Node: FullNodeComponents, EthApi: FullEthApi>(
    block_data: &EthereumBlockData,
    _components: ProcessingComponents<Node, EthApi>,
    writer: &mut DbWriter,
) -> Result<()> {
    let block = &block_data.0;
    let receipts = &block_data.1;
    let block_number = block.num_hash().number;

    // Iterate through transactions and their receipts
    for (tx_idx, (tx, receipt)) in block.body().transactions.iter().zip(receipts.iter()).enumerate() {
        // Process each log in the receipt
        for (log_idx, log) in receipt.logs.iter().enumerate() {
            // Filter on univ4 contract address
            if log.address == UNIV4_FACTORY_CONTRACT_ADDRESS {
                // First check if this log matches our event signature
                if log.topics().get(0) != Some(&Initialize::SIGNATURE_HASH) {
                    continue;
                }

                match Initialize::decode_raw_log(log.topics(), &log.data.data) {
                    Ok(create) => {
                        writer
                            .write_record(record_values![
                            block_number as i64,
                            tx.hash(),
                            tx_idx as i64,
                            log_idx as i64,
                            log.address,
                            create.id,
                            create.currency0,
                            create.currency1,
                            create.fee.to_string(),
                            create.tickSpacing.to_string(),
                            create.hooks,
                            create.sqrtPriceX96.to_string(),
                            create.tick.to_string(),
                            Utc::now(),
                        ])
                        .await?;
                    }
                    Err(e) => {
                        debug!("Failed to decode univ4 pool creation event: {:?}", e);
                    }
                }
            }
        }
    }

    Ok(())
}

