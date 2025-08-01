use crate::record_values;
use crate::db_writer::DbWriter;
use alloy::{sol, sol_types::SolEvent, primitives::{address, Address}};
use chrono::Utc;
use eyre::Result;
use reth_node_api::FullNodeComponents;
use reth_rpc_eth_api::helpers::FullEthApi;
use tracing::debug;
use crate::indexer::{ProcessingComponents, EthereumBlockData};

const UNIV2_FACTORY_CONTRACT_ADDRESS: Address = address!("0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f");

sol! {
    event PairCreated(
        address indexed token0,
        address indexed token1,
        address pair,
        uint256 pair_idx
    );
}

pub async fn process_uni_v2_pools<Node: FullNodeComponents, EthApi: FullEthApi>(
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
            // Filter on univ2 contract address
            if log.address == UNIV2_FACTORY_CONTRACT_ADDRESS {
                // First check if this log matches our event signature
                if log.topics().get(0) != Some(&PairCreated::SIGNATURE_HASH) {
                    continue;
                }

                match PairCreated::decode_raw_log(log.topics(), &log.data.data) {
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
                            create.pair,
                            create.pair_idx.to_string(),  // pair index
                            Utc::now(),
                        ])
                            .await?;
                    }
                    Err(e) => {
                        debug!("Failed to decode univ2 pool creation event: {:?}", e);
                    }
                }
            }
        }
    }

    Ok(())
}

