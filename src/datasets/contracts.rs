use crate::record_values;
use crate::db_writer::DbWriter;
use alloy::primitives::{hex, keccak256, Address};
use alloy_rpc_types_eth::TransactionTrait;
use alloy_rpc_types_trace::parity::{TraceResultsWithTransactionHash, Action, TraceOutput};
use chrono::Utc;
use eyre::Result;
use reth_node_api::FullNodeComponents;
use reth_rpc_eth_api::helpers::FullEthApi;
use crate::indexer::{ProcessingComponents, EthereumBlockData};


/// Find the deployer (originating EOA) from a set of transaction traces
///
/// The trace structure is hierarchical:
/// - TraceResultsWithTransactionHash contains:
///   - transaction_hash: The hash of the transaction
///   - full_trace: TraceResults which contains:
///     - trace: Vec<TransactionTrace>: A vector of traces for this transaction
///
/// Each TransactionTrace in the vector represents a call in the transaction's execution path.
/// The first trace (index 0) is always the top-level call that initiated the transaction.
fn find_deployer(traces: &[TraceResultsWithTransactionHash], current_trace: &TraceResultsWithTransactionHash) -> Option<Address> {
    let tx_hash = current_trace.transaction_hash;

    traces.iter()
        .find(|trace| trace.transaction_hash == tx_hash)
        .and_then(|trace| {
            // Get the first trace from the vector
            trace.full_trace.trace.first()
        })
        .and_then(|first_trace| match &first_trace.action {
            Action::Call(call) => Some(call.from),
            Action::Create(create) => Some(create.from),
            _ => None,
        })
}

pub async fn process_contracts<Node: FullNodeComponents, EthApi: FullEthApi>(
    block_data: &EthereumBlockData,
    components: ProcessingComponents<Node, EthApi>,
    writer: &mut DbWriter,
) -> Result<()> {
    let block = &block_data.0;
    let block_number = block.num_hash().number;
    let block_hash = block.num_hash().hash;

    if let Some(traces) = components.block_traces {
        let mut create_index = 0;

        for trace in &traces {
            // Process each trace in the vector of traces
            for sub_trace in trace.full_trace.trace.iter() {
                if let Action::Create(create) = &sub_trace.action {
                    let init_code = &create.init;
                    let init_code_hash = keccak256(init_code);

                    // Get deployed code from trace result
                    if let Some(TraceOutput::Create(result)) = &sub_trace.result {
                        let contract_address = result.address;
                        let deployed_code = &result.code;
                        let code_hash = keccak256(deployed_code);

                        // Find the original deployer (EOA that initiated the transaction)
                        let deployer = find_deployer(&traces, trace)
                            .unwrap_or_else(|| Address::default());

                        // Factory is the immediate creator of the contract
                        let factory = create.from;

                        writer
                            .write_record(record_values![
                                block_number as i64,
                                block_hash,
                                create_index as i32,
                                trace.transaction_hash,
                                contract_address,
                                deployer,
                                factory,
                                hex::encode(init_code),
                                hex::encode(deployed_code),
                                hex::encode(&init_code_hash),
                                init_code.len() as i32,
                                deployed_code.len() as i32,
                                hex::encode(&code_hash),
                                block.clone().into_transactions()
                                    .iter()
                                    .find(|tx| tx.tx_hash().clone() == trace.transaction_hash)
                                    .and_then(|tx| tx.chain_id())
                                    .map(|id| id as i64),
                                Utc::now(),
                            ])
                            .await?;

                        create_index += 1;
                    }
                }
            }
        }
    }

    Ok(())
}