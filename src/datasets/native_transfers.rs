use crate::record_values;
use crate::db_writer::DbWriter;
use alloy_rpc_types_trace::parity::{TraceOutput, Action};
use reth_primitives::{SealedBlockWithSenders, Receipt};
use chrono::Utc;
use eyre::Result;
use reth_node_api::FullNodeComponents;
use crate::indexer::ProcessingComponents;

pub async fn process_native_transfers<Node: FullNodeComponents>(
    block_data: &(SealedBlockWithSenders, Vec<Option<Receipt>>),
    components: ProcessingComponents<Node>,
    writer: &mut DbWriter,
) -> Result<()> {
    let block = &block_data.0;
    let block_number = block.block.header.header().number;
    let block_hash = block.block.header.hash();

    // Process traces for all transfers (both native and internal)
    if let Some(traces) = components.block_traces {
        let mut transfer_index = 0;

        for trace in traces {
            // Find transaction position by matching the hash
            let tx_position = block.body().transactions
                .iter()
                .position(|tx| tx.hash() == trace.transaction_hash)
                .map(|pos| pos as i32);

            // Process each trace in the vector of traces
            for (sub_trace_idx, sub_trace) in trace.full_trace.trace.iter().enumerate() {
                match &sub_trace.action {
                    // Handle both native transactions and internal calls
                    Action::Call(call) => {
                        if !call.value.is_zero() {
                            let transfer_type = if sub_trace_idx == 0 {
                                "transaction" // Native transfer (index 0 means top-level transaction)
                            } else {
                                "internal_call" // Internal transfer
                            };

                            writer
                                .write_record(record_values![
                                    block_number as i64,
                                    block_hash,
                                    tx_position,
                                    transfer_index as i32,
                                    trace.transaction_hash,
                                    call.from,
                                    call.to,
                                    call.value,
                                    transfer_type,
                                    Utc::now(),
                                ])
                                .await?;
                            transfer_index += 1;
                        }
                    }
                    Action::Create(create) if !create.value.is_zero() => {
                        writer
                            .write_record(record_values![
                                block_number as i64,
                                block_hash,
                                tx_position,
                                transfer_index as i32,
                                trace.transaction_hash,
                                create.from,
                                sub_trace.result.as_ref()
                                    .and_then(|r| match r {
                                        TraceOutput::Create(create) => Some(create.address),
                                        _ => None,
                                    })
                                    .unwrap_or_default(),
                                create.value,
                                "contract_creation",
                                Utc::now(),
                            ])
                            .await?;
                        transfer_index += 1;
                    }
                    Action::Selfdestruct(selfdestruct) => {
                        writer
                            .write_record(record_values![
                                block_number as i64,
                                block_hash,
                                tx_position,
                                transfer_index as i32,
                                trace.transaction_hash,
                                selfdestruct.address,
                                selfdestruct.refund_address,
                                selfdestruct.balance,
                                "selfdestruct",
                                Utc::now(),
                            ])
                            .await?;
                        transfer_index += 1;
                    }
                    Action::Reward(reward) => {
                        writer
                            .write_record(record_values![
                                block_number as i64,
                                block_hash,
                                None::<i32>,
                                transfer_index as i32,
                                None::<String>,
                                "0x0",
                                reward.author,
                                reward.value,
                                format!("reward_{:?}", reward.reward_type).to_lowercase(),
                                Utc::now(),
                            ])
                            .await?;
                        transfer_index += 1;
                    }
                    _ => {}
                }
            }
        }
    }

    Ok(())
}