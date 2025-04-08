use crate::record_values;
use crate::db_writer::DbWriter;
use alloy::primitives::hex;
use alloy_rpc_types_trace::parity::{Action, TraceOutput};
use reth_primitives::{SealedBlockWithSenders, Receipt};
use chrono::Utc;
use eyre::Result;
use reth_node_api::FullNodeComponents;
use crate::indexer::ProcessingComponents;

pub async fn process_traces<Node: FullNodeComponents>(
    block_data: &(SealedBlockWithSenders, Vec<Option<Receipt>>),
    components: ProcessingComponents<Node>,
    writer: &mut DbWriter,
) -> Result<()> {
    let block = &block_data.0;
    let receipts = &block_data.1;
    let block_number = block.block.header.header().number;
    let block_hash = block.block.header.hash();

    if let Some(traces) = components.block_traces {
        for (tx_idx, (trace, receipt)) in traces.iter().zip(receipts.iter()).enumerate() {
            let tx_success = receipt.as_ref().map(|r| r.success).unwrap_or(false);
            let tx_hash = trace.transaction_hash;

            // Process each trace in the vector of traces
            for (sub_trace_idx, sub_trace) in trace.full_trace.trace.iter().enumerate() {
                let trace_address = sub_trace_idx.to_string();

                let (
                    action_type,
                    from,
                    to,
                    value,
                    gas,
                    input,
                    output,
                    success,
                    deployed_contract_address,
                    deployed_contract_code,
                    call_type,
                    reward_type,
                    gas_used,
                ) = match &sub_trace.action {
                    Action::Call(call) => (
                        "call",
                        Some(call.from),
                        Some(call.to),
                        Some(call.value),
                        Some(call.gas as i64),
                        Some(hex::encode(&call.input)),
                        sub_trace.result.as_ref().map(|r| match r {
                            TraceOutput::Call(call) => hex::encode(&call.output),
                            _ => String::new(),
                        }),
                        sub_trace.result.is_some(),
                        None,
                        None,
                        Some(format!("{:?}", call.call_type)),
                        None,
                        sub_trace.result.as_ref().map(|r| r.gas_used() as i64),
                    ),
                    Action::Create(create) => (
                        "create",
                        Some(create.from),
                        None,
                        Some(create.value),
                        Some(create.gas as i64),
                        Some(hex::encode(&create.init)),
                        sub_trace.result.as_ref().map(|r| match r {
                            TraceOutput::Create(create) => hex::encode(&create.code),
                            _ => String::new(),
                        }),
                        sub_trace.result.is_some(),
                        sub_trace.result.as_ref().and_then(|r| match r {
                            TraceOutput::Create(create) => Some(create.address),
                            _ => None,
                        }),
                        sub_trace.result.as_ref().and_then(|r| match r {
                            TraceOutput::Create(create) => Some(hex::encode(&create.code)),
                            _ => None,
                        }),
                        None,
                        None,
                        sub_trace.result.as_ref().map(|r| r.gas_used() as i64),
                    ),
                    Action::Selfdestruct(selfdestruct) => (
                        "selfdestruct",
                        Some(selfdestruct.address),
                        Some(selfdestruct.refund_address),
                        Some(selfdestruct.balance),
                        None,
                        None,
                        None,
                        true,
                        None,
                        None,
                        None,
                        None,
                        None,
                    ),
                    Action::Reward(reward) => (
                        "reward",
                        None,
                        Some(reward.author),
                        Some(reward.value),
                        None,
                        None,
                        None,
                        true,
                        None,
                        None,
                        None,
                        Some(format!("{:?}", reward.reward_type)),
                        None,
                    ),
                };

                writer
                    .write_record(record_values![
                        block_number as i64,
                        block_hash,
                        tx_hash,
                        tx_idx as i32,
                        trace_address,
                        sub_trace.subtraces as i32,
                        action_type,
                        from,
                        to,
                        value,
                        gas,
                        gas_used,
                        input,
                        output,
                        success,
                        tx_success,
                        sub_trace.error.as_ref().map(|e| e.to_string()),
                        deployed_contract_address,
                        deployed_contract_code,
                        call_type,
                        reward_type,
                        Utc::now(),
                    ])
                    .await?;
            }
        }
    }

    Ok(())
}