use crate::record_values;
use crate::db_writer::DbWriter;
use alloy::{sol, sol_types::SolCall, primitives::hex};
use reth_primitives::{SealedBlockWithSenders, Receipt};
use reth_node_api::FullNodeComponents;
use reth_rpc::EthApi;
use reth_rpc_eth_api::helpers::EthCall;
use reth_rpc_eth_api::helpers::{Call, LoadPendingBlock};
use reth_rpc_eth_api::FullEthApiTypes;
use alloy_rpc_types::{state::EvmOverrides, BlockId};
use alloy_rpc_types_eth::{TransactionTrait, transaction::TransactionRequest};
use alloy_rpc_types_trace::parity::{TraceOutput, Action};
use chrono::Utc;
use eyre::Result;
use crate::indexer::ProcessingComponents;

// Define the complete ERC20 interface
sol! {
    interface IERC20 {
        function name() external view returns (string);
        function symbol() external view returns (string);
        function decimals() external view returns (uint8);
        function totalSupply() external view returns (uint256);
        function balanceOf(address account) external view returns (uint256);
        function transfer(address recipient, uint256 amount) external returns (bool);
        function allowance(address owner, address spender) external view returns (uint256);
        function approve(address spender, uint256 amount) external returns (bool);
        function transferFrom(address sender, address recipient, uint256 amount) external returns (bool);
    }
}

// Check if contract bytecode contains ERC20 function signatures
fn is_erc20_contract(code: &[u8]) -> bool {
    let code_hex = hex::encode(code);

    // Check for presence of core ERC20 function signatures in the bytecode
    // We'll check for the most commonly implemented functions
    let required_selectors = [
        IERC20::transferCall::SELECTOR,
        IERC20::balanceOfCall::SELECTOR
    ];

    let optional_selectors = [
        IERC20::nameCall::SELECTOR,
        IERC20::symbolCall::SELECTOR,
        IERC20::decimalsCall::SELECTOR
    ];

    // Must have all required selectors
    let has_required = required_selectors.iter()
        .all(|selector| code_hex.contains(&hex::encode(selector)));

    // Must have at least one of the optional selectors
    let has_optional = optional_selectors.iter()
        .any(|selector| code_hex.contains(&hex::encode(selector)));

    has_required && has_optional
}

pub async fn process_erc20_metadata<Node: FullNodeComponents>(
    block_data: &(SealedBlockWithSenders, Vec<Option<Receipt>>),
    components: ProcessingComponents<Node>,
    writer: &mut DbWriter,
) -> Result<()>
where
    EthApi<Node::Provider, Node::Pool, Node::Network, Node::Evm>: Call + LoadPendingBlock + FullEthApiTypes,
{
    let block = &block_data.0;
    let block_number = block.block.header.header().number;

    // Look for contract creations in traces
    if let Some(traces) = components.block_traces {
        for trace in traces {
            // Process each trace in the vector of traces
            for sub_trace in trace.full_trace.trace.iter() {
                if let Action::Create(_create) = &sub_trace.action {
                    // Get deployed code from trace result
                    if let Some(TraceOutput::Create(result)) = &sub_trace.result {
                        // Check if the deployed code matches ERC20 patterns
                        if is_erc20_contract(&result.code) {
                            let contract_address = result.address;

                            // Get chain_id by finding the transaction in the block
                            let chain_id = block.block.body().transactions()
                                .find(|tx| tx.to_owned().hash() == trace.transaction_hash)
                                .and_then(|tx| tx.chain_id())
                                .map(|id| id as i64);

                            // Extract name from the contract
                            let name = {
                                let tx_request = TransactionRequest::default()
                                    .to(contract_address)
                                    .input(IERC20::nameCall::SELECTOR.to_vec().into());

                                match components.eth_api.call(tx_request, Some(BlockId::from(block_number)), EvmOverrides::default()).await {
                                    Ok(output) => {
                                        let output_vec = output.to_vec();
                                        let decoded = IERC20::nameCall::abi_decode_returns(&output_vec, true);
                                        match decoded {
                                            Ok(name_struct) => Some(name_struct._0),
                                            Err(_) => Some("Unknown".to_string())
                                        }
                                    },
                                    Err(_) => Some("Unknown".to_string())
                                }
                            };

                            // Extract symbol from the contract
                            let symbol = {
                                let tx_request = TransactionRequest::default()
                                    .to(contract_address)
                                    .input(IERC20::symbolCall::SELECTOR.to_vec().into());

                                match components.eth_api.call(tx_request, Some(BlockId::from(block_number)), EvmOverrides::default()).await {
                                    Ok(output) => {
                                        let output_vec = output.to_vec();
                                        let decoded = IERC20::symbolCall::abi_decode_returns(&output_vec, true);
                                        match decoded {
                                            Ok(symbol) => Some(symbol._0),
                                            Err(_) => Some("???".to_string())
                                        }
                                    },
                                    Err(_) => Some("???".to_string())
                                }
                            };

                            // Extract decimals from the contract
                            let decimals = {
                                let tx_request = TransactionRequest::default()
                                    .to(contract_address)
                                    .input(IERC20::decimalsCall::SELECTOR.to_vec().into());

                                match components.eth_api.call(tx_request, Some(BlockId::from(block_number)), EvmOverrides::default()).await {
                                    Ok(output) => {
                                        let output_vec = output.to_vec();
                                        let decoded = IERC20::decimalsCall::abi_decode_returns(&output_vec, true);
                                        match decoded {
                                            Ok(decimals) => Some(decimals._0 as i32),
                                            Err(_) => Some(18)
                                        }
                                    },
                                    Err(_) => Some(18)
                                }
                            };

                            writer
                                .write_record(record_values![
                                    block_number as i64,
                                    contract_address,
                                    name,
                                    symbol,
                                    decimals,
                                    chain_id,
                                    Utc::now(),
                                ])
                                .await?;
                        }
                    }
                }
            }
        }
    }

    Ok(())
}