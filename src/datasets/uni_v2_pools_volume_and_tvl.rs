use crate::record_values;
use crate::indexer::{ProcessingComponents, EthereumBlockData};
use crate::db_writer::DbWriter;
use alloy::{
    sol,
    sol_types::{SolCall, SolValue, SolEvent},
    primitives::{address, b256, Address, B256, U256, U160, U128, U32, keccak256},
    primitives::aliases::U112
};
use reth::providers::{StateProviderFactory};
use reth_node_api::FullNodeComponents;
use eyre::Result;
use chrono::Utc;
use reth_rpc_eth_api::helpers::FullEthApi;
use alloy_rpc_types::{state::EvmOverrides, BlockId};
use lazy_static::lazy_static;
use std::collections::HashMap;
use alloy_network::TransactionBuilder;
use serde::Deserialize;

// Add Swap event definition
sol! {
    event Swap(
        address indexed sender,
        uint256 amount0In,
        uint256 amount1In,
        uint256 amount0Out,
        uint256 amount1Out,
        address indexed to
    );
}

#[derive(Debug, Deserialize)]
pub struct UniV2PoolPricesTVLConfig {
    pub pools: Vec<String>,
}

// const UNIV2_FACTORY_CONTRACT_ADDRESS: Address = address!("0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f");
const WETH_ADDRESS: Address = address!("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2");
const USDC_ADDRESS: Address = address!("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
const USDT_ADDRESS: Address = address!("0xdAC17F958D2ee523a2206206994597C13D831ec7");

const ALL_PAIRS_SLOT: B256 = b256!("0000000000000000000000000000000000000000000000000000000000000003");
lazy_static! {
    static ref ALL_PAIRS_SLOT_HASH: B256 = keccak256(ALL_PAIRS_SLOT.abi_encode());
}
const PAIR_TOKEN0: B256 = b256!("0000000000000000000000000000000000000000000000000000000000000006");
const PAIR_TOKEN1: B256 = b256!("0000000000000000000000000000000000000000000000000000000000000007");
const PAIR_RESERVE: B256 = b256!("0000000000000000000000000000000000000000000000000000000000000008");

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

#[derive(Debug, Clone)]
struct PoolInfo {
    pair_address: Address,
    reserve0: U112,
    reserve1: U112,
    token0: Address,
    token1: Address,
    decimals0: u8,
    decimals1: u8,
    n_trades: i64,
    volume: f64,
}

#[derive(Debug, Clone)]
struct PoolMetadata {
    token0: Address,
    token1: Address,
    decimals0: u8,
    decimals1: u8,
    reserve0: U112,
    reserve1: U112,
}

struct UniV2PriceOracle {
    pools: HashMap<(Address, Address), Vec<PoolInfo>>,
    weth_address: Address,
    usdc_address: Address,
    usdt_address: Address,
}

impl UniV2PriceOracle {
    fn new(weth_address: Address, usdc_address: Address, usdt_address: Address) -> Self {
        UniV2PriceOracle {
            pools: HashMap::new(),
            weth_address,
            usdc_address,
            usdt_address,
        }
    }

    fn add_pool(&mut self, pool_info: PoolInfo) {
        // Only store pool in one direction (token0, token1)
        self.pools
            .entry((pool_info.token0, pool_info.token1))
            .or_insert_with(Vec::new)
            .push(pool_info);
    }

    fn get_pool_by_tokens(&self, token0: Address, token1: Address) -> Option<&PoolInfo> {
        // Try both orderings when looking up pools
        self.pools.get(&(token0, token1))
            .or_else(|| self.pools.get(&(token1, token0)))
            .and_then(|pools| pools.iter().max_by_key(|pool| pool.reserve0))
    }

    fn get_token_price(&self, token: Address, preferred_base: Option<Address>) -> Option<f64> {
        // Special case for USDC/USDT pool pricing
        if let Some(base) = preferred_base {
            if (token == self.usdc_address && base == self.usdt_address) ||
                (token == self.usdt_address && base == self.usdc_address) {
                // For USDC/USDT pairs, calculate the actual exchange rate
                if let Some(pool) = self.get_pool_by_tokens(self.usdc_address, self.usdt_address) {
                    let rate = calculate_price(
                        pool.reserve0,
                        pool.reserve1,
                        pool.decimals0,
                        pool.decimals1
                    );
                    if !rate.is_nan() && !rate.is_infinite() && rate > 0.0 {
                        if token == pool.token0 {
                            return Some(1.0 / rate);
                        } else {
                            return Some(rate);
                        }
                    }
                }
            }
        }

        // If token is one of our base currencies and not in a USDC/USDT pair
        if token == self.usdc_address && preferred_base != Some(self.usdt_address) {
            return Some(1.0);
        }
        if token == self.usdt_address && preferred_base != Some(self.usdc_address) {
            return Some(1.0);
        }

        // If token is WETH, get its price directly
        if token == self.weth_address {
            return self.get_weth_price(preferred_base);
        }

        // Try direct pair with preferred base currency first
        if let Some(base) = preferred_base {
            if let Some(pool) = self.get_pool_by_tokens(token, base) {
                let price = if token == pool.token0 {
                    calculate_price(
                        pool.reserve0,
                        pool.reserve1,
                        pool.decimals0,
                        pool.decimals1
                    )
                } else {
                    1.0 / calculate_price(
                        pool.reserve0,
                        pool.reserve1,
                        pool.decimals0,
                        pool.decimals1
                    )
                };
                if !price.is_nan() && !price.is_infinite() && price > 0.0 {
                    return Some(price);
                }
            }
        }

        // Try WETH route
        if let Some(pool) = self.get_pool_by_tokens(token, self.weth_address) {
            let weth_price = self.get_weth_price(preferred_base)?;
            let token_weth_price = if token == pool.token0 {
                calculate_price(
                    pool.reserve0,
                    pool.reserve1,
                    pool.decimals0,
                    pool.decimals1
                )
            } else {
                1.0 / calculate_price(
                    pool.reserve0,
                    pool.reserve1,
                    pool.decimals0,
                    pool.decimals1
                )
            };

            let price = token_weth_price * weth_price;
            if !price.is_nan() && !price.is_infinite() && price > 0.0 {
                return Some(price);
            }
        }

        // Try USDC pair if not already tried
        if preferred_base != Some(self.usdc_address) {
            if let Some(pool) = self.get_pool_by_tokens(token, self.usdc_address) {
                let price = if token == pool.token0 {
                    calculate_price(
                        pool.reserve0,
                        pool.reserve1,
                        pool.decimals0,
                        pool.decimals1
                    )
                } else {
                    1.0 / calculate_price(
                        pool.reserve0,
                        pool.reserve1,
                        pool.decimals0,
                        pool.decimals1
                    )
                };
                if !price.is_nan() && !price.is_infinite() && price > 0.0 {
                    return Some(price);
                }
            }
        }

        // Try USDT pair if not already tried
        if preferred_base != Some(self.usdt_address) {
            if let Some(pool) = self.get_pool_by_tokens(token, self.usdt_address) {
                let price = if token == pool.token0 {
                    calculate_price(
                        pool.reserve0,
                        pool.reserve1,
                        pool.decimals0,
                        pool.decimals1
                    )
                } else {
                    1.0 / calculate_price(
                        pool.reserve0,
                        pool.reserve1,
                        pool.decimals0,
                        pool.decimals1
                    )
                };
                if !price.is_nan() && !price.is_infinite() && price > 0.0 {
                    return Some(price);
                }
            }
        }

        None
    }

    fn get_weth_price(&self, preferred_base: Option<Address>) -> Option<f64> {
        let base = preferred_base.unwrap_or(self.usdc_address);

        // Get WETH/base currency pool and find the one with highest liquidity
        if let Some(pool) = self.get_pool_by_tokens(self.weth_address, base) {
            let price = if self.weth_address == pool.token0 {
                calculate_price(
                    pool.reserve0,
                    pool.reserve1,
                    pool.decimals0,
                    pool.decimals1
                )
            } else {
                1.0 / calculate_price(
                    pool.reserve0,
                    pool.reserve1,
                    pool.decimals0,
                    pool.decimals1
                )
            };

            // Filter out invalid prices
            if !price.is_nan() && !price.is_infinite() && price > 0.0 {
                return Some(price);
            }
        }
        None
    }

    fn calculate_pool_tvl(&self, pool: &PoolInfo) -> Option<f64> {
        // Determine which base currency to use for pricing
        let preferred_base = if pool.token0 == self.usdt_address || pool.token1 == self.usdt_address {
            Some(self.usdt_address)
        } else if pool.token0 == self.usdc_address || pool.token1 == self.usdc_address {
            Some(self.usdc_address)
        } else {
            Some(self.usdc_address)
        };

        let price0 = self.get_token_price(pool.token0, preferred_base)?;
        let price1 = self.get_token_price(pool.token1, preferred_base)?;

        let value0 = f64::from(U128::from(pool.reserve0)) / 10f64.powi(pool.decimals0 as i32) * price0;
        let value1 = f64::from(U128::from(pool.reserve1)) / 10f64.powi(pool.decimals1 as i32) * price1;

        let tvl = value0 + value1;
        if tvl.is_nan() || tvl.is_infinite() || tvl <= 0.0 {
            return None;
        }
        Some(tvl)
    }
}

pub async fn process_uni_v2_pools_volume_and_tvl<Node: FullNodeComponents, EthApi: FullEthApi>(
    block_data: &EthereumBlockData,
    components: ProcessingComponents<Node, EthApi>,
    writer: &mut DbWriter,
) -> Result<()>
where
    EthApi: reth_rpc_eth_api::EthApiTypes,
    <EthApi as reth_rpc_eth_api::EthApiTypes>::NetworkTypes: reth_rpc_convert::RpcTypes + alloy_network::Network,
    <<EthApi as reth_rpc_eth_api::EthApiTypes>::NetworkTypes as reth_rpc_convert::RpcTypes>::TransactionRequest: Default + TransactionBuilder<<EthApi as reth_rpc_eth_api::EthApiTypes>::NetworkTypes>,
{
    let block_number = block_data.0.num_hash().number;
    let block_hash = block_data.0.num_hash().hash;
    let mut oracle = UniV2PriceOracle::new(WETH_ADDRESS, USDC_ADDRESS, USDT_ADDRESS);

    // Get configured pools from config
    let config = components.config.get::<UniV2PoolPricesTVLConfig>("UniV2PoolVolumeTVL")?;
    let pool_addresses: Vec<Address> = config.pools
        .iter()
        .filter_map(|addr_str| {
            if let Ok(addr) = addr_str.parse::<Address>() {
                Some(addr)
            } else {
                println!("Warning: Invalid pool address in config: {}", addr_str);
                None
            }
        })
        .collect();

    if pool_addresses.is_empty() {
        println!("Warning: No valid pool addresses configured for UniV2PoolPricesTVL");
        return Ok(());
    }

    // First, get all pool metadata and reserves upfront
    let mut pool_metadata: HashMap<Address, PoolMetadata> = HashMap::new();
    for pair_address in &pool_addresses {
        let state_provider = components.provider.history_by_block_hash(block_hash)?;

        // Read token addresses
        let token0 = match state_provider.storage(*pair_address, PAIR_TOKEN0) {
            Ok(Some(value)) => Address::from(U160::from(value)),
            _ => {
                println!("Warning: Failed to read token0 for pool {}", pair_address);
                continue;
            }
        };

        let token1 = match state_provider.storage(*pair_address, PAIR_TOKEN1) {
            Ok(Some(value)) => Address::from(U160::from(value)),
            _ => {
                println!("Warning: Failed to read token1 for pool {}", pair_address);
                continue;
            }
        };

        // Get decimals for both tokens
        let decimals0 = {
            let mut tx_req = <<EthApi as reth_rpc_eth_api::EthApiTypes>::NetworkTypes as reth_rpc_convert::RpcTypes>::TransactionRequest::default();
            tx_req = tx_req.with_to(token0).with_input(IERC20::decimalsCall::SELECTOR.to_vec());

            match components.eth_api.call(tx_req, Some(BlockId::from(block_number)), EvmOverrides::default()).await {
                Ok(output) => {
                    let output_vec = output.to_vec();
                    let decoded = IERC20::decimalsCall::abi_decode_returns(&output_vec);
                    match decoded {
                        Ok(decimals) => decimals as u8,
                        Err(_) => 18
                    }
                },
                Err(_) => 18
            }
        };

        let decimals1 = {
            let mut tx_req = <<EthApi as reth_rpc_eth_api::EthApiTypes>::NetworkTypes as reth_rpc_convert::RpcTypes>::TransactionRequest::default();
            tx_req = tx_req.with_to(token1).with_input(IERC20::decimalsCall::SELECTOR.to_vec());

            match components.eth_api.call(tx_req, Some(BlockId::from(block_number)), EvmOverrides::default()).await {
                Ok(output) => {
                    let output_vec = output.to_vec();
                    let decoded = IERC20::decimalsCall::abi_decode_returns(&output_vec);
                    match decoded {
                        Ok(decimals) => decimals as u8,
                        Err(_) => 18
                    }
                },
                Err(_) => 18
            }
        };

        // Read pool reserves
        let (_block_timestamp_last, reserve1, reserve0) = match state_provider.storage(*pair_address, PAIR_RESERVE) {
            Ok(storage_value) => match storage_value {
                Some(value) => {
                    let bytes = value.to_be_bytes_vec();
                    let block_timestamp_last = U32::from_be_slice(&bytes[0..4]);
                    let reserve1 = U112::from_be_slice(&bytes[4..18]);
                    let reserve0 = U112::from_be_slice(&bytes[18..32]);
                    (block_timestamp_last.to::<u32>(), reserve1, reserve0)
                }
                None => (0, U112::ZERO, U112::ZERO), // pair not initialized
            },
            Err(e) => {
                println!("Error reading reserves for pool {}: {}", pair_address, e);
                continue;
            },
        };

        // Store metadata including reserves
        pool_metadata.insert(*pair_address, PoolMetadata {
            token0,
            token1,
            decimals0,
            decimals1,
            reserve0,
            reserve1,
        });

        // Initialize oracle with this pool's data
        let pool_info = PoolInfo {
            pair_address: *pair_address,
            reserve0,
            reserve1,
            token0,
            token1,
            decimals0,
            decimals1,
            n_trades: 0,
            volume: 0.0,
        };
        oracle.add_pool(pool_info);

        drop(state_provider);
    }

    // Create a map to track trades and volume for each pool
    let mut pool_trades: HashMap<Address, (i64, f64)> = HashMap::new();

    // Process Swap events from logs
    let receipts = &block_data.1;
    for (_tx, receipt) in block_data.0.body().transactions.iter().zip(receipts) {
        for log in &receipt.logs {
            // Check if the log is from one of our tracked pools
            if !pool_addresses.contains(&log.address) {
                continue;
            }

            // Check if this is a Swap event
            if log.topics().get(0) != Some(&Swap::SIGNATURE_HASH) {
                continue;
            }

            // Get pool metadata
            let metadata = match pool_metadata.get(&log.address) {
                Some(m) => m,
                None => continue,
            };

            match Swap::decode_raw_log(log.topics(), &log.data.data) {
                Ok(swap) => {
                    let entry = pool_trades.entry(log.address).or_insert((0, 0.0));
                    entry.0 += 1; // Increment trade count

                    // Calculate volume for token0 side
                    let amount0_in = f64::from(U128::from(swap.amount0In)) / 10f64.powi(metadata.decimals0 as i32);
                    let amount0_out = f64::from(U128::from(swap.amount0Out)) / 10f64.powi(metadata.decimals0 as i32);
                    let volume0 = amount0_in.max(amount0_out);

                    // Calculate volume for token1 side
                    let amount1_in = f64::from(U128::from(swap.amount1In)) / 10f64.powi(metadata.decimals1 as i32);
                    let amount1_out = f64::from(U128::from(swap.amount1Out)) / 10f64.powi(metadata.decimals1 as i32);
                    let volume1 = amount1_in.max(amount1_out);

                    // Get token prices using the oracle (which now has the initial reserves)
                    let price0 = oracle.get_token_price(metadata.token0, None);
                    let price1 = oracle.get_token_price(metadata.token1, None);

                    // Calculate USD volumes
                    let volume0_usd = price0.map(|p| volume0 * p).unwrap_or(0.0);
                    let volume1_usd = price1.map(|p| volume1 * p).unwrap_or(0.0);

                    // Use the larger USD volume
                    entry.1 += volume0_usd.max(volume1_usd);
                }
                Err(e) => {
                    println!("Failed to decode swap event: {:?}", e);
                }
            }
        }
    }

    // Process each configured pool for final state
    for pair_address in pool_addresses {
        // Get pool metadata
        let metadata = match pool_metadata.get(&pair_address) {
            Some(m) => m,
            None => continue,
        };

        // Create PoolInfo struct with trade data and final reserves
        let (n_trades, volume) = pool_trades.get(&pair_address).copied().unwrap_or((0, 0.0));
        let pool_info = PoolInfo {
            pair_address,
            reserve0: metadata.reserve0,
            reserve1: metadata.reserve1,
            token0: metadata.token0,
            token1: metadata.token1,
            decimals0: metadata.decimals0,
            decimals1: metadata.decimals1,
            n_trades,
            volume,
        };

        let price = calculate_price(
            pool_info.reserve0,
            pool_info.reserve1,
            pool_info.decimals0,
            pool_info.decimals1
        );

        // Skip pools with invalid prices
        if price.is_nan() || price.is_infinite() || price <= 0.0 {
            continue;
        }

        let tvl = match oracle.calculate_pool_tvl(&pool_info) {
            Some(tvl) if !tvl.is_nan() && !tvl.is_infinite() && tvl > 0.0 => tvl,
            _ => continue  // Skip pools with invalid TVL
        };

        writer.write_record(record_values![
            block_number as i64,
            pool_info.pair_address,
            pool_info.token0,
            pool_info.token1,
            U256::from(pool_info.reserve0).to_string(),
            U256::from(pool_info.reserve1).to_string(),
            price,
            tvl,
            pool_info.n_trades,
            pool_info.volume,
            Utc::now(),
        ]).await?;
    }

    Ok(())
}

fn calculate_price(reserve0: U112, reserve1: U112, decimals0: u8, decimals1: u8) -> f64 {
    let reserve0_adjusted = f64::from(U128::from(reserve0)) * 10f64.powi((18 - decimals0) as i32);
    let reserve1_adjusted = f64::from(U128::from(reserve1)) as f64 * 10f64.powi((18 - decimals1) as i32);
    reserve1_adjusted / reserve0_adjusted
}