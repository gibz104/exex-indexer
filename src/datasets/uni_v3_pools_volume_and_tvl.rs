use crate::record_values;
use crate::indexer::ProcessingComponents;
use crate::db_writer::DbWriter;
use alloy::{
    sol,
    sol_types::{SolCall, SolEvent},
    primitives::{address, b256, Address, B256, U256, U160},
};
use alloy_consensus::BlockHeader;
use reth::providers::{StateProviderFactory};
use reth_node_api::FullNodeComponents;
use reth_primitives::{SealedBlockWithSenders, Receipt};
use eyre::Result;
use chrono::Utc;
use reth_rpc_eth_api::helpers::EthCall;
use alloy_rpc_types::{state::EvmOverrides, BlockId};
use alloy_rpc_types_eth::transaction::TransactionRequest;
use std::collections::HashMap;
use reth_rpc::EthApi;
use reth_rpc_eth_api::FullEthApiTypes;
use reth_rpc_eth_api::helpers::{Call, LoadPendingBlock};
use serde::Deserialize;
use reth::providers::StateProvider;

// Add Swap event definition for UniV3
sol! {
    event Swap(
        address indexed sender,
        address indexed recipient,
        int256 amount0,
        int256 amount1,
        uint160 sqrtPriceX96,
        uint128 liquidity,
        int24 tick
    );
}

#[derive(Debug, Deserialize)]
pub struct UniV3PoolPricesTVLConfig {
    pub pools: Vec<String>,
}

// Add storage slot constants and helper functions
const SLOT0: B256 = b256!("0000000000000000000000000000000000000000000000000000000000000000");
const WETH_ADDRESS: Address = address!("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2");
const USDC_ADDRESS: Address = address!("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
const USDT_ADDRESS: Address = address!("0xdAC17F958D2ee523a2206206994597C13D831ec7");

// Define the complete ERC20 interface
sol! {
    interface IERC20 {
        function decimals() external view returns (uint8);
        function balanceOf(address account) external view returns (uint256);
    }
    
    interface IUniswapV3Pool {
        function token0() external view returns (address);
        function token1() external view returns (address);
        function tickSpacing() external view returns (int24);
    }
}

#[derive(Debug, Clone)]
struct PoolInfo {
    pair_address: Address,
    token0: Address,
    token1: Address,
    decimals0: u8,
    decimals1: u8,
    sqrt_price_x96: U160,
    n_trades: i64,
    volume: f64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PoolMetadata {
    token0: Address,
    token1: Address,
    decimals0: u8,
    decimals1: u8,
    sqrt_price_x96: U160,
    tick_spacing: u32,
}

#[derive(Debug)]
struct Slot0Data {
    sqrt_price_x96: U160,
}

struct UniV3PriceOracle {
    pools: HashMap<(Address, Address), Vec<PoolInfo>>,
    weth_address: Address,
    usdc_address: Address,
    usdt_address: Address,
}

impl UniV3PriceOracle {
    fn new(weth_address: Address, usdc_address: Address, usdt_address: Address) -> Self {
        UniV3PriceOracle {
            pools: HashMap::new(),
            weth_address,
            usdc_address,
            usdt_address,
        }
    }

    fn add_pool(&mut self, pool_info: PoolInfo) {
        self.pools
            .entry((pool_info.token0, pool_info.token1))
            .or_insert_with(Vec::new)
            .push(pool_info);
    }

    fn get_pool_by_tokens(&self, token0: Address, token1: Address) -> Option<&PoolInfo> {
        self.pools.get(&(token0, token1))
            .or_else(|| self.pools.get(&(token1, token0)))
            .and_then(|pools| pools.iter().max_by_key(|pool| pool.sqrt_price_x96))
    }

    fn calculate_price_from_sqrt_price_x96(sqrt_price_x96: U160, decimals0: u8, decimals1: u8) -> f64 {
        let price_x192 = U256::from(sqrt_price_x96) * U256::from(sqrt_price_x96);
        let scale = U256::from(10).pow(U256::from(decimals0 as u64));
        let denominator = U256::from(2).pow(U256::from(192));
        let price_adjusted = (price_x192 * scale) / denominator;
        let scale1 = U256::from(10).pow(U256::from(decimals1 as u64));
        f64::from(price_adjusted) / f64::from(scale1)
    }

    fn get_token_price(&self, token: Address, preferred_base: Option<Address>) -> Option<f64> {
        // Special case for USDC/USDT pool pricing
        if let Some(base) = preferred_base {
            if (token == self.usdc_address && base == self.usdt_address) ||
                (token == self.usdt_address && base == self.usdc_address) {
                if let Some(pool) = self.get_pool_by_tokens(self.usdc_address, self.usdt_address) {
                    let price = if token == pool.token0 {
                        Self::calculate_price_from_sqrt_price_x96(pool.sqrt_price_x96, pool.decimals0, pool.decimals1)
                    } else {
                        1.0 / Self::calculate_price_from_sqrt_price_x96(pool.sqrt_price_x96, pool.decimals0, pool.decimals1)
                    };
                    if !price.is_nan() && !price.is_infinite() && price > 0.0 {
                        return Some(price);
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

        // Try direct pair with preferred base currency first
        if let Some(base) = preferred_base {
            if let Some(pool) = self.get_pool_by_tokens(token, base) {
                let price = if token == pool.token0 {
                    Self::calculate_price_from_sqrt_price_x96(pool.sqrt_price_x96, pool.decimals0, pool.decimals1)
                } else {
                    1.0 / Self::calculate_price_from_sqrt_price_x96(pool.sqrt_price_x96, pool.decimals0, pool.decimals1)
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
                Self::calculate_price_from_sqrt_price_x96(pool.sqrt_price_x96, pool.decimals0, pool.decimals1)
            } else {
                1.0 / Self::calculate_price_from_sqrt_price_x96(pool.sqrt_price_x96, pool.decimals0, pool.decimals1)
            };

            let price = token_weth_price * weth_price;
            if !price.is_nan() && !price.is_infinite() && price > 0.0 {
                return Some(price);
            }
        }

        None
    }

    fn get_weth_price(&self, preferred_base: Option<Address>) -> Option<f64> {
        let base = preferred_base.unwrap_or(self.usdc_address);

        if let Some(pool) = self.get_pool_by_tokens(self.weth_address, base) {
            let price = if self.weth_address == pool.token0 {
                Self::calculate_price_from_sqrt_price_x96(pool.sqrt_price_x96, pool.decimals0, pool.decimals1)
            } else {
                1.0 / Self::calculate_price_from_sqrt_price_x96(pool.sqrt_price_x96, pool.decimals0, pool.decimals1)
            };

            if !price.is_nan() && !price.is_infinite() && price > 0.0 {
                return Some(price);
            }
        }
        None
    }

    async fn calculate_pool_tvl<Node: FullNodeComponents>(
        &self,
        pool: &PoolInfo,
        components: &ProcessingComponents<Node>,
        block_id: BlockId,
    ) -> Option<f64>
    where
        EthApi<Node::Provider, Node::Pool, Node::Network, Node::Evm>: Call + LoadPendingBlock + FullEthApiTypes,
    {
        let preferred_base = if pool.token0 == self.usdt_address || pool.token1 == self.usdt_address {
            Some(self.usdt_address)
        } else if pool.token0 == self.usdc_address || pool.token1 == self.usdc_address {
            Some(self.usdc_address)
        } else {
            Some(self.usdc_address)
        };

        // Get token prices
        let price0 = self.get_token_price(pool.token0, preferred_base)?;
        let price1 = self.get_token_price(pool.token1, preferred_base)?;

        // Get token0 balance
        let tx_request0 = TransactionRequest::default()
            .to(pool.token0)
            .input(IERC20::balanceOfCall { account: pool.pair_address }.abi_encode().into());

        let balance0 = match components.eth_api.call(tx_request0, Some(block_id), EvmOverrides::default()).await {
            Ok(output) => {
                let output_vec = output.to_vec();
                match IERC20::balanceOfCall::abi_decode_returns(&output_vec, true) {
                    Ok(decoded) => decoded._0,
                    Err(_) => return None,
                }
            },
            Err(_) => return None,
        };

        // Get token1 balance
        let tx_request1 = TransactionRequest::default()
            .to(pool.token1)
            .input(IERC20::balanceOfCall { account: pool.pair_address }.abi_encode().into());

        let balance1 = match components.eth_api.call(tx_request1, Some(block_id), EvmOverrides::default()).await {
            Ok(output) => {
                let output_vec = output.to_vec();
                match IERC20::balanceOfCall::abi_decode_returns(&output_vec, true) {
                    Ok(decoded) => decoded._0,
                    Err(_) => return None,
                }
            },
            Err(_) => return None,
        };

        // Convert balances to f64, adjusting for decimals
        let balance0_adjusted = f64::from(balance0) / 10f64.powi(pool.decimals0 as i32);
        let balance1_adjusted = f64::from(balance1) / 10f64.powi(pool.decimals1 as i32);

        // Calculate TVL
        let value0 = balance0_adjusted * price0;
        let value1 = balance1_adjusted * price1;

        let tvl = value0 + value1;
        if tvl.is_nan() || tvl.is_infinite() || tvl <= 0.0 {
            return None;
        }

        Some(tvl)
    }
}

fn read_slot0(storage_value: B256) -> eyre::Result<Slot0Data> {
    let bytes = storage_value.as_slice();
    let sqrt_price_x96 = U160::from_be_slice(&bytes[12..32]);
    Ok(Slot0Data {
        sqrt_price_x96,
    })
}

pub async fn process_uni_v3_pools_volume_and_tvl<Node: FullNodeComponents>(
    block_data: &(SealedBlockWithSenders, Vec<Option<Receipt>>),
    components: ProcessingComponents<Node>,
    writer: &mut DbWriter,
) -> Result<()>
where
    EthApi<Node::Provider, Node::Pool, Node::Network, Node::Evm>: Call + LoadPendingBlock + FullEthApiTypes,
{
    let block_number = block_data.0.block.header.number();
    let block_hash = block_data.0.block.hash();
    let mut oracle = UniV3PriceOracle::new(WETH_ADDRESS, USDC_ADDRESS, USDT_ADDRESS);

    // Get configured pools from config
    let config = components.config.get::<UniV3PoolPricesTVLConfig>("UniV3PoolVolumeTVL")?;
    let pool_addresses: Vec<Address> = config.pools
        .iter()
        .filter_map(|addr_str| addr_str.parse::<Address>().ok())
        .collect();

    if pool_addresses.is_empty() {
        println!("Warning: No valid pool addresses configured for UniV3PoolPricesTVL");
        return Ok(());
    }

    // First, get all pool metadata and state upfront
    let mut pool_metadata: HashMap<Address, PoolMetadata> = HashMap::new();
    for pair_address in &pool_addresses {
        let state_provider = components.provider.history_by_block_hash(block_hash)?;

        // Read token0 and token1 addresses using eth_api since they're immutable variables
        let token0 = {
            let tx_request = TransactionRequest::default()
                .to(*pair_address)
                .input(IUniswapV3Pool::token0Call::SELECTOR.to_vec().into());

            match components.eth_api.call(tx_request, Some(BlockId::from(block_number)), EvmOverrides::default()).await {
                Ok(output) => {
                    let output_vec = output.to_vec();
                    let decoded = IUniswapV3Pool::token0Call::abi_decode_returns(&output_vec, true)?;
                    decoded._0
                },
                Err(e) => {
                    println!("Warning: Failed to call token0 for pool {}: {}", pair_address, e);
                    continue;
                }
            }
        };

        let token1 = {
            let tx_request = TransactionRequest::default()
                .to(*pair_address)
                .input(IUniswapV3Pool::token1Call::SELECTOR.to_vec().into());

            match components.eth_api.call(tx_request, Some(BlockId::from(block_number)), EvmOverrides::default()).await {
                Ok(output) => {
                    let output_vec = output.to_vec();
                    let decoded = IUniswapV3Pool::token1Call::abi_decode_returns(&output_vec, true)?;
                    decoded._0
                },
                Err(e) => {
                    println!("Warning: Failed to call token1 for pool {}: {}", pair_address, e);
                    continue;
                }
            }
        };

        // Get decimals for both tokens using eth_api since they're token-specific
        let decimals0 = {
            let tx_request = TransactionRequest::default()
                .to(token0)
                .input(IERC20::decimalsCall::SELECTOR.to_vec().into());

            match components.eth_api.call(tx_request, Some(BlockId::from(block_number)), EvmOverrides::default()).await {
                Ok(output) => {
                    let output_vec = output.to_vec();
                    let decoded = IERC20::decimalsCall::abi_decode_returns(&output_vec, true)?;
                    decoded._0 as u8
                },
                Err(_) => 18
            }
        };

        let decimals1 = {
            let tx_request = TransactionRequest::default()
                .to(token1)
                .input(IERC20::decimalsCall::SELECTOR.to_vec().into());

            match components.eth_api.call(tx_request, Some(BlockId::from(block_number)), EvmOverrides::default()).await {
                Ok(output) => {
                    let output_vec = output.to_vec();
                    let decoded = IERC20::decimalsCall::abi_decode_returns(&output_vec, true)?;
                    decoded._0 as u8
                },
                Err(_) => 18
            }
        };

        // Read slot0 for current price and tick from storage slot
        let slot0 = match state_provider.storage(*pair_address, SLOT0)? {
            Some(value) => match read_slot0(value.into()) {
                Ok(slot0) => slot0,
                Err(e) => {
                    println!("Warning: Failed to read slot0 for pool {}: {}", pair_address, e);
                    continue;
                }
            },
            None => {
                println!("Warning: Failed to read slot0 for pool {}", pair_address);
                continue;
            }
        };

        // Get tick spacing using eth_api
        let tick_spacing = {
            let tx_request = TransactionRequest::default()
                .to(*pair_address)
                .input(IUniswapV3Pool::tickSpacingCall::SELECTOR.to_vec().into());

            match components.eth_api.call(tx_request, Some(BlockId::from(block_number)), EvmOverrides::default()).await {
                Ok(output) => {
                    let output_vec = output.to_vec();
                    let decoded = IUniswapV3Pool::tickSpacingCall::abi_decode_returns(&output_vec, true)?;
                    let spacing = decoded._0.to_string().parse::<i32>().unwrap_or(60) as u32;
                    if spacing == 0 {
                        println!("Warning: Pool {} returned zero tick spacing, using default of 60", pair_address);
                        60
                    } else {
                        spacing
                    }
                },
                Err(e) => {
                    println!("Warning: Failed to call tickSpacing() for pool {}: {}", pair_address, e);
                    continue;
                }
            }
        };

        // Store metadata
        pool_metadata.insert(*pair_address, PoolMetadata {
            token0,
            token1,
            decimals0,
            decimals1,
            sqrt_price_x96: slot0.sqrt_price_x96,
            tick_spacing,
        });

        // Initialize oracle with this pool's data
        let pool_info = PoolInfo {
            pair_address: *pair_address,
            token0,
            token1,
            decimals0,
            decimals1,
            sqrt_price_x96: slot0.sqrt_price_x96,
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
    for (_tx, receipt) in block_data.0.transactions().into_iter().zip(receipts) {
        if let Some(receipt) = receipt {
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

                match Swap::decode_raw_log(log.topics(), &log.data.data, true) {
                    Ok(swap) => {
                        let entry = pool_trades.entry(log.address).or_insert((0, 0.0));
                        entry.0 += 1; // Increment trade count

                        // Calculate volume for token0 side (handle negative amounts)
                        let amount0 = swap.amount0.abs().to_string().parse::<f64>().unwrap_or(0.0) / 10f64.powi(metadata.decimals0 as i32);

                        // Calculate volume for token1 side (handle negative amounts)
                        let amount1 = swap.amount1.abs().to_string().parse::<f64>().unwrap_or(0.0) / 10f64.powi(metadata.decimals1 as i32);

                        // Get token prices using the oracle
                        let price0 = oracle.get_token_price(metadata.token0, None);
                        let price1 = oracle.get_token_price(metadata.token1, None);

                        // Calculate USD volumes
                        let volume0_usd = price0.map(|p| amount0 * p).unwrap_or(0.0);
                        let volume1_usd = price1.map(|p| amount1 * p).unwrap_or(0.0);

                        // Use the larger USD volume
                        entry.1 += volume0_usd.max(volume1_usd);
                    }
                    Err(e) => {
                        println!("Failed to decode swap event: {:?}", e);
                    }
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

        // Create PoolInfo struct with trade data and final state
        let (n_trades, volume) = pool_trades.get(&pair_address).copied().unwrap_or((0, 0.0));
        let pool_info = PoolInfo {
            pair_address,
            token0: metadata.token0,
            token1: metadata.token1,
            decimals0: metadata.decimals0,
            decimals1: metadata.decimals1,
            sqrt_price_x96: metadata.sqrt_price_x96,
            n_trades,
            volume,
        };

        let price = UniV3PriceOracle::calculate_price_from_sqrt_price_x96(
            pool_info.sqrt_price_x96,
            pool_info.decimals0,
            pool_info.decimals1
        );

        // Debug log for price validation
        if price.is_nan() || price.is_infinite() || price <= 0.0 {
            println!("Warning: Pool {} skipped due to invalid price: {} (sqrt_price_x96: {}, decimals0: {}, decimals1: {})",
                     pool_info.pair_address, price, pool_info.sqrt_price_x96, pool_info.decimals0, pool_info.decimals1);
            continue;
        }

        let tvl = match oracle.calculate_pool_tvl(&pool_info, &components, BlockId::from(block_number)).await {
            Some(tvl) if !tvl.is_nan() && !tvl.is_infinite() && tvl > 0.0 => tvl,
            Some(tvl) => {
                println!("Warning: Pool {} skipped due to invalid TVL: {}", pool_info.pair_address, tvl);
                continue;
            }
            None => {
                println!("Warning: Pool {} skipped due to TVL calculation failure", pool_info.pair_address);
                continue;
            }
        };
        
        writer.write_record(record_values![
            block_number as i64,
            pool_info.pair_address,
            pool_info.token0,
            pool_info.token1,
            price,
            tvl,
            pool_info.n_trades,
            pool_info.volume,
            Utc::now(),
        ]).await?;
    }

    Ok(())
}

