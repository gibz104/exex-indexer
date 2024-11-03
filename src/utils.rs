use reth_rpc::{EthApi, TraceApi};
use reth_rpc_eth_types::{EthStateCache, GasPriceOracle, FeeHistoryCache, FeeHistoryCacheConfig};
use reth_tasks::pool::{BlockingTaskGuard, BlockingTaskPool};
use reth_node_api::FullNodeComponents;
use reth::chainspec::ChainSpecProvider;
use reth::chainspec::EthChainSpec;
use reth::primitives::{EthereumHardforks};
use std::sync::Arc;
use reth::builder::NodeTypes;
use reth_exex::ExExContext;
use reth_rpc_server_types::constants::{
    DEFAULT_ETH_PROOF_WINDOW,
    DEFAULT_MAX_SIMULATE_BLOCKS,
    DEFAULT_PROOF_PERMITS,
};

/// Sanitize a byte slice to ensure it's valid UTF-8.
pub(crate) fn sanitize_bytes(input: &[u8]) -> String {
    // First, remove all null bytes
    let without_nulls: Vec<u8> = input.iter().filter(|&&b| b != 0).cloned().collect();

    // Then, convert to a string, replacing any invalid UTF-8 sequences
    String::from_utf8_lossy(&without_nulls).into_owned()
}

/// Create a trace API instance with all the required components and trait bounds
pub(crate) fn create_trace_api<Node>(
    ctx: &ExExContext<Node>,
) -> Arc<TraceApi<Node::Provider, EthApi<Node::Provider, Node::Pool, Node::Network, Node::Evm>>>
where
    Node: FullNodeComponents,
    Node::Types: NodeTypes,
    <Node::Types as NodeTypes>::ChainSpec: EthereumHardforks,
    Node::Provider: ChainSpecProvider,
{
    let provider = ctx.components.provider().clone();
    let cache = EthStateCache::spawn(
        provider.clone(),
        Default::default(),
        ctx.components.evm_config().clone(),
    );

    let fee_history_cache = FeeHistoryCache::new(cache.clone(), FeeHistoryCacheConfig::default());

    let gas_oracle = GasPriceOracle::new(
        provider.clone(),
        Default::default(),
        cache.clone()
    );

    let eth_api = EthApi::new(
        provider.clone(),
        ctx.pool().clone(),
        ctx.network().clone(),
        cache.clone(),
        gas_oracle,
        provider.chain_spec().max_gas_limit(),
        DEFAULT_MAX_SIMULATE_BLOCKS,
        DEFAULT_ETH_PROOF_WINDOW,
        BlockingTaskPool::build().expect("failed to build tracing pool"),
        fee_history_cache,
        ctx.components.evm_config().clone(),
        DEFAULT_PROOF_PERMITS,
    );

    Arc::new(TraceApi::new(
        provider,
        eth_api,
        BlockingTaskGuard::new(10),
    ))
}