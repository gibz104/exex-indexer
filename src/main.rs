mod utils;
mod indexer;
mod datasets;
mod table_definitions;
mod db_writer;
use utils::{Config, connect_to_postgres, create_tables};
use indexer::{Indexer, EthereumBlockData};
use eyre::{Result, WrapErr};
use futures::{TryStreamExt};
use reth_ethereum::{
    cli::Cli,
    chainspec::EthereumHardforks,
    exex::{ExExContext, ExExEvent, ExExNotification},
    node::{
        api::{FullNodeComponents, NodeTypes},
        builder::rpc::RpcHandle,
        EthereumNode,
    },
    rpc::api::eth::helpers::FullEthApi,
    primitives::NodePrimitives
};
use reth_node_api::FullNodeTypes;
use reth_rpc_eth_api::EthApiTypes;
use reth_rpc_convert::RpcTypes;
use alloy_network::{Network, TransactionBuilder};
use reth_tracing::tracing::{info, warn};
use std::sync::Arc;
use tokio_postgres::Client;
use tokio::sync::oneshot;

/// An ExEx that follows ETH head and stores data in a Postgres database.
async fn indexer_exex<Node, EthApi>(
    mut ctx: ExExContext<Node>,
    rpc_handle: oneshot::Receiver<RpcHandle<Node, EthApi>>,
    client: Arc<Client>,
    indexer: Indexer<Node, EthApi>,
) -> Result<()>
where
    Node: FullNodeComponents<Types: NodeTypes<ChainSpec: EthereumHardforks>>,
    EthApi: FullEthApi + EthApiTypes,
    <EthApi as EthApiTypes>::NetworkTypes: RpcTypes + Network,
    <<EthApi as EthApiTypes>::NetworkTypes as RpcTypes>::TransactionRequest: Default + TransactionBuilder<<EthApi as EthApiTypes>::NetworkTypes>,
    Vec<(reth_primitives_traits::RecoveredBlock<alloy_consensus::Block<alloy_consensus::EthereumTxEnvelope<alloy_consensus::TxEip4844>>>, Vec<reth_primitives::Receipt>)>: FromIterator<(reth_primitives_traits::RecoveredBlock<<<<Node as FullNodeTypes>::Types as NodeTypes>::Primitives as NodePrimitives>::Block>, Vec<<<<Node as FullNodeTypes>::Types as NodeTypes>::Primitives as NodePrimitives>::Receipt>)>
{
    // Wait for the ethapi to be sent from the main function
    let rpc_handle = rpc_handle.await?;
    info!("Received rpc handle inside exex");

    // obtain the ethapi from the rpc handle
    let eth_api = rpc_handle.eth_api();
    let trace_api = rpc_handle.trace_api();

    // Process all new chain state notifications
    while let Some(notification) = ctx.notifications.try_next().await? {
        match &notification {
            ExExNotification::ChainReverted { old } => {
                let blocks: Vec<_> = old.blocks_iter().collect();
                let block_numbers: Vec<i64> = blocks.iter().map(|b| b.num_hash().number as i64).collect();

                // Revert all events for the given blocks
                if let Err(e) = indexer.revert_blocks(&block_numbers, &client).await {
                    warn!("Failed to revert blocks: {}", e);
                }

                info!(block_range = ?old.range(), "Successfully reverted block data");
            },
            ExExNotification::ChainCommitted { new } => {
                let blocks_and_receipts: Vec<EthereumBlockData> = new.blocks_and_receipts()
                    .map(|(block, receipts)| (block.clone(), receipts.clone()))
                    .collect();

                // Process the committed blocks
                if let Err(e) = indexer.process_blocks(
                    blocks_and_receipts,
                    &client,
                    ctx.provider().clone(),
                    eth_api,
                    &trace_api
                ).await {
                    warn!("Failed to process committed blocks: {}", e);
                }

                // Advance ExEx
                ctx.events.send(ExExEvent::FinishedHeight(new.tip().num_hash()))?;
            },
            ExExNotification::ChainReorged { old, new } => {
                info!(from_chain = ?old.range(), to_chain = ?new.range(), "Received reorg");
            },
        }
    }

    Ok(())
}

fn main() -> Result<()> {
    // Launch node w/ exex
    Cli::parse_args().run(|builder, _| {
        Box::pin(async move {
            // Load configs from yaml file
            let config = Config::load().wrap_err("Failed to load configuration")?;

            // Initialize postgres database client
            let client = Arc::new(connect_to_postgres().await?);
            create_tables(&client).await?;

            // Create indexer with all processors initialized internally
            let indexer = Indexer::new(config);

            let (rpc_handle_tx, rpc_handle_rx) = oneshot::channel();
            let handle = builder
                .node(EthereumNode::default())
                .install_exex("exex-indexer", async move |ctx| {
                    Ok(indexer_exex(ctx, rpc_handle_rx, client, indexer))
                })
                .launch()
                .await?;

            // Retrieve the rpc handle from the node and send it to the exex
            rpc_handle_tx
                .send(handle.node.add_ons_handle.clone())
                .expect("Failed to send ethapi to ExEx");

            handle.wait_for_node_exit().await
        })
    })
}