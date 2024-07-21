use alloy_primitives::{Address, BlockHash, U256};
use futures::{stream::FuturesUnordered, StreamExt};
use governor::{DefaultDirectRateLimiter, Quota, RateLimiter};
use primitive_types::H384;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use std::{
    collections::HashMap,
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use thiserror::Error;
use tracing::trace;
use url::Url;

/// These structs are from Flashbot's rbuilder repo and used to fetch
/// data from known mev-boost relays. https://github.com/flashbots/rbuilder


pub type MevBoostRelayID = String;

pub const RELAYS: [KnownRelay; 9] = [
    KnownRelay::Flashbots,
    KnownRelay::BloxrouteMaxProfit,
    KnownRelay::BloxrouteRegulated,
    KnownRelay::Eden,
    KnownRelay::SecureRpc,
    KnownRelay::Ultrasound,
    KnownRelay::Agnostic,
    KnownRelay::Aestus,
    KnownRelay::Wenmerge,
];

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum KnownRelay {
    Flashbots,
    BloxrouteMaxProfit,
    BloxrouteEthical,
    BloxrouteRegulated,
    Eden,
    SecureRpc,
    Ultrasound,
    Agnostic,
    Aestus,
    Wenmerge,
}

impl KnownRelay {
    pub fn url(&self) -> Url {
        Url::parse(match self {
            KnownRelay::Flashbots => "https://0xac6e77dfe25ecd6110b8e780608cce0dab71fdd5ebea22a16c0205200f2f8e2e3ad3b71d3499c54ad14d6c21b41a37ae@boost-relay.flashbots.net",
            KnownRelay::BloxrouteMaxProfit => "https://0x8b5d2e73e2a3a55c6c87b8b6eb92e0149a125c852751db1422fa951e42a09b82c142c3ea98d0d9930b056a3bc9896b8f@bloxroute.max-profit.blxrbdn.com",
            KnownRelay::BloxrouteEthical => "https://0xad0a8bb54565c2211cee576363f3a347089d2f07cf72679d16911d740262694cadb62d7fd7483f27afd714ca0f1b9118@bloxroute.ethical.blxrbdn.com",
            KnownRelay::BloxrouteRegulated => "https://0xb0b07cd0abef743db4260b0ed50619cf6ad4d82064cb4fbec9d3ec530f7c5e6793d9f286c4e082c0244ffb9f2658fe88@bloxroute.regulated.blxrbdn.com",
            KnownRelay::Eden => "https://0xb3ee7afcf27f1f1259ac1787876318c6584ee353097a50ed84f51a1f21a323b3736f271a895c7ce918c038e4265918be@relay.edennetwork.io",
            KnownRelay::SecureRpc => "https://0x98650451ba02064f7b000f5768cf0cf4d4e492317d82871bdc87ef841a0743f69f0f1eea11168503240ac35d101c9135@mainnet-relay.securerpc.com",
            KnownRelay::Ultrasound => "https://0xa1559ace749633b997cb3fdacffb890aeebdb0f5a3b6aaa7eeeaf1a38af0a8fe88b9e4b1f61f236d2e64d95733327a62@relay.ultrasound.money",
            KnownRelay::Agnostic => "https://0xa7ab7a996c8584251c8f925da3170bdfd6ebc75d50f5ddc4050a6fdc77f2a3b5fce2cc750d0865e05d7228af97d69561@agnostic-relay.net",
            KnownRelay::Aestus => "https://0xa15b52576bcbf1072f4a011c0f99f9fb6c66f3e1ff321f11f461d15e31b1cb359caa092c71bbded0bae5b5ea401aab7e@aestus.live",
            KnownRelay::Wenmerge => "https://0x8c7d33605ecef85403f8b7289c8058f440cbb6bf72b055dfe2f3e2c6695b6a1ea5a9cd0eb3a7982927a463feb4c3dae2@relay.wenmerge.com",
        }).unwrap()
    }

    pub fn name(&self) -> String {
        match self {
            KnownRelay::Flashbots => "flashbots",
            KnownRelay::BloxrouteMaxProfit => "bloxroute_max_profit",
            KnownRelay::BloxrouteEthical => "bloxroute_ethical",
            KnownRelay::BloxrouteRegulated => "bloxroute_regulated",
            KnownRelay::Eden => "eden",
            KnownRelay::SecureRpc => "secure_rpc",
            KnownRelay::Ultrasound => "ultrasound",
            KnownRelay::Agnostic => "agnostic",
            KnownRelay::Aestus => "aestus",
            KnownRelay::Wenmerge => "wenmerge",
        }
            .to_string()
    }
}

impl FromStr for KnownRelay {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "flashbots" => Ok(KnownRelay::Flashbots),
            "bloxroute_max_profit" => Ok(KnownRelay::BloxrouteMaxProfit),
            "bloxroute_ethical" => Ok(KnownRelay::BloxrouteEthical),
            "bloxroute_regulated" => Ok(KnownRelay::BloxrouteRegulated),
            "eden" => Ok(KnownRelay::Eden),
            "secure_rpc" => Ok(KnownRelay::SecureRpc),
            "ultrasound" => Ok(KnownRelay::Ultrasound),
            "agnostic" => Ok(KnownRelay::Agnostic),
            "aestus" => Ok(KnownRelay::Aestus),
            "wenmerge" => Ok(KnownRelay::Wenmerge),
            _ => Err(()),
        }
    }
}

/// de/serializes U256 as decimal value (U256 serde default is hexa). Needed to interact with some JSONs (eg:ProposerPayloadDelivered in relay provider API)
pub mod u256decimal_serde_helper {
    use std::str::FromStr;

    use alloy_primitives::U256;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(value: &U256, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        //fmt::Display for U256 uses decimal
        serializer.serialize_str(&value.to_string())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<U256, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        //from_str is robust, can take decimal or other prefixed (eg:"0x" hexa) formats.
        U256::from_str(&s).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug)]
pub struct PayloadDeliveredResult {
    pub delivered: Vec<(MevBoostRelayID, BuilderBlockReceived)>,
    pub relay_errors: HashMap<MevBoostRelayID, DeliveredPayloadBidTraceErr>,
}

impl PayloadDeliveredResult {
    pub fn best_bid(&self) -> Option<BuilderBlockReceived> {
        self.delivered.first().map(|(_, p)| p.clone())
    }

    pub fn best_relay(&self) -> Option<MevBoostRelayID> {
        self.delivered.first().map(|(r, _)| r.clone())
    }
}

#[derive(Debug, Clone)]
pub struct PayloadDeliveredFetcher {
    relays: HashMap<MevBoostRelayID, RelayClient>,
}

impl Default for PayloadDeliveredFetcher {
    fn default() -> Self {
        let relays = RELAYS
            .iter()
            .map(|r| {
                (
                    r.name(),
                    RelayClient::from_known_relay(r.clone()),
                )
            })
            .collect();

        Self { relays }
    }
}

impl PayloadDeliveredFetcher {
    pub fn from_relays(relays: &[MevBoostRelay]) -> Self {
        let relays = relays.iter().map(|relay| (relay.id.clone(), relay.client.clone())).collect();
        Self { relays }
    }

    pub async fn get_payload_delivered(&self, block_number: u64) -> PayloadDeliveredResult {
        let mut relay_errors = HashMap::new();

        let delivered_payloads = self
            .relays
            .clone()
            .into_iter()
            .map(|(id, relay)| async move {
                (
                    id,
                    self.get_delivered_payload_bid_trace(relay, block_number).await,
                )
            })
            .collect::<FuturesUnordered<_>>()
            .collect::<Vec<_>>()
            .await;

        let mut relays_delivered_payload = Vec::new();
        for (relay, res) in delivered_payloads {
            match res {
                Ok(payload) => {
                    relays_delivered_payload.push((relay, payload));
                }
                Err(DeliveredPayloadBidTraceErr::NoDeliveredBidTrace) => {
                    trace!(
                        relay = relay,
                        "No payload bid trace for block {}",
                        block_number
                    );
                }
                Err(err) => {
                    trace!(
                        relay = relay,
                        "Relay error while delivering payload: {:?}",
                        err
                    );
                    relay_errors.insert(relay, err);
                }
            }
        }

        relays_delivered_payload.sort_by_key(|(_, p)| p.timestamp_ms);
        PayloadDeliveredResult {
            delivered: relays_delivered_payload,
            relay_errors,
        }
    }

    async fn get_delivered_payload_bid_trace(
        &self,
        relay: RelayClient,
        block_number: u64,
    ) -> Result<BuilderBlockReceived, DeliveredPayloadBidTraceErr> {
        let payload = relay
            .proposer_payload_delivered_block_number(block_number)
            .await?
            .ok_or(DeliveredPayloadBidTraceErr::NoDeliveredBidTrace)?;

        relay
            .builder_block_received_block_hash(payload.block_hash)
            .await?
            .ok_or(DeliveredPayloadBidTraceErr::NoDeliveredBidTrace)
    }
}

#[derive(thiserror::Error, Debug)]
pub enum DeliveredPayloadBidTraceErr {
    #[error("RelayError: {0}")]
    RelayError(#[from] RelayError),
    #[error("No delivered bid trace")]
    NoDeliveredBidTrace,
}

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct BuilderBlockReceived {
    #[serde_as(as = "DisplayFromStr")]
    pub slot: u64,
    pub parent_hash: BlockHash,
    pub block_hash: BlockHash,
    pub builder_pubkey: H384,
    pub proposer_pubkey: H384,
    pub proposer_fee_recipient: Address,
    #[serde_as(as = "DisplayFromStr")]
    pub gas_limit: u64,
    #[serde_as(as = "DisplayFromStr")]
    pub gas_used: u64,
    #[serde(with = "u256decimal_serde_helper")]
    pub value: U256,
    #[serde_as(as = "DisplayFromStr")]
    pub num_tx: u64,
    #[serde_as(as = "DisplayFromStr")]
    pub block_number: u64,
    #[serde_as(as = "DisplayFromStr")]
    pub timestamp: u64,
    #[serde_as(as = "DisplayFromStr")]
    pub timestamp_ms: u64,
    #[serde(default)]
    pub optimistic_submission: bool,
}

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ProposerPayloadDelivered {
    #[serde_as(as = "DisplayFromStr")]
    pub slot: u64,
    pub parent_hash: BlockHash,
    pub block_hash: BlockHash,
    pub builder_pubkey: H384,
    pub proposer_pubkey: H384,
    pub proposer_fee_recipient: Address,
    #[serde_as(as = "DisplayFromStr")]
    pub gas_limit: u64,
    #[serde_as(as = "DisplayFromStr")]
    pub gas_used: u64,
    #[serde(with = "u256decimal_serde_helper")]
    pub value: U256,
    #[serde_as(as = "DisplayFromStr")]
    pub block_number: u64,
    #[serde_as(as = "DisplayFromStr")]
    pub num_tx: u64,
}

#[derive(Error, Debug, Clone, Serialize, Deserialize)]
pub struct RelayErrorResponse {
    code: Option<u64>,
    message: String,
}

impl std::fmt::Display for RelayErrorResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Relay error: (code: {}, message: {})",
            self.code.unwrap_or_default(),
            self.message
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RelayResponse<T> {
    Ok(T),
    Error(RelayErrorResponse),
}

#[derive(Error, Debug)]
pub enum RelayError {
    #[error("Request error: {0}")]
    RequestError(#[from] reqwest::Error),
    #[error("Header error")]
    InvalidHeader,
    #[error("Relay error: {0}")]
    RelayError(#[from] RelayErrorResponse),
    #[error("Unknown relay response, status: {0}, body: {1}")]
    UnknownRelayError(StatusCode, String),
    #[error("Too many requests")]
    TooManyRequests,
    #[error("Connection error")]
    ConnectionError,
    #[error("Internal Error")]
    InternalError,
}

#[derive(Debug, Clone)]
pub struct MevBoostRelay {
    pub id: MevBoostRelayID,
    pub client: RelayClient,
    pub priority: usize,
    pub use_ssz_for_submit: bool,
    pub use_gzip_for_submit: bool,
    pub optimistic: bool,
    pub submission_rate_limiter: Option<Arc<DefaultDirectRateLimiter>>,
}

impl MevBoostRelay {
    pub fn try_from_name_or_url(
        id: &str,
        name_or_url: &str,
        priority: usize,
        use_ssz_for_submit: bool,
        use_gzip_for_submit: bool,
        optimistic: bool,
        authorization_header: Option<String>,
        builder_id_header: Option<String>,
        api_token_header: Option<String>,
        interval_between_submissions: Option<Duration>,
    ) -> eyre::Result<Self> {
        let client = RelayClient::from_url(
            name_or_url.parse()?,
            authorization_header,
            builder_id_header,
            api_token_header,
        );

        let submission_rate_limiter = interval_between_submissions.map(|d| {
            Arc::new(RateLimiter::direct(
                Quota::with_period(d).expect("Rate limiter time period"),
            ))
        });

        Ok(MevBoostRelay {
            id: id.to_string(),
            client,
            priority,
            use_ssz_for_submit,
            use_gzip_for_submit,
            optimistic,
            submission_rate_limiter,
        })
    }
}

#[derive(Debug, Clone)]
pub struct RelayClient {
    url: Url,
    client: reqwest::Client,
    authorization_header: Option<String>,
    builder_id_header: Option<String>,
    api_token_header: Option<String>,
}

impl RelayClient {
    pub fn from_url(
        url: Url,
        authorization_header: Option<String>,
        builder_id_header: Option<String>,
        api_token_header: Option<String>,
    ) -> Self {
        Self {
            url,
            client: reqwest::Client::new(),
            authorization_header,
            builder_id_header,
            api_token_header,
        }
    }

    pub fn from_known_relay(relay: KnownRelay) -> Self {
        Self::from_url(relay.url(), None, None, None)
    }

    async fn get_one_delivered_payload(
        &self,
        query: &str,
    ) -> Result<Option<ProposerPayloadDelivered>, RelayError> {
        let url = {
            let mut url = self.url.clone();
            url.set_path("/relay/v1/data/bidtraces/proposer_payload_delivered");
            url.set_query(Some(query));
            url
        };

        let payloads = reqwest::get(url)
            .await?
            .json::<RelayResponse<Vec<ProposerPayloadDelivered>>>()
            .await?;

        match payloads {
            RelayResponse::Ok(payloads) => Ok(payloads.into_iter().next()),
            RelayResponse::Error(error) => Err(RelayError::RelayError(error)),
        }
    }

    pub async fn proposer_payload_delivered_slot(
        &self,
        slot: u64,
    ) -> Result<Option<ProposerPayloadDelivered>, RelayError> {
        self.get_one_delivered_payload(&format!("slot={}", slot)).await
    }

    pub async fn proposer_payload_delivered_block_number(
        &self,
        block_number: u64,
    ) -> Result<Option<ProposerPayloadDelivered>, RelayError> {
        self.get_one_delivered_payload(&format!("block_number={}", block_number))
            .await
    }

    pub async fn proposer_payload_delivered_block_hash(
        &self,
        block_hash: BlockHash,
    ) -> Result<Option<ProposerPayloadDelivered>, RelayError> {
        self.get_one_delivered_payload(&format!("block_hash={:?}", block_hash))
            .await
    }

    async fn get_one_builder_block_received(
        &self,
        query: &str,
    ) -> Result<Option<BuilderBlockReceived>, RelayError> {
        let url = {
            let mut url = self.url.clone();
            url.set_path("/relay/v1/data/bidtraces/builder_blocks_received");
            url.set_query(Some(query));
            url
        };

        let payloads = reqwest::get(url)
            .await?
            .json::<RelayResponse<Vec<BuilderBlockReceived>>>()
            .await?;

        match payloads {
            RelayResponse::Ok(payloads) => Ok(payloads.into_iter().next()),
            RelayResponse::Error(error) => Err(RelayError::RelayError(error)),
        }
    }

    pub async fn builder_block_received_block_hash(
        &self,
        block_hash: BlockHash,
    ) -> Result<Option<BuilderBlockReceived>, RelayError> {
        self.get_one_builder_block_received(&format!("block_hash={:?}", block_hash))
            .await
    }

}