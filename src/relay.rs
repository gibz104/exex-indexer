use alloy_primitives::{Address, BlockHash, U256};
use primitive_types::H384;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use std::time::Duration;
use thiserror::Error;
use url::{Url, form_urlencoded};
use reth_tracing::tracing::warn;

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

pub mod u256decimal_serde_helper {
    use std::str::FromStr;
    use alloy_primitives::U256;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(value: &U256, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&value.to_string())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<U256, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        U256::from_str(&s).map_err(serde::de::Error::custom)
    }
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
    #[error("Relay error: {0}")]
    RelayError(#[from] RelayErrorResponse),
    #[error("Too many requests")]
    TooManyRequests,
    #[error("Request timeout")]
    TimeoutError,
    #[error("Internal Error")]
    InternalError,
}

#[derive(Debug, Clone)]
pub struct RelayClient {
    url: Url,
    client: reqwest::Client,
}

impl RelayClient {
    pub fn from_known_relay(relay: KnownRelay) -> (MevBoostRelayID, Self) {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .connect_timeout(Duration::from_secs(5))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());

        (relay.name(), Self {
            url: relay.url(),
            client,
        })
    }

    async fn make_request<T: for<'de> Deserialize<'de>>(
        &self,
        url: Url,
    ) -> Result<T, RelayError> {
        const MAX_RETRIES: u32 = 3;
        const BASE_DELAY: u64 = 1;

        let mut last_error = None;

        for retry in 0..MAX_RETRIES {
            if retry > 0 {
                let delay = Duration::from_secs(BASE_DELAY * 2u64.pow(retry - 1));
                warn!(
                    "exex{{id=\"exex-indexer\"}}: Relay request retry {} of {} for {}. Waiting {} seconds before retry...",
                    retry,
                    MAX_RETRIES - 1,
                    url,
                    delay.as_secs()
                );
                tokio::time::sleep(delay).await;
            }

            let result = tokio::time::timeout(
                Duration::from_secs(15),
                self.client.get(url.clone()).send()
            ).await;

            match result {
                Ok(Ok(response)) => {
                    if response.status() == StatusCode::TOO_MANY_REQUESTS {
                        warn!(
                            "exex{{id=\"exex-indexer\"}}: Rate limit exceeded for relay request to {}",
                            url
                        );
                        last_error = Some(RelayError::TooManyRequests);
                        continue;
                    }

                    match response.json::<RelayResponse<T>>().await {
                        Ok(RelayResponse::Ok(data)) => {
                            return Ok(data);
                        }
                        Ok(RelayResponse::Error(error)) => {
                            warn!(
                                "exex{{id=\"exex-indexer\"}}: Relay request failed for {}: {}",
                                url, error
                            );
                            last_error = Some(RelayError::RelayError(error));
                        }
                        Err(e) => {
                            warn!(
                                "exex{{id=\"exex-indexer\"}}: Failed to parse relay response from {}: {}",
                                url, e
                            );
                            last_error = Some(RelayError::RequestError(e));
                        }
                    }
                }
                Ok(Err(e)) => {
                    warn!(
                        "exex{{id=\"exex-indexer\"}}: Request failed for {}: {}",
                        url, e
                    );
                    last_error = Some(RelayError::RequestError(e));
                }
                Err(_) => {
                    warn!(
                        "exex{{id=\"exex-indexer\"}}: Request timeout after 15s for {}",
                        url
                    );
                    last_error = Some(RelayError::TimeoutError);
                }
            }

            if retry == MAX_RETRIES - 1 {
                warn!(
                    "exex{{id=\"exex-indexer\"}}: Relay request failed after {} retries for {}",
                    MAX_RETRIES,
                    url
                );
            }
        }

        Err(last_error.unwrap_or(RelayError::InternalError))
    }

    pub async fn get_builder_block_received(
        &self,
        limit: u32,
        block_number: u64,
    ) -> Result<Option<Vec<BuilderBlockReceived>>, RelayError> {
        let url = {
            let mut url = self.url.clone();
            url.set_path("/relay/v1/data/bidtraces/builder_blocks_received");

            let query_string = form_urlencoded::Serializer::new(String::new())
                .append_pair("limit", &limit.to_string())
                .append_pair("block_number", &block_number.to_string())
                .finish();

            url.set_query(Some(&query_string));
            url
        };

        self.make_request::<Vec<BuilderBlockReceived>>(url)
            .await
            .map(Some)
    }

    pub async fn get_delivered_payload(
        &self,
        limit: u32,
        block_number: u64,
    ) -> Result<Option<Vec<ProposerPayloadDelivered>>, RelayError> {
        let url = {
            let mut url = self.url.clone();
            url.set_path("/relay/v1/data/bidtraces/proposer_payload_delivered");

            let query_string = form_urlencoded::Serializer::new(String::new())
                .append_pair("limit", &limit.to_string())
                .append_pair("block_number", &block_number.to_string())
                .finish();

            url.set_query(Some(&query_string));
            url
        };

        self.make_request::<Vec<ProposerPayloadDelivered>>(url)
            .await
            .map(Some)
    }
}