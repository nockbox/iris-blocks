use iris_nockchain_types::{BlockchainConstants, TxEngineSettings};
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub struct ChainActivations {
    constants: BlockchainConstants,
    tx_engine: BTreeMap<u32, TxEngineSettings>,
}

impl ChainActivations {
    pub fn mainnet() -> Self {
        Self {
            constants: BlockchainConstants::mainnet(),
            tx_engine: [
                (0, TxEngineSettings::v0_default()),
                (39000, TxEngineSettings::v1_default()),
                (54000, TxEngineSettings::v1_bythos_default()),
            ]
            .into(),
        }
    }

    pub fn constants(&self) -> BlockchainConstants {
        self.constants
    }

    pub fn tx_engine(&self, block_height: u32) -> TxEngineSettings {
        *self
            .tx_engine
            .range(0..=block_height)
            .rev()
            .next()
            .unwrap()
            .1
    }
}
