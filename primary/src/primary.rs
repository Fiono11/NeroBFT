// Copyright(C) Facebook, Inc. and its affiliates.
use async_trait::async_trait;
use bytes::Bytes;
use config::{Committee, Parameters, KeyPair};
use crypto::{Digest, PublicKey, SignatureService};
use futures::sink::SinkExt as _;
use log::{error, info, warn};
use network::{MessageHandler, Receiver, Writer};
use serde::{Deserialize, Serialize};
use std::error::Error;
use ed25519_dalek::Keypair;
use store::Store;
use tokio::sync::mpsc::{channel, Sender};
use crate::core::Core;
use crate::messages::{Batch, Transaction};

//#[cfg(test)]
//#[path = "tests/worker_tests.rs"]
//pub mod worker_tests;

/// The default channel capacity for each channel of the worker.
pub const CHANNEL_CAPACITY: usize = 1_000;

pub struct Node {
    /// The keypair of this authority.
    keypair: KeyPair,
    /// The committee information.
    committee: Committee,
    /// The configuration parameters.
    parameters: Parameters,
    /// The persistent storage.
    store: Store,
}

impl Node {
    pub fn spawn(
        keypair: KeyPair,
        committee: Committee,
        parameters: Parameters,
        store: Store,
    ) {
        // Define a worker instance.
        let primary = Self {
            keypair: keypair.clone(),
            committee: committee.clone(),
            parameters,
            store,
        };

        // Spawn all primary tasks.
        primary.handle_transactions();

        // NOTE: This log entry is used to compute performance.
        info!(
            "Primary {} successfully booted on {}",
            keypair.name.clone(),
            committee
                .primary(&keypair.name)
                .expect("Our public key or worker id is not in the committee")
                .transactions
                .ip()
        );
    }

    /// Spawn all tasks responsible to handle clients transactions.
    fn handle_transactions(&self) {
        let (tx_batch_maker, rx_batch_maker) = channel(CHANNEL_CAPACITY);

        // We first receive clients' transactions from the network.
        let mut address = self
            .committee
            .primary(&self.keypair.name)
            .expect("Our public key is not in the committee")
            .transactions;
        address.set_ip("0.0.0.0".parse().unwrap());
        Receiver::spawn(
            address,
            /* handler */ TxReceiverHandler { tx_batch_maker },
        );

        // The `SignatureService` is used to require signatures on specific digests.
        let signature_service = SignatureService::new(self.keypair.secret.clone());

        // The transactions are sent to the `BatchMaker` that assembles them into batches. It then broadcasts
        // (in a reliable manner) the batches to all other workers that share the same `id` as us. Finally, it
        // gathers the 'cancel handlers' of the messages and send them to the `QuorumWaiter`.
        Core::spawn(
            self.keypair.name.clone(),
            signature_service,
            self.committee.clone(),
            self.parameters.batch_size,
            self.parameters.max_batch_delay,
            /* rx_transaction */ rx_batch_maker,
            self.committee
                .others_primaries(&self.keypair.name)
                .iter()
                .map(|(name, addresses)| (*name, addresses.transactions))
                .collect(),
        );

        info!(
            "Primary {} listening to client transactions on {}",
            self.keypair.name, address
        );
    }
}

/// Defines how the network receiver handles incoming transactions.
#[derive(Clone)]
struct TxReceiverHandler {
    tx_batch_maker: Sender<Transaction>,
}

#[async_trait]
impl MessageHandler for TxReceiverHandler {
    async fn dispatch(&self, _writer: &mut Writer, message: Bytes) -> Result<(), Box<dyn Error>> {
        // Send the transaction to the batch maker.
        match bincode::deserialize(&message) {
            Ok(tx) => self.tx_batch_maker
                .send(tx)
                .await
                .expect("Failed to send transaction"),
            Err(e) => warn!("Serialization error: {}", e),
        }

        // Give the change to schedule other tasks.
        //tokio::task::yield_now().await;
        Ok(())
    }
}