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
use crate::BlockHash;
use crate::core::Core;
use crate::messages::{Batch, Transaction};

//#[cfg(test)]
//#[path = "tests/worker_tests.rs"]
//pub mod worker_tests;

/// The default channel capacity for each channel of the worker.
pub const CHANNEL_CAPACITY: usize = 1_000;

pub struct Primary {
    /// The keypair of this authority.
    keypair: KeyPair,
    /// The committee information.
    committee: Committee,
    /// The configuration parameters.
    parameters: Parameters,
    /// The persistent storage.
    store: Store,
}

impl Primary {
    pub fn spawn(
        keypair: KeyPair,
        committee: Committee,
        parameters: Parameters,
        store: Store,
        tx_digest: Sender<BlockHash>,
    ) {
        // Write the parameters to the logs.
        parameters.log();

        // Define a primary instance.
        let primary = Self {
            keypair: keypair.clone(),
            committee: committee.clone(),
            parameters,
            store,
        };

        let (tx_batch_maker, rx_batch_maker) = channel(CHANNEL_CAPACITY);

        // Spawn all primary tasks.
        primary.handle_transactions(tx_batch_maker);

        // The `SignatureService` is used to require signatures on specific digests.
        let signature_service = SignatureService::new(keypair.secret.clone());

        // The transactions are sent to the `BatchMaker` that assembles them into batches. It then broadcasts
        // (in a reliable manner) the batches to all other workers that share the same `id` as us. Finally, it
        // gathers the 'cancel handlers' of the messages and send them to the `QuorumWaiter`.
        Core::spawn(
            keypair.name.clone(),
            signature_service,
            committee.clone(),
            primary.parameters.batch_size,
            primary.parameters.max_batch_delay,
            /* rx_transaction */ rx_batch_maker,
            committee
                .others_primaries(&keypair.name)
                .iter()
                .map(|(name, addresses)| (*name, addresses.transactions))
                .collect(),
            tx_digest,
        );

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
    fn handle_transactions(&self, tx_batch_maker: Sender<Transaction>) {
        // We first receive clients' transactions from the network.
        let mut address = self
            .committee
            .primary(&self.keypair.name)
            .expect("Our public key is not in the committee")
            .transactions;
        address.set_ip("127.0.0.1".parse().unwrap());
        Receiver::spawn(
            address,
            /* handler */ TxReceiverHandler { tx_batch_maker },
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
        match bincode::deserialize::<Transaction>(&message) {
            Ok(tx) => {
                info!("Received {:?}", tx.digest().0);
                self.tx_batch_maker
                    .send(tx)
                    .await
                    .expect("Failed to send transaction")
            },
            Err(e) => warn!("Serialization error: {}", e),
        }

        // Give the change to schedule other tasks.
        tokio::task::yield_now().await;
        Ok(())
    }
}