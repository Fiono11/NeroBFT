use std::collections::{BTreeMap, BTreeSet, HashMap};
// Copyright(C) Facebook, Inc. and its affiliates.
use bytes::Bytes;
//#[cfg(feature = "benchmark")]
use crypto::{Digest, Signature, SignatureService};
use crypto::PublicKey;
//#[cfg(feature = "benchmark")]
use ed25519_dalek::{Digest as _, Sha512};
//#[cfg(feature = "benchmark")]
use log::info;
use network::ReliableSender;
//#[cfg(feature = "benchmark")]
use std::convert::TryInto as _;
use std::net::SocketAddr;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};
use config::{Authority, Committee, Stake};
use crate::messages::{Batch, Transaction, Vote};
use std::convert::TryFrom;
use store::Store;

//#[cfg(test)]
//#[path = "tests/batch_maker_tests.rs"]
//pub mod batch_maker_tests;

/// Assemble clients transactions into batches.
pub struct Core {
    /// The public key of this primary.
    name: PublicKey,
    /// Service to sign votes.
    signature_service: SignatureService,
    /// Committee
    committee: Committee,
    /// The preferred batch size (in bytes).
    batch_size: usize,
    /// The maximum delay after which to seal the batch (in ms).
    max_batch_delay: u64,
    /// Channel to receive transactions from the network.
    rx_transaction: Receiver<Transaction>,
    /// The network addresses of the other primaries.
    primary_addresses: Vec<(PublicKey, SocketAddr)>,
    /// Holds the current batch.
    current_batch: Batch,
    /// Holds the size of the current batch (in bytes).
    current_batch_size: usize,
    /// A network sender to broadcast the batches to the other workers.
    network: ReliableSender,
    /// Election container
    elections: HashMap<Digest, (Committee, Digest)>,
}

impl Core {
    pub fn spawn(
        name: PublicKey,
        signature_service: SignatureService,
        committee: Committee,
        batch_size: usize,
        max_batch_delay: u64,
        rx_transaction: Receiver<Transaction>,
        primary_addresses: Vec<(PublicKey, SocketAddr)>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                signature_service,
                committee,
                batch_size,
                max_batch_delay,
                rx_transaction,
                primary_addresses,
                current_batch: Batch::with_capacity(batch_size * 2),
                current_batch_size: 0,
                network: ReliableSender::new(),
                elections: HashMap::new(),
            }
            .run()
            .await;
        });
    }

    /// Main loop receiving incoming transactions and creating batches.
    async fn run(&mut self) {
        let timer = sleep(Duration::from_millis(self.max_batch_delay));
        tokio::pin!(timer);

        loop {
            tokio::select! {
                // Assemble client transactions into batches of preset size.
                Some(transaction) = self.rx_transaction.recv() => {
                    // verify timestamp
                    // verify payload

                    let mut votes = transaction.votes();
                    let payload = &transaction.payload();
                    let digest = Digest::try_from(&payload[..]).unwrap();
                    let parent = &transaction.parent();

                    let mut tx = Transaction {
                        payload: vec![],
                        parent: Digest::default(),
                        votes: BTreeSet::new(),
                    };

                    // first time this transaction is seen
                    if !self.elections.contains_key(&parent) {

                        // tally votes of the transaction
                        let mut committee = self.committee.clone();
                        for vote in &votes {
                            if !committee.authorities.contains_key(&vote.author) {
                                // verify signature
                                vote.verify(&self.committee);
                                // add vote
                                committee.authorities.insert(vote.author, Authority::new(committee.stake(&vote.author), committee.primary(&vote.author).unwrap()));
                            }
                        }

                        // tally own vote
                        let own_vote = Vote::new(digest.clone(), &self.name, &mut self.signature_service);
                        votes.insert(own_vote.await);

                        self.elections.insert(parent.clone(), (committee, digest.clone()));
                    }
                    else {
                        // check fork
                        let (c, d) = self.elections.get(&parent).unwrap();
                        if d != &digest {
                            break;
                        }
                        else {
                            // check that all votes of the transaction are already tallied
                        }
                    }

                    // check quorum

                    self.current_batch_size += payload.len();
                    self.current_batch.push(transaction);
                    if self.current_batch_size >= self.batch_size {
                        self.seal().await;
                        timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                    }
                },

                // If the timer triggers, seal the batch even if it contains few transactions.
                () = &mut timer => {
                    if !self.current_batch.is_empty() {
                        self.seal().await;
                    }
                    timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                }
            }

            // Give the change to schedule other tasks.
            tokio::task::yield_now().await;
        }
    }

    /// Seal and broadcast the current batch.
    async fn seal(&mut self) {
        #[cfg(feature = "benchmark")]
        let size = self.current_batch_size;

        // Look for sample txs (they all start with 0) and gather their txs id (the next 8 bytes).
        #[cfg(feature = "benchmark")]
        let tx_ids: Vec<_> = self
            .current_batch
            .iter()
            .filter(|tx| tx[0] == 0u8 && tx.len() > 8)
            .filter_map(|tx| tx[1..9].try_into().ok())
            .collect();

        // Serialize the batch.
        self.current_batch_size = 0;
        let batch: Vec<_> = self.current_batch.drain(..).collect();
        let serialized = bincode::serialize(&batch).expect("Failed to serialize our own batch");

        #[cfg(feature = "benchmark")]
        {
            // NOTE: This is one extra hash that is only needed to print the following log entries.
            let digest = Digest(
                Sha512::digest(&serialized).as_slice()[..32]
                    .try_into()
                    .unwrap(),
            );

            for id in tx_ids {
                // NOTE: This log entry is used to compute performance.
                info!(
                    "Batch {:?} contains sample tx {}",
                    digest,
                    u64::from_be_bytes(id)
                );
            }

            // NOTE: This log entry is used to compute performance.
            info!("Batch {:?} contains {} B", digest, size);
        }

        // Broadcast the batch through the network.
        let (names, addresses): (Vec<_>, _) = self.primary_addresses.iter().cloned().unzip();
        let bytes = Bytes::from(serialized.clone());
        let handlers = self.network.broadcast(addresses, bytes).await;

        // Send the batch through the deliver channel for further processing.
        /*self.tx_message
            .send(QuorumWaiterMessage {
                batch: serialized,
                handlers: names.into_iter().zip(handlers.into_iter()).collect(),
            })
            .await
            .expect("Failed to deliver batch");*/
    }
}
