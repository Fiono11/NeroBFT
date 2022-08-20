use crate::error::{DagError, DagResult};
use bytes::Bytes;
use config::Committee;
use crypto::{Digest, Hash};
use crypto::{PublicKey, SignatureService};
use log::{debug, error, info, warn};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::convert::TryInto;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
//use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};
use crate::{BlockHash, ensure, Transaction};
use async_recursion::async_recursion;
use ed25519_dalek::{Digest as _, Sha512};
use std::net::SocketAddr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use serde::__private::de::TagOrContentField::Tag;
use tokio::time::{Instant, sleep};
use tracing::field::debug;
use network::{CancelHandler, ReliableSender, SimpleSender};
use crate::elections::Election;
use crate::messages::Batch;
use std::convert::TryFrom;
use std::sync::mpsc::{channel, sync_channel};
use config::Authority;
use crate::messages::Vote;
use crate::primary::CHANNEL_CAPACITY;

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
    network: SimpleSender,
    /// Election container
    elections: Election,
    /// Confirmed txs
    confirmed_txs: BTreeSet<BlockHash>,
    /// Network delay
    network_delay: u64,
    counter: u64,
    tx_digest: Sender<BlockHash>,
}

impl Core {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        signature_service: SignatureService,
        committee: Committee,
        batch_size: usize,
        max_batch_delay: u64,
        rx_transaction: Receiver<Transaction>,
        primary_addresses: Vec<(PublicKey, SocketAddr)>,
        tx_digest: Sender<BlockHash>,
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
                network: SimpleSender::new(),
                elections: HashMap::new(),
                confirmed_txs: BTreeSet::new(),
                network_delay: 0,
                counter: 0,
                tx_digest,
            }
            .run()
            .await;
        });
    }

    /// Main loop receiving incoming transactions and creating batches.
    pub async fn run(&mut self) {
        let timer = sleep(Duration::from_millis(self.max_batch_delay));
        tokio::pin!(timer);

        loop {
            tokio::select! {
                // Assemble client transactions into batches of preset size.
                Some(transaction) = self.rx_transaction.recv() => {
                    /*self.counter += 1;
                    //info!("tx {}: {:#?}", self.counter, transaction);
                    // verify timestamp
                    if now() > transaction.timestamp() + self.network_delay ||
                        now() < transaction.timestamp() - self.network_delay {
                            //continue;
                    }*/

                    // verify payload

                    let mut votes = transaction.votes();
                    let payload = &transaction.payload();
                    let digest = BlockHash(Digest::try_from(&payload.0[..]).unwrap());
                    let parent = transaction.parent();
                    let mut committee = Committee::empty();

                    let mut tx = Transaction {
                        timestamp: transaction.timestamp,
                        payload: payload.clone(),
                        parent: parent.clone(),
                        votes: votes.clone(),
                    };

                    // first time this transaction is seen
                    if !self.elections.contains_key(&parent) {

                        // tally votes of the transaction
                        for vote in &votes {
                            if !committee.authorities.contains_key(&vote.author) {
                                // verify signature
                                vote.verify(&self.committee);
                                // add vote
                                committee.authorities.insert(vote.author, Authority::new(self.committee.stake(&vote.author), self.committee.primary(&vote.author).unwrap()));
                            }
                        }

                        // tally own vote
                        let own_vote = Vote::new(digest.clone(), &self.name, &mut self.signature_service).await;

                        self.elections.insert(parent.clone(), (committee.clone(), digest.clone()));

                        tx.votes.insert(own_vote);
                    }
                    else {
                        // check fork
                        let (c, d) = self.elections.get(&parent).unwrap();
                        if d != &digest {
                            //continue;
                        }
                        else {
                            // check that all votes of the transaction are already tallied
                            for vote in &votes {
                                if !committee.authorities.contains_key(&vote.author) {
                                    // verify signature
                                    vote.verify(&self.committee);
                                    // add vote
                                    committee.authorities.insert(vote.author, Authority::new(self.committee.stake(&vote.author), self.committee.primary(&vote.author).unwrap()));
                                }
                            }
                            self.elections.insert(parent.clone(), (committee.clone(), digest.clone()));
                        }
                    }

                    // check quorum
                    if committee.total_stake() >= committee.quorum_threshold() && !self.confirmed_txs.contains(&digest) {
                        self.confirmed_txs.insert(digest.clone());
                        info!("Committed {:?} -> {:?}=", parent.clone(), digest.clone());
                        self.tx_digest.send(digest.clone()).await;
                    }

                    self.current_batch_size += payload.0.len();
                    self.current_batch.push(tx);
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
            };

            // Give the change to schedule other tasks.
            tokio::task::yield_now().await;
        }
    }

    /// Seal and broadcast the current batch.
    async fn seal(&mut self) {
        #[cfg(feature = "benchmark")]
        let size = self.current_batch_size;

        // Look for sample txs (they all start with 0) and gather their txs id (the next 8 bytes).
        /*#[cfg(feature = "benchmark")]
        let tx_ids: Vec<_> = self
            .current_batch
            .iter()
            .filter(|tx| tx.payload.0[0] == 0u8 && tx.payload.0.len() > 8)
            .filter_map(|tx| tx.payload.0[1..9].try_into().ok())
            .collect();*/

        // Serialize the batch.
        self.current_batch_size = 0;
        let batch: Vec<_> = self.current_batch.drain(..).collect();
        for tx in batch {
            let serialized = bincode::serialize(&tx).expect("Failed to serialize our own batch");
            // Broadcast the batch through the network.
            let (names, addresses): (Vec<_>, _) = self.primary_addresses.iter().cloned().unzip();
            let bytes = Bytes::from(serialized.clone());
            info!("addresses: {:#?}", addresses);
            let handlers = self.network.broadcast(addresses, bytes).await;
            info!("tx sent: {:#?}", tx);
        }

        /*#[cfg(feature = "benchmark")]
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
            //info!("Batch {:?} contains {} B", digest, size);
        }*/

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

pub fn now() -> u64 {
    let start = SystemTime::now();
    let since_the_epoch = start
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    let in_ms = since_the_epoch.as_secs() * 1000 +
        since_the_epoch.subsec_nanos() as u64 / 1_000_000;
    in_ms
}
