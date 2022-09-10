use bytes::Bytes;
use config::Committee;
use crypto::{PublicKey, SignatureService};
use log::{info, warn};
use std::collections::{BTreeSet, HashMap};
//use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};
use crate::{BlockHash, Transaction};
use std::net::SocketAddr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::{Instant, sleep};
use network::SimpleSender;
use crate::elections::Election;
use crate::messages::{Batch, PrimaryMessage, VoteType};
use config::Authority;
use crate::messages::PrimaryVote;
use crypto::Digest;
use std::convert::TryFrom;
use rand::{Rng, thread_rng};
use crate::messages::VoteType::{Strong, Weak};
use crypto::Hash;
use crate::PrimaryMessage::{Decision, Vote};
use async_recursion::async_recursion;
use serde::__private::de::TagOrContentField::Tag;

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
    rx_transaction: Receiver<Vec<Transaction>>,
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
    /// Decided txs
    decided_txs: HashMap<BlockHash, HashMap<PublicKey, usize>>,
    /// Network delay
    network_delay: u64,
    counter: u64,
    rx_votes: Receiver<PrimaryVote>,
    byzantine_node: bool,
    votes: HashMap<Round, Tally>,
    current_round: usize,
    rx_decisions: Receiver<(BlockHash, PublicKey, usize)>,
}

#[derive(Debug, Clone)]
pub struct VoteDecision {
    decision: usize,
    proof: BTreeSet<PrimaryVote>,
    decision_type: VoteType,
    decided: bool,
}

impl VoteDecision {
    pub fn new(decision: usize, proof: BTreeSet<PrimaryVote>, decision_type: VoteType, decided: bool) -> Self {
        Self { decision, proof, decision_type, decided }
    }
}

pub type Round = usize;

#[derive(Debug, Clone)]
pub struct Tally {
    votes: BTreeSet<PrimaryVote>,
    voted: bool,
    weak_zeros: usize,
    weak_ones: usize,
    strong_zeros: usize,
    strong_ones: usize,
}

impl Tally {
    pub fn new(votes: BTreeSet<PrimaryVote>, voted: bool, weak_zeros: usize, weak_ones: usize, strong_zeros: usize, strong_ones: usize) -> Self {
        Self {
            votes, voted, weak_zeros, weak_ones, strong_zeros, strong_ones,
        }
    }
}

impl Core {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        signature_service: SignatureService,
        committee: Committee,
        batch_size: usize,
        max_batch_delay: u64,
        rx_transaction: Receiver<Vec<Transaction>>,
        primary_addresses: Vec<(PublicKey, SocketAddr)>,
        rx_votes: Receiver<PrimaryVote>,
        byzantine_node: bool,
        rx_decisions: Receiver<(BlockHash, PublicKey, usize)>,
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
                decided_txs: HashMap::new(),
                network_delay: 200,
                counter: 0,
                rx_votes,
                byzantine_node,
                votes: HashMap::new(),
                current_round: 0,
                rx_decisions,
            }
            .run()
            .await;
        });
    }

    /// Broadcast message
    async fn broadcast_message(&mut self, message: PrimaryMessage, addresses: Vec<SocketAddr>) {
        let serialized = bincode::serialize(&message).expect("Failed to serialize our own vote");
        let bytes = Bytes::from(serialized.clone());
        if !addresses.is_empty() {
            let delay = rand::thread_rng().gen_range(0..1000) as u64;
            sleep(Duration::from_millis(delay)).await;
            info!("Message {:?} sent to {:?} with a delay of {:?} ms", message, addresses, delay);
            let handlers = self.network.broadcast(addresses, bytes).await;
        }
    }

    /// Tally vote
    async fn tally_vote(&self, vote: PrimaryVote) -> Tally {
        let mut new_tally = Tally::new(BTreeSet::new(), false, 0, 0, 0, 0);
        match self.votes.get(&vote.round) {
            Some(tally) => {
                new_tally = Tally::new(tally.votes.clone(), tally.voted, tally.weak_zeros, tally.weak_ones, tally.strong_zeros, tally.strong_ones);
                new_tally.votes.insert(vote.clone());
                if vote.author == self.name {
                    new_tally.voted = true;
                }
                if vote.decision == 0 && vote.vote_type == VoteType::Weak {
                    new_tally.weak_zeros += 1;
                }
                else if vote.decision == 1 && vote.vote_type == VoteType::Weak {
                    new_tally.weak_ones += 1;
                }
                else if vote.decision == 0 && vote.vote_type == VoteType::Strong {
                    new_tally.strong_zeros += 1;
                }
                else if vote.decision == 1 && vote.vote_type == VoteType::Strong {
                    new_tally.strong_ones += 1;
                }
                //return new_tally;
            }
            None => {
                let mut votes = BTreeSet::new();
                votes.insert(vote.clone());
                let mut voted = false;
                if vote.author == self.name {
                    voted = true;
                }
                if vote.decision == 0 && vote.vote_type == VoteType::Weak {
                    new_tally = Tally::new(votes, voted, 1, 0, 0, 0);
                }
                else if vote.decision == 1 && vote.vote_type == VoteType::Weak {
                    new_tally = Tally::new(votes, voted, 0, 1, 0, 0);
                }
                else if vote.decision == 0 && vote.vote_type == VoteType::Strong {
                    new_tally = Tally::new(votes, voted, 0, 0, 1, 0);
                }
                else if vote.decision == 1 && vote.vote_type == VoteType::Strong {
                    new_tally = Tally::new(votes, voted, 0, 0, 0, 1);
                }
            }
        }
        info!("New tally in round {}: {:?}", vote.round, new_tally.clone());
        new_tally
    }

    async fn make_decision(&self, round: usize) -> VoteDecision {
        let mut decision = VoteDecision::new(0, BTreeSet::new(), VoteType::Weak, false);
        match self.votes.get(&round) {
            Some(tally) => {
                decision.proof = self.votes.get(&round).unwrap().votes.clone();
                if tally.strong_ones >= 3 {
                    decision.decision = 1;
                    decision.decision_type = VoteType::Strong;
                    decision.decided = true;
                    info!("Confirmed {:?} in round {}!", self.votes.get(&round).unwrap().votes.first().unwrap().tx.clone(), round);
                }
                else if tally.strong_zeros >= 3 {
                    decision.decision_type = VoteType::Strong;
                    decision.decided = true;
                    info!("Rejected {:?} in round {}!", self.votes.get(&round).unwrap().votes.first().unwrap().tx.clone(), round);
                }
                else if tally.weak_zeros + tally.strong_zeros >= 3 {
                    decision.decision_type = VoteType::Strong;
                }
                else if tally.weak_ones + tally.strong_ones >= 3 {
                    decision.decision = 1;
                    decision.decision_type = VoteType::Strong;
                }
                else if tally.weak_zeros + tally.strong_zeros < tally.weak_ones + tally.strong_ones {
                    decision.decision = 1;
                }
            }
            None => {
                let random_decision = rand::thread_rng().gen_range(0..2) as usize;
                return VoteDecision::new(random_decision, BTreeSet::new(), VoteType::Weak, false);
            }
        }
        info!("Decided in round {}: {:?}", round, decision.decided);
        decision
    }

    /// Validate vote proof
    #[async_recursion]
    async fn validate_proof(&self, vote: PrimaryVote) -> bool {
        if vote.round == 0 {
            return match vote.signature.verify(&vote.digest(), &vote.author) {
                Ok(()) => {
                    info!("Proof validation: signature of vote {} is valid!", vote.digest());
                    true
                },
                Err(e) => {
                    info!("Proof validation: signature of vote {} is not valid!", vote.digest());
                    false
                },
            }
        }
        else {
            let mut validations = vec![];
            for v in &vote.proof {
                if self.votes.get(&v.round).unwrap().votes.contains(&v) {
                    info!("Proof of vote {} is valid!", v.digest());
                    return true;
                }
                if v.round == 0 {
                    match v.signature.verify(&v.digest(), &v.author) {
                        Ok(()) => {
                            info!("Proof validation of vote {}: signature of vote {} is valid!", vote.digest(), v.digest());
                            validations.push(true);
                        },
                        Err(e) => {
                            info!("Proof validation of vote {}: signature of vote {} is not valid!", vote.digest(), v.digest());
                            return false;
                        },
                    }
                }
                else {
                    //info!("Vote: {:?}", v);
                    assert!(v.proof.len() >= 3);
                    match self.votes.get(&v.round) {
                        Some(t) => {
                            if !t.votes.contains(&v) {
                                validations.push(true);
                            }
                            else {
                                for vote in &v.proof {
                                    if self.validate_proof(vote.clone()).await {
                                        validations.push(true);
                                    }
                                    else {
                                        return false;
                                    }
                                }
                            }
                        }
                        None => {
                            for vote in &v.proof {
                                if self.validate_proof(vote.clone()).await {
                                    validations.push(true);
                                }
                                else {
                                    return false;
                                }
                            }
                        }
                    }
                }
            }
            return if validations.len() == vote.proof.len() {
                info!("Proof of vote {} is valid!", vote.digest());
                true
            } else {
                info!("Proof of vote {} is not valid!", vote.digest());
                false
            }
        }
    }

    /// Main loop receiving incoming transactions and creating batches.
    pub async fn run(&mut self) {
        let (names, mut addresses): (Vec<PublicKey>, Vec<SocketAddr>) = self.primary_addresses.iter().cloned().unzip();
        let timer = sleep(Duration::from_millis(self.max_batch_delay));
        tokio::pin!(timer);

        loop {
            tokio::select! {
                Some(decision) = self.rx_decisions.recv() => {
                    info!("Received a decision: {:#?}", decision);
                    let mut hp = HashMap::new();
                    hp.insert(decision.1, decision.2);
                    self.decided_txs.insert(decision.0, hp);
                }

                Some(transactions) = self.rx_transaction.recv() => {
                    for transaction in transactions {
                        info!("Received tx {:?}", transaction.digest().0);

                        /// Initial random vote
                        let decision = rand::thread_rng().gen_range(0..2);
                        let first_own_vote = PrimaryVote::new(transaction.digest(), decision, &self.name, &self.name, &mut self.signature_service, 0, BTreeSet::new(), VoteType::Weak).await;
                        let mut new_tally = Tally::new(BTreeSet::new(), false, 0, 0, 0, 0);

                        match self.votes.get(&0) {
                            Some(tally) => {
                                new_tally = tally.clone();
                                let decision = self.make_decision(first_own_vote.round).await;
                                if decision.decided {
                                    let mut hp = HashMap::new();
                                    hp.insert(self.name, decision.decision);
                                    self.decided_txs.insert(first_own_vote.tx.clone(), hp);
                                    self.broadcast_message(PrimaryMessage::Decision((first_own_vote.tx.clone(), self.name, decision.decision)), addresses.clone()).await;
                                }
                            }
                            None => (),
                        }
                        new_tally = self.tally_vote(first_own_vote.clone()).await;
                        self.votes.insert(first_own_vote.round, new_tally);
                        self.broadcast_message(PrimaryMessage::Vote(first_own_vote.clone()), addresses.clone()).await
                    }
                }

                Some(vote) = self.rx_votes.recv() => {
                    info!("Received a vote: {:#?}", vote);
                    if self.byzantine_node && self.decided_txs.len() <= 2 {
                        for address in &addresses {
                            let random_decision = rand::thread_rng().gen_range(0..2);
                            let byzantine_vote = PrimaryVote::new(vote.tx.clone(), random_decision, &self.name, &self.name, &mut self.signature_service, 0, BTreeSet::new(), VoteType::Weak).await;
                            self.broadcast_message(PrimaryMessage::Vote(byzantine_vote), vec![*address]).await;
                        }
                    }
                    if !self.byzantine_node && self.decided_txs.len() <= 2 {
                        let mut is_signature_valid = false;
                        let mut is_proof_valid = false;
                        let mut own_vote = PrimaryVote::new(vote.tx.clone(), 0, &self.name, &self.name, &mut self.signature_service, vote.round + 1, BTreeSet::new(), VoteType::Weak).await;
                        let mut decision = VoteDecision::new(0, BTreeSet::new(), VoteType::Weak, false);
                        let mut next_round_tally = Tally::new(BTreeSet::new(), false, 0, 0, 0, 0);
                        let mut new_tally = Tally::new(BTreeSet::new(), false, 0, 0, 0, 0);
                        match self.votes.get(&vote.round) {
                            Some(tally) => {
                                match tally.votes.iter().find(|x| x.author == vote.author) {
                                    Some(v) => info!("Vote of node {} in round {} was already tallied!", vote.author, vote.round),
                                    None => {
                                        /// Validate signature
                                        match vote.signature.verify(&vote.digest(), &vote.author) {
                                            Ok(()) => {
                                                info!("Signature of vote {} is valid!", &vote.digest());
                                                is_signature_valid = true;
                                            }
                                            Err(e) => info!("Signature of vote {} is not valid!", &vote.digest()),
                                        }
                                        is_proof_valid = self.validate_proof(vote.clone()).await;
                                    }
                                }
                            }
                            None => {
                                /// Validate signature
                                match vote.signature.verify(&vote.digest(), &vote.author) {
                                    Ok(()) => {
                                        info!("Signature of vote {} is valid!", &vote.digest());
                                        is_signature_valid = true;
                                    }
                                    Err(e) => info!("Signature of vote {} is not valid!", &vote.digest()),
                                }
                                is_proof_valid = self.validate_proof(vote.clone()).await;
                            }
                        }
                        if is_signature_valid && is_proof_valid {
                            new_tally = self.tally_vote(vote.clone()).await;
                            self.votes.insert(vote.round, new_tally.clone());
                            if (!next_round_tally.voted && new_tally.votes.len() == 3 && timer.is_elapsed()) || (!next_round_tally.voted && new_tally.votes.len() == 4) {
                                if self.decided_txs.contains_key(&vote.tx.clone()) {
                                    match self.decided_txs.get(&vote.tx.clone()).unwrap().get(&self.name) {
                                        Some(d) => {
                                            decision.decided = true;
                                            decision.decision = *d;
                                            decision.decision_type = VoteType::Strong;
                                            decision.proof = self.votes.get(&(vote.round - 1)).unwrap().votes.clone();
                                        }
                                        None => {
                                            decision = self.make_decision(vote.round).await;
                                        }
                                    }
                                }
                                else {
                                    decision = self.make_decision(vote.round).await;
                                }
                                own_vote.decision = decision.decision;
                                own_vote.proof = decision.proof;
                                own_vote.vote_type = decision.decision_type;
                                self.broadcast_message(PrimaryMessage::Vote(own_vote.clone()), addresses.clone()).await;
                                next_round_tally = self.tally_vote(own_vote.clone()).await;
                                self.votes.insert(own_vote.round, next_round_tally);
                                /// Reset timer
                                timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                            }
                            else {
                                decision = self.make_decision(vote.round).await;
                            }
                            if decision.decided {
                                let mut hp = HashMap::new();
                                hp.insert(self.name, decision.decision);
                                self.decided_txs.insert(vote.tx.clone(), hp);
                                self.broadcast_message(PrimaryMessage::Decision((vote.tx.clone(), self.name, decision.decision)), addresses.clone()).await;
                            }
                        }
                    }
                }
            };

            // Give the change to schedule other tasks.
            tokio::task::yield_now().await;
        }
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
