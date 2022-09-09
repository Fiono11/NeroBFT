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
    async fn broadcast_message(&mut self, message: PrimaryMessage) {
        let serialized = bincode::serialize(&message).expect("Failed to serialize our own vote");
        let (names, mut addresses): (Vec<PublicKey>, Vec<SocketAddr>) = self.primary_addresses.iter().cloned().unzip();
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
                return new_tally;
            }
            None => {
                let mut votes = BTreeSet::new();
                votes.insert(vote.clone());
                let mut voted = false;
                if vote.author == self.name {
                    voted = true;
                }
                if vote.decision == 0 && vote.vote_type == VoteType::Weak {
                    return Tally::new(votes, voted, 1, 0, 0, 0);
                }
                else if vote.decision == 1 && vote.vote_type == VoteType::Weak {
                    return Tally::new(votes, voted, 0, 1, 0, 0);
                }
                else if vote.decision == 0 && vote.vote_type == VoteType::Strong {
                    return Tally::new(votes, voted, 0, 0, 1, 0);
                }
                else if vote.decision == 1 && vote.vote_type == VoteType::Strong {
                    return Tally::new(votes, voted, 0, 0, 0, 1);
                }
            }
        }
        new_tally
    }

    async fn make_decision(&self, round: usize) -> VoteDecision {
        let mut decision = VoteDecision::new(0, BTreeSet::new(), VoteType::Weak, false);
        match self.votes.get(&round) {
            Some(tally) => {
                decision.proof = self.votes.get(&(round - 1)).unwrap().votes.clone();
                if tally.strong_ones >= 3 {
                    decision.decision = 1;
                    decision.decision_type = VoteType::Strong;
                    decision.decided = true;
                }
                else if tally.strong_zeros >= 3 {
                    decision.decision_type = VoteType::Strong;
                    decision.decided = true;
                }
                else if tally.weak_zeros >= 3 {
                    decision.decision_type = VoteType::Strong;
                }
                else if tally.weak_ones >= 3 {
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
        decision
    }

    /// Validate vote proof
    #[async_recursion]
    async fn validate_proof(&self, v: PrimaryVote) -> bool {
        let mut validations = vec![];
        if v.round == 0 {
            match v.signature.verify(&v.digest(), &v.author) {
                Ok(()) => {
                    info!("Signature of vote {} of proof is valid!", v.digest());
                    validations.push(true);
                },
                Err(e) => {
                    info!("Signature of vote {} of proof is not valid!", v.digest());
                    return false;
                },
            }
        }
        else {
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
        if validations.len() == v.proof.len() {
            return true;
        }
        else {
            return false;
        }
    }

    /// Main loop receiving incoming transactions and creating batches.
    pub async fn run(&mut self) {
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

                Some(vote) = self.rx_votes.recv() => {
                    info!("Received a vote: {:#?}", vote);
                    // send different votes to different nodes
                    // validate votes
                    // put round timer
                    // flip asynchronicity
                    if self.byzantine_node && self.decided_txs.len() <= 2 {
                        let random_decision = rand::thread_rng().gen_range(0..2);
                        let byzantine_vote = PrimaryVote::new(vote.tx.clone(), random_decision, &self.name, &self.name, &mut self.signature_service, 0, BTreeSet::new(), VoteType::Weak).await;
                        self.broadcast_message(PrimaryMessage::Vote(byzantine_vote)).await;
                    }
                    if !self.byzantine_node && self.decided_txs.len() <= 2 {
                        let mut is_signature_valid = false;
                        let mut is_proof_valid = false;
                        let mut vote = PrimaryVote::new(vote.tx.clone(), 0, &self.name, &self.name, &mut self.signature_service, vote.round, BTreeSet::new(), VoteType::Weak).await;
                        let mut decision = VoteDecision::new(0, BTreeSet::new(), VoteType::Weak, false);
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

                                        if is_signature_valid && is_proof_valid {
                                            self.tally_vote(vote.clone()).await;
                                            if !tally.voted {
                                                match self.decided_txs.get(&vote.tx.clone()) {
                                                    Some(d) => {
                                                        decision.decided = true;
                                                        decision.decision = *d.get(&self.name).unwrap();
                                                        decision.decision_type = VoteType::Strong;
                                                        decision.proof = self.votes.get(&(vote.round - 1)).unwrap().votes.clone();
                                                    }
                                                    None => {
                                                        decision = self.make_decision(vote.round).await;
                                                        //if decision.decided {
                                                            //let mut hp = HashMap::new();
                                                            //hp.insert(self.name, decision.decision);
                                                            //self.decided_txs.insert(vote.tx.clone(), hp);
                                                            //self.broadcast_message(PrimaryMessage::Decision((vote.tx.clone(), self.name, decision.decision))).await;
                                                        //}
                                                    }
                                                }
                                            }
                                        }

                                        /// Vote next round
                                        if tally.votes.len() >= 3 {

                                        }
                                    }
                                }
                            }
                            None => {
                                /// Validate signature
                                match vote.signature.verify(&vote.digest(), &vote.author) {
                                    Ok(()) => info!("Signature of vote {} is valid!", &vote.digest()),
                                    Err(e) => info!("Signature of vote {} is not valid!", &vote.digest()),
                                }
                                /// Validate proof
                                self.validate_proof(vote.clone()).await;
                            }
                        }
                        vote.decision = decision.decision;
                        vote.proof = decision.proof;
                        vote.vote_type = decision.decision_type;
                        self.broadcast_message(PrimaryMessage::Vote(vote.clone())).await;
                        let new_tally = self.tally_vote(vote.clone()).await;
                        self.votes.insert(vote.round, new_tally);
                        let new_decision = self.make_decision(vote.round).await;
                        if new_decision.decided {
                            let mut hp = HashMap::new();
                            hp.insert(self.name, decision.decision);
                            self.decided_txs.insert(vote.tx.clone(), hp);
                            self.broadcast_message(PrimaryMessage::Decision((vote.tx.clone(), self.name, new_decision.decision))).await;
                        }
                    }
                    /*else if !self.byzantine_node {
                        //if validations.len() == vote.proof.len() {
                            match self.votes.get(&vote.round) {
                                Some(tally) => {
                                    let mut weak_zeros = tally.weak_zeros;
                                    let mut weak_ones = tally.weak_ones;
                                    let mut strong_zeros = tally.strong_zeros;
                                    let mut strong_ones = tally.strong_ones;
                                    let author = vote.author;
                                    match tally.votes.iter().find(|x| x.author == author) {
                                        Some(vote) => info!("Vote of node {} in round {} was already tallied!", author, vote.round),
                                        None => {
                                            /// Validate signature
                                            match vote.signature.verify(&vote.digest(), &vote.author) {
                                                Ok(()) => info!("Signature of vote {} is valid!", &vote.digest()),
                                                Err(e) => info!("Signature of vote {} is not valid!", &vote.digest()),
                                            }

                                            let mut validations = vec![];

                                            /// Validate proof
                                            if vote.round != 0 {
                                                for v in &vote.proof {
                                                    let mut validated = false;
                                                    for i in (0..v.round).rev() {
                                                        if self.validate_proof(i, v.clone()).await {
                                                            validated = true;
                                                            validations.push(validated);
                                                            info!("{:?} proof of vote {} is valid!", v, &vote.digest());
                                                            break;
                                                        }
                                                    }
                                                    if !validated {
                                                        info!("{:?} proof of vote {} is not valid!", v, &vote.digest());
                                                    }
                                                }
                                            }
                                            if vote.decision == 0 && vote.vote_type == VoteType::Weak {
                                            weak_zeros += 1;
                                            }
                                            if vote.decision == 1 && vote.vote_type == VoteType::Weak {
                                                weak_ones += 1;
                                            }
                                            if vote.decision == 0 && vote.vote_type == VoteType::Strong {
                                                strong_zeros += 1;
                                            }
                                            if vote.decision == 1 && vote.vote_type == VoteType::Strong {
                                                strong_ones += 1;
                                            }
                                            let mut votes_of_the_round_of_the_vote_received = tally.votes.clone();
                                            votes_of_the_round_of_the_vote_received.insert(vote.clone());
                                            self.votes.insert(vote.round, Tally::new(votes_of_the_round_of_the_vote_received.clone(), tally.voted, weak_zeros, weak_ones, strong_zeros, strong_ones));
                                            info!("Tx {:?} has {} weak zeros, {} weak ones, {} strong zeros and {} strong ones in round {}", vote.tx, weak_zeros, weak_ones, strong_zeros, strong_ones, vote.round);
                                        }
                                    }
                                    if !self.decided_txs.contains(&(vote.tx.clone(), self.name, 0)) && !self.decided_txs.contains(&(vote.tx.clone(), self.name, 1)) {
                                        if strong_ones >= 3 {
                                            self.decided_txs.insert((vote.tx.clone(), self.name, 1));
                                            info!("Confirmed {:?} in round {}!", vote.tx.clone(), vote.round);
                                            let serialized = bincode::serialize(&PrimaryMessage::Decision((vote.tx.clone(), self.name, 1))).expect("Failed to serialize our own vote");
                                            let (names, mut addresses): (Vec<PublicKey>, Vec<SocketAddr>) = self.primary_addresses.iter().cloned().unzip();
                                            let bytes = Bytes::from(serialized.clone());
                                            if !addresses.is_empty() {
                                                let delay = rand::thread_rng().gen_range(0..1000);
                                                sleep(Duration::from_millis(delay)).await;
                                                info!("Decision {:?} sent to {:?} with a delay of {:?} ms", 1, addresses, delay);
                                                let handlers = self.network.broadcast(addresses, bytes).await;
                                            }
                                        }
                                        if strong_zeros >= 3 {
                                            self.decided_txs.insert((vote.tx.clone(), self.name, 0));
                                            info!("Rejected {:?} in round {}!", vote.tx.clone(), vote.round);
                                            let serialized = bincode::serialize(&PrimaryMessage::Decision((vote.tx.clone(), self.name, 0))).expect("Failed to serialize our own vote");
                                            let (names, mut addresses): (Vec<PublicKey>, Vec<SocketAddr>) = self.primary_addresses.iter().cloned().unzip();
                                            let bytes = Bytes::from(serialized.clone());
                                            if !addresses.is_empty() {
                                                let delay = rand::thread_rng().gen_range(0..1000);
                                                sleep(Duration::from_millis(delay)).await;
                                                info!("Decision {:?} sent to {:?} with a delay of {:?} ms", 0, addresses, delay);
                                                let handlers = self.network.broadcast(addresses, bytes).await;
                                            }
                                        }
                                    }
                                }
                                None => {
                                    let mut weak_zeros = 0;
                                    let mut weak_ones = 0;
                                    let mut strong_zeros = 0;
                                    let mut strong_ones = 0;
                                    if vote.decision == 0 && vote.vote_type == VoteType::Weak {
                                        weak_zeros += 1;
                                    }
                                    if vote.decision == 1 && vote.vote_type == VoteType::Weak {
                                        weak_ones += 1;
                                    }
                                    if vote.decision == 0 && vote.vote_type == VoteType::Strong {
                                        strong_zeros += 1;
                                    }
                                    if vote.decision == 1 && vote.vote_type == VoteType::Strong {
                                        strong_ones += 1;
                                    }
                                    let mut votes = BTreeSet::new();
                                    votes.insert(vote.clone());
                                    self.votes.insert(vote.round, Tally::new(votes, false, weak_zeros, weak_ones, strong_zeros, strong_ones));
                                }
                            }
                        //}

                        let mut decision = 0;
                        let mut vote_type = VoteType::Weak;
                        let tally = self.votes.get(&vote.round).unwrap();
                        let (votes_of_the_round_of_the_vote_received, voted_previous, mut weak_zeros, mut weak_ones, mut strong_zeros, mut strong_ones) =
                            (tally.votes.clone(), tally.voted, tally.weak_zeros, tally.weak_ones, tally.strong_zeros, tally.strong_ones);
                        let mut voted = false;
                        match self.votes.get(&(vote.round + 1)) {
                            Some(tally) => {
                                if tally.voted == true {
                                    voted = tally.voted;
                                }
                            }
                            None => ()
                        }
                        let round = vote.round;

                        /// If 2f votes of round i have been received, send the vote of round i+1 to 2f
                        /// nodes with justification of 2f + 1 votes of round i (including own vote)
                        if votes_of_the_round_of_the_vote_received.len() >= 3 {
                            let tx = vote.tx.clone();
                            if !self.decided_txs.contains(&(tx.clone(), self.name, 0)) && !self.decided_txs.contains(&(tx.clone(), self.name, 1)) {
                                if weak_zeros > weak_ones {
                                    decision = 0;
                                    if weak_zeros > 2 {
                                        vote_type = Strong;
                                    }
                                }
                                else {
                                    decision = 1;
                                    if weak_ones > 2 {
                                        vote_type = Strong;
                                    }
                                }
                            }
                            if self.decided_txs.contains(&(tx.clone(), self.name, 1)) {
                                decision = 1;
                                vote_type = Strong;
                            }
                            if self.decided_txs.contains(&(tx.clone(), self.name, 0)) {
                                decision = 0;
                                vote_type = Strong;
                            }

                            if self.decided_txs.len() <= 2 {
                                if !voted {
                                    let mut new_votes_of_the_current_round: BTreeSet<Vote> = BTreeSet::new();

                                    for vote in votes_of_the_round_of_the_vote_received {
                                        let mut new_vote = vote.clone();
                                        //new_vote.proof = BTreeSet::new();
                                        if new_vote.round == round {
                                            new_votes_of_the_current_round.insert(new_vote);
                                        }
                                    }

                                    let mut own_vote: Vote = Vote::new(tx.clone(), decision, &self.name, &self.name,
                                        &mut self.signature_service, round + 1, new_votes_of_the_current_round.clone(), vote_type).await;

                                    match self.votes.get(&(round + 1)) {
                                        Some(tally) => {
                                            let mut weak_zeros = tally.weak_zeros;
                                            let mut weak_ones = tally.weak_ones;
                                            let mut strong_zeros = tally.strong_zeros;
                                            let mut strong_ones = tally.strong_ones;
                                            if own_vote.decision == 0 && own_vote.vote_type == VoteType::Weak {
                                                weak_zeros += 1;
                                            }
                                            if own_vote.decision == 1 && own_vote.vote_type == VoteType::Weak {
                                                weak_ones += 1;
                                            }
                                            if own_vote.decision == 0 && own_vote.vote_type == VoteType::Strong {
                                                strong_zeros += 1;
                                            }
                                            if own_vote.decision == 1 && own_vote.vote_type == VoteType::Strong {
                                                strong_ones += 1;
                                            }
                                            let mut votes = tally.votes.clone();
                                            votes.insert(own_vote.clone());
                                            self.votes.insert(round + 1, Tally::new(votes, true, weak_zeros, weak_ones, strong_zeros, strong_ones));
                                            info!("Voted in round {}", round + 1);
                                            if !self.decided_txs.contains(&(vote.tx.clone(), self.name, 0)) && !self.decided_txs.contains(&(vote.tx.clone(), self.name, 1)) {
                                                if strong_ones >= 3 {
                                                    self.decided_txs.insert((tx.clone(), self.name, 1));
                                                    info!("Confirmed {:?} in round {}!", tx.clone(), vote.round);
                                                    let serialized = bincode::serialize(&PrimaryMessage::Decision((tx.clone(), self.name, decision))).expect("Failed to serialize our own vote");
                                                    let (names, mut addresses): (Vec<PublicKey>, Vec<SocketAddr>) = self.primary_addresses.iter().cloned().unzip();
                                                    let bytes = Bytes::from(serialized.clone());
                                                    if !addresses.is_empty() {
                                                        let delay = rand::thread_rng().gen_range(0..1000);
                                                        sleep(Duration::from_millis(delay)).await;
                                                        info!("Decision {:?} sent to {:?} with a delay of {:?} ms", decision, addresses, delay);
                                                        let handlers = self.network.broadcast(addresses, bytes).await;
                                                    }
                                                }
                                                if strong_zeros >= 3 {
                                                    self.decided_txs.insert((tx.clone(), self.name, 0));
                                                    info!("Rejected {:?} in round {}!", tx.clone(), vote.round);
                                                    let serialized = bincode::serialize(&PrimaryMessage::Decision((tx.clone(), self.name, decision))).expect("Failed to serialize our own vote");
                                                    let (names, mut addresses): (Vec<PublicKey>, Vec<SocketAddr>) = self.primary_addresses.iter().cloned().unzip();
                                                    let bytes = Bytes::from(serialized.clone());
                                                    if !addresses.is_empty() {
                                                        let delay = rand::thread_rng().gen_range(0..1000);
                                                        sleep(Duration::from_millis(delay)).await;
                                                        info!("Decision {:?} sent to {:?} with a delay of {:?} ms", decision, addresses, delay);
                                                        let handlers = self.network.broadcast(addresses, bytes).await;
                                                    }
                                                }
                                            }
                                        }
                                        None => {
                                            let mut weak_zeros = 0;
                                            let mut weak_ones = 0;
                                            let mut strong_zeros = 0;
                                            let mut strong_ones = 0;
                                            if own_vote.decision == 0 && own_vote.vote_type == VoteType::Weak {
                                                weak_zeros += 1;
                                            }
                                            if own_vote.decision == 1 && own_vote.vote_type == VoteType::Weak {
                                                weak_ones += 1;
                                            }
                                            if own_vote.decision == 0 && own_vote.vote_type == VoteType::Strong {
                                                strong_zeros += 1;
                                            }
                                            if own_vote.decision == 1 && own_vote.vote_type == VoteType::Strong {
                                                strong_ones += 1;
                                            }
                                            let mut votes = BTreeSet::new();
                                            votes.insert(own_vote.clone());
                                            self.votes.insert(round + 1, Tally::new(votes, true, weak_zeros, weak_ones, strong_zeros, strong_ones));
                                            info!("Voted in round {}", round + 1);
                                        }
                                    }
                                    //info!("votes2: {:?}", self.votes);
                                    let serialized = bincode::serialize(&PrimaryMessage::Vote(own_vote.clone())).expect("Failed to serialize our own vote");

                                    /// Send the initial vote to 2f random nodes
                                    let (names, mut addresses): (Vec<PublicKey>, Vec<SocketAddr>) = self.primary_addresses.iter().cloned().unzip();
                                    /*let len = addresses.len();
                                    for i in 0..len - 2 * 1 {
                                        let random = thread_rng().gen_range(0..addresses.len());
                                        addresses.remove(random);
                                    }*/
                                    let bytes = Bytes::from(serialized.clone());
                                    if !addresses.is_empty() {
                                        let delay = rand::thread_rng().gen_range(0..1000);
                                        sleep(Duration::from_millis(delay)).await;
                                        info!("Vote {:?} sent to {:?} with a delay of {:?} ms", own_vote, addresses, delay);
                                        let handlers = self.network.broadcast(addresses, bytes).await;
                                    }
                                //self.current_round += 1;
                                }
                            }
                        }
                    }*/
                },

                // Assemble client transactions into batches of preset size.
                Some(transactions) = self.rx_transaction.recv() => {
                    for transaction in transactions {
                        info!("Received tx {:?}", transaction.digest().0);

                        /// Initial random vote
                        let decision = rand::thread_rng().gen_range(0..2);
                        let first_own_vote = PrimaryVote::new(transaction.digest(), decision, &self.name, &self.name, &mut self.signature_service, 0, BTreeSet::new(), VoteType::Weak).await;
                        match self.votes.get(&0) {
                            Some(tally) => {
                                let mut weak_zeros = tally.weak_zeros;
                                let mut weak_ones = tally.weak_ones;
                                let mut strong_zeros = tally.strong_zeros;
                                let mut strong_ones = tally.strong_ones;
                                let mut votes = tally.votes.clone();
                                votes.insert(first_own_vote.clone());
                                if decision == 0 && first_own_vote.vote_type == VoteType::Weak {
                                    self.votes.insert(0, Tally::new(votes.clone(), true, weak_zeros + 1, weak_ones, strong_zeros, strong_ones));
                                }
                                if decision == 1 && first_own_vote.vote_type == VoteType::Weak {
                                    self.votes.insert(0, Tally::new(votes.clone(), true, weak_zeros, weak_ones + 1, strong_zeros, strong_ones));
                                }
                                if decision == 0 && first_own_vote.vote_type == VoteType::Strong {
                                    self.votes.insert(0, Tally::new(votes.clone(), true, weak_zeros, weak_ones, strong_zeros + 1, strong_ones));
                                }
                                if decision == 1 && first_own_vote.vote_type == VoteType::Strong {
                                    self.votes.insert(0, Tally::new(votes.clone(), true, weak_zeros, weak_ones, strong_zeros, strong_ones + 1));
                                }
                            }
                            None => {
                                let mut votes = BTreeSet::new();
                                votes.insert(first_own_vote.clone());
                                if decision == 0 {
                                    self.votes.insert(0, Tally::new(votes.clone(), true, 1, 0, 0, 0));
                                }
                                if decision == 1 {
                                    self.votes.insert(0, Tally::new(votes.clone(), true, 0, 1, 0, 0));
                                }
                            }
                        }
                        let serialized = bincode::serialize(&PrimaryMessage::Vote(first_own_vote.clone())).expect("Failed to serialize our own vote");

                        // Send the initial vote to all nodes
                        /// Send the initial vote to 2f random nodes
                        let (names, mut addresses): (Vec<PublicKey>, Vec<SocketAddr>) = self.primary_addresses.iter().cloned().unzip();
                        /*let len = addresses.len();
                        for i in 0..len - 2 * 1 {
                            let random = thread_rng().gen_range(0..addresses.len());
                            addresses.remove(random);
                        }*/
                        let bytes = Bytes::from(serialized.clone());
                        if !addresses.is_empty() {
                            let delay = rand::thread_rng().gen_range(0..1000);
                            sleep(Duration::from_millis(delay)).await;
                            info!("Vote {:?} sent to {:?} with a delay of {:?} ms", first_own_vote, addresses, delay);
                            let handlers = self.network.broadcast(addresses, bytes).await;
                        }

                        // verify timestamp
                        /*if now() > transaction.timestamp() + self.network_delay ||
                            now() < transaction.timestamp() - self.network_delay {
                            warn!("Tx {:?} has invalid timestamp: now -> {:?} vs t -> {:?}", transaction.digest().0, now(), transaction.timestamp());
                            break;
                        }

                        // verify payload

                        let votes = transaction.votes();
                        let payload = &transaction.payload();
                        let digest = transaction.digest();
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
                                    vote.verify(&self.committee).unwrap();
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
                                warn!("Fork detected!");
                                break;
                            }
                            else {
                                // check that all votes of the transaction are already tallied
                                for vote in &votes {
                                    if !committee.authorities.contains_key(&vote.author) {
                                        // verify signature
                                        vote.verify(&self.committee).unwrap();
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
                            info!("Committed {:?} -> {:?}", parent.clone(), digest.0.clone());
                            //self.tx_digest.send(digest.clone()).await;
                        }

                        self.current_batch_size += payload.0.len();
                        self.current_batch.push(tx);
                        if self.current_batch_size >= self.batch_size {
                            self.seal().await;
                            timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                        }*/
                    }
                },

                // If the timer triggers, seal the batch even if it contains few transactions.
                /*() = &mut timer => {
                    if !self.current_batch.is_empty() {
                        self.seal().await;
                    }
                    timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                }*/
            };

            // Give the change to schedule other tasks.
            tokio::task::yield_now().await;
        }
    }

    /// Seal and broadcast the current batch.
    async fn seal(&mut self) {
        #[cfg(feature = "benchmark")]
        let size = self.current_batch_size;

        // Serialize the batch.
        self.current_batch_size = 0;
        let batch: Vec<_> = self.current_batch.drain(..).collect();
        //for tx in batch {
            let serialized = bincode::serialize(&batch).expect("Failed to serialize our own batch");
            // Broadcast the batch through the network.
            let (names, addresses): (Vec<PublicKey>, Vec<SocketAddr>) = self.primary_addresses.iter().cloned().unzip();
            /*for vote in &tx.votes {
                if let Some(p) = names.iter().position(|&r| r == vote.author) {
                    addresses.remove(p);
                }
            }*/
            let bytes = Bytes::from(serialized.clone());
            if !addresses.is_empty() {
                info!("batch sent to {:?}: {:?}", addresses, batch);
                let handlers = self.network.broadcast(addresses, bytes).await;
            }

            info!("Batch {:?} contains {} B", batch[0].digest().0, serialized.len());
        //}
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
