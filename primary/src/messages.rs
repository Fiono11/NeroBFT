use std::collections::BTreeSet;
use std::convert::{TryFrom, TryInto};
use std::fmt;
use ed25519_dalek::{Digest as _, Sha512};
use config::Committee;
use crypto::{Digest, Hash, PublicKey, Signature, SignatureService};
use serde::{Deserialize, Serialize};
use crate::ensure;
use crate::error::{DagError, DagResult};

#[derive(Debug, Clone, Eq, Hash, PartialEq, Serialize, Deserialize, Ord, PartialOrd)]
pub struct BlockHash(pub Digest);

#[derive(Debug, Clone, Eq, Hash, PartialEq, Serialize, Deserialize, Ord, PartialOrd)]
pub struct ParentHash(pub Digest);

impl AsRef<[u8]> for ParentHash {
    fn as_ref(&self) -> &[u8] {
        &self.0.as_ref()
    }
}

impl AsRef<[u8]> for BlockHash {
    fn as_ref(&self) -> &[u8] {
        &self.0.as_ref()
    }
}

pub type Batch = Vec<Transaction>;

#[derive(Debug, Hash, PartialEq, Default, Eq, Clone, Deserialize, Serialize, Ord, PartialOrd)]
pub struct Payload(pub Vec<u8>);

#[derive(Clone, Serialize, Deserialize, Debug, Ord, PartialOrd, Eq, PartialEq)]
pub struct Transaction {
    pub timestamp: u64,
    pub payload: Payload,
    pub parent: ParentHash,
    pub votes: BTreeSet<Vote>,
}

impl Transaction {
    pub fn new() -> Self {
        Self {
            timestamp: 0,
            payload: Payload(vec![]),
            parent: ParentHash(Digest::default()),
            votes: BTreeSet::new(),
        }
    }

    pub fn payload(&self) -> Payload {
        self.payload.clone()
    }

    pub fn parent(&self) -> ParentHash {
        self.parent.clone()
    }

    pub fn votes(&self) -> BTreeSet<Vote>{
        self.votes.clone()
    }

    pub fn timestamp(&self) -> u64{
        self.timestamp
    }

    pub fn digest(&self) -> BlockHash {
        let digest = Digest(
            Sha512::digest(&self.payload.0[..]).as_slice()[..32]
                .try_into()
                .unwrap(),
        );
        BlockHash(digest)
    }
}

#[derive(Clone, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq)]
pub struct Vote {
    pub id: BlockHash,
    pub author: PublicKey,
    pub signature: Signature,
}

impl Vote {
    pub async fn new(
        id: BlockHash,
        author: &PublicKey,
        signature_service: &mut SignatureService,
    ) -> Self {
        let vote = Self {
            id,
            author: *author,
            signature: Signature::default(),
        };
        let signature = signature_service.request_signature(vote.digest()).await;
        Self { signature, ..vote }
    }

    pub fn verify(&self, committee: &Committee) -> DagResult<()> {
        // Ensure the authority has voting rights.
        ensure!(
            committee.stake(&self.author) > 0,
            DagError::UnknownAuthority(self.author)
        );

        // Check the signature.
        self.signature
            .verify(&self.digest(), &self.author)
            .map_err(DagError::from)
    }
}

impl Hash for Vote {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(&self.id);
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}

impl fmt::Debug for Vote {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "{}: ({}, {})",
            self.digest(),
            self.author,
            self.id.0
        )
    }
}