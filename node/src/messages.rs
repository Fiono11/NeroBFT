use std::collections::BTreeSet;
use std::convert::TryInto;
use std::fmt;
use ed25519_dalek::{Digest as _, Sha512};
use config::Committee;
use crypto::{Digest, Hash, PublicKey, Signature, SignatureService};
use serde::{Deserialize, Serialize};
use crate::ensure;
use crate::error::{DagError, DagResult};

pub type Batch = Vec<Transaction>;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Transaction {
    pub payload: Vec<u8>,
    pub parent: Digest,
    pub votes: BTreeSet<Vote>,
}

impl Transaction {
    pub fn payload(&self) -> Vec<u8> {
        self.payload.clone()
    }

    pub fn parent(&self) -> Digest {
        self.parent.clone()
    }

    pub fn votes(&self) -> BTreeSet<Vote>{
        self.votes.clone()
    }
}

#[derive(Clone, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq)]
pub struct Vote {
    pub id: Digest,
    pub author: PublicKey,
    pub signature: Signature,
}

impl Vote {
    pub async fn new(
        id: Digest,
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
            self.id
        )
    }
}