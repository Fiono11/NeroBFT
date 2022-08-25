use std::collections::HashMap;
use rand::prelude::SliceRandom;
use rand::{Rng, thread_rng};

const NUMBER_OF_NODES: usize = 4;
const NUMBER_OF_BYZANTINE_NODES: usize = (NUMBER_OF_NODES - 1) / 3;
const NUMBER_OF_HONEST_NODES: usize = NUMBER_OF_NODES - NUMBER_OF_BYZANTINE_NODES;
const QUORUM: usize = NUMBER_OF_BYZANTINE_NODES * 2 + 1;

fn main() {
    println!("nodes: {}", NUMBER_OF_NODES);
    println!("honest nodes: {}", NUMBER_OF_HONEST_NODES);
    println!("byzantine nodes: {}", NUMBER_OF_BYZANTINE_NODES);
    println!("quorum: {}", QUORUM);
    for _ in 0..1
    {
        let mut r1_votes: HashMap<usize, Vec<i32>> = HashMap::new();
        let mut decided: HashMap<usize, Vec<i32>> = HashMap::new();
        let mut votes = vec![];

        /// Pick the initial votes of honest nodes between 0 and 1
        for i in 0..NUMBER_OF_HONEST_NODES {
            match decided.get(&i) {
                Some(d) => {
                    votes = d.clone();
                },
                None => {
                    let v1 = vec![(0, 1), (1, 1)];
                    let vote = v1.choose_weighted(&mut rand::thread_rng(), |item| item.1).unwrap().0;
                    votes.push(vote);
                }
            }
        }

        loop {
            for i in 0..NUMBER_OF_NODES {
                if (0..NUMBER_OF_HONEST_NODES).collect::<Vec<usize>>().contains(&i) {
                    let mut votes1: Vec<i32> = vec![];
                    //let vote = rand::thread_rng().gen_range(0..2);
                    //let v1 = vec![(0, 1), (1, 10)];
                    //let vote = v1.choose_weighted(&mut rand::thread_rng(), |item| item.1).unwrap().0;
                    /// There is a probability of a vote of an honest node to be -1 because it was lost or delayed
                    for j in 0..NUMBER_OF_NODES {
                        let v = vec![(votes[i], 10), (-1, 0)];
                        let r = v.choose_weighted(&mut rand::thread_rng(), |item| item.1).unwrap().0;
                        votes1.push(r);
                    }
                    r1_votes.insert(i, votes1);
                } else {
                    let mut votes2 = vec![];
                    /// Votes of byzantine nodes are random and can be -1, 0 or 1
                    for i in 0..NUMBER_OF_NODES {
                        let vote = rand::thread_rng().gen_range(-1..2);
                        votes2.push(vote);
                    }
                    r1_votes.insert(i, votes2);
                }
            }
            println!("r1_votes: {:?}", r1_votes);
            for i in 0..NUMBER_OF_NODES {
                assert_eq!(r1_votes[&i].len(), NUMBER_OF_NODES);
            }

            let mut r2_votes = HashMap::new();
            for i in 0..NUMBER_OF_NODES {
                if (0..NUMBER_OF_HONEST_NODES).collect::<Vec<usize>>().contains(&i) {
                    let mut v = r1_votes.get(&i).unwrap().clone();
                    let mut votes3 = vec![];
                    for j in 0..r1_votes.len() {
                        //if i != j {
                        let vote = r1_votes.get(&j).unwrap();
                        votes3.push(vote[i]);
                        //}
                    }
                    //println!("votes3: {:?}", votes3);
                    let n = votes3.iter().filter(|n| **n == 1).count();
                    let l = votes3.iter().filter(|n| **n == 0).count();

                    if votes3.len() > QUORUM {
                        if n > QUORUM {
                            //v = vec![1, 1, 1];
                            for i in 0..v.len() {
                                v[i] = 1;
                            }
                        }
                        //r2_votes.insert(i, vec![1, 1, 1]);
                    } else if l > QUORUM {
                        for i in 0..v.len() {
                            v[i] = 0;
                        }
                    }
                    //v = vec![0, 0, 0];
                    //r2_votes.insert(i, vec![0, 0, 0]);
                    //}
                    //} else {
                    //for i in 0..v.len() {
                    //v[i] = -1;
                    //}
                    //v = vec![-1, -1, -1];
                    //r2_votes.insert(i, vec![-1, -1, -1]);
                    //}
                    r2_votes.insert(i, v);
                } else {
                    let mut v1 = vec![];
                    for i in 0..NUMBER_OF_NODES {
                        let vote = rand::thread_rng().gen_range(-1..2);
                        v1.push(vote);
                        //v.push(vote);
                    }
                    r2_votes.insert(i, v1);
                }
            }

            println!("r2_votes: {:?}", r2_votes);
            for i in 0..NUMBER_OF_NODES {
                assert_eq!(r2_votes[&i].len(), NUMBER_OF_NODES);
            }

            //let mut decided = vec![];
            let mut confirmed = 0;

            for i in 0..NUMBER_OF_HONEST_NODES {
                let mut votes5 = vec![];
                for j in 0..r2_votes.len() {
                    //if i != j {
                    let vote = r2_votes.get(&j).unwrap();
                    votes5.push(vote[i]);
                    //}
                }
                let n = votes5.iter().filter(|n| **n == 1).count();
                let k = votes5.iter().filter(|n| **n == 0).count();

                if !decided.contains_key(&i) {
                    if votes5.len() > QUORUM {
                        if n > 2 {
                            println!("Node {} decided {}", i, 1);
                            decided.insert(i, vec![1; NUMBER_OF_NODES]);
                            confirmed += 1;
                        } else if k > 2 {
                            println!("Node {} decided {}", i, 0);
                            decided.insert(i, vec![0; NUMBER_OF_NODES]);
                        } else {
                            println!("Node {} hasn't decided yet!", i);
                        }
                    } else {
                        println!("Node {} hasn't decided yet!", i);
                    }
                }
            }
            if decided.len() == NUMBER_OF_HONEST_NODES {
                assert!(decided.iter().all(|(a, b)| b == decided.get(&0).unwrap()));
                println!("Consensus achieved!");
                println!("{} txs confirmed!", confirmed);
                break;
            }
        }
    }
}
