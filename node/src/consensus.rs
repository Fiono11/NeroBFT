use std::collections::HashMap;
use rand::prelude::SliceRandom;
use rand::{Rng, thread_rng};

fn main() {
    for _ in 0..1000
    {
        let mut r1_votes: HashMap<usize, Vec<i32>> = HashMap::new();
        let mut votes = vec![];
        for i in 0..3 {
            let v1 = vec![(0, 1), (1, 1)];
            let vote = v1.choose_weighted(&mut rand::thread_rng(), |item| item.1).unwrap().0;
            votes.push(vote);
        }

        loop {
            for i in 0..4 {
                if i != 3 {
                    let mut votes1: Vec<i32> = vec![];
                    //let vote = rand::thread_rng().gen_range(0..2);
                    //let v1 = vec![(0, 1), (1, 10)];
                    //let vote = v1.choose_weighted(&mut rand::thread_rng(), |item| item.1).unwrap().0;
                    for i in 0..3 {
                        let v = vec![(votes[i], 1), (-1, 1)];
                        let r = v.choose_weighted(&mut rand::thread_rng(), |item| item.1).unwrap().0;
                        votes1.push(r);
                    }
                    r1_votes.insert(i, votes1);
                } else {
                    let mut votes2 = vec![];
                    for i in 0..3 {
                        let vote = rand::thread_rng().gen_range(-1..2);
                        votes2.push(vote);
                    }
                    r1_votes.insert(i, votes2);
                }
            }
            println!("r1_votes: {:?}", r1_votes);

            let mut r2_votes = HashMap::new();
            for i in 0..4 {
                if i != 3 {
                    let mut votes3 = vec![];
                    for j in 0..r1_votes.len() {
                        //if i != j {
                        let vote = r1_votes.get(&j).unwrap();
                        votes3.push(vote[i]);
                        //}
                    }
                    //println!("votes3: {:?}", votes3);
                    let n = votes3.iter().filter(|n| **n == 1).count();
                    let l = votes3.iter().filter(|n| **n == -1).count();
                    if l < 2 {
                        if n > 2 {
                            r2_votes.insert(i, vec![1, 1, 1]);
                        } else {
                            r2_votes.insert(i, vec![0, 0, 0]);
                        }
                    } else {
                        r2_votes.insert(i, vec![-1, -1, -1]);
                    }
                } else {
                    let mut votes4 = vec![];
                    for i in 0..3 {
                        let vote = rand::thread_rng().gen_range(-1..2);
                        votes4.push(vote);
                    }
                    r2_votes.insert(i, votes4);
                }
            }

            println!("r2_votes: {:?}", r2_votes);

            let mut decided = vec![];

            for i in 0..3 {
                let mut votes5 = vec![];
                for j in 0..r2_votes.len() {
                    //if i != j {
                    let vote = r2_votes.get(&j).unwrap();
                    votes5.push(vote[i]);
                    //}
                }
                let n = votes5.iter().filter(|n| **n == 1).count();
                let k = votes5.iter().filter(|n| **n == 0).count();
                let l = votes5.iter().filter(|n| **n == -1).count();

                if l < 2 {
                    if n > 2 {
                        println!("Node {} decided {}", i, 1);
                        decided.push(1);
                    } else if k > 2 {
                        println!("Node {} decided {}", i, 0);
                        decided.push(0);
                    } else {
                        println!("Node {} hasn't decided yet!", i);
                    }
                } else {
                    println!("Node {} hasn't decided yet!", i);
                }
            }
            if decided.len() > 2 {
                assert!(decided.iter().all(|&item| item == decided[0]));
                println!("Consensus achieved!");
                break;
            }
        }
    }
}