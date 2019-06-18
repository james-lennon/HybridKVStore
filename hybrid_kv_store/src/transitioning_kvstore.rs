use std::collections::BinaryHeap;
use std::sync::atomic::Ordering;

use lsmtree::{LSMTree, Run};
use btree::{BTree, BTreeOptions};
use kvstore::KVStore;
use constants::*;


static STEP_SIZE : usize = TRANSITION_STEP_N_BLOCKS * FANOUT;


#[derive(Debug)]
pub enum TransitionType {
    SortMerge,
    BatchInsert,
}


pub enum StepResult {
    Incomplete,
    Complete,
}


pub struct TransitioningKVStore {
    lsmtree: LSMTree,
    btree: BTree,
    pivot: Option<i32>,
    transition_type: TransitionType,
}

impl TransitioningKVStore {
    pub fn new(lsmtree: LSMTree, transition_type: TransitionType, btree_dir: &str) -> TransitioningKVStore {
        TransitioningKVStore {
            lsmtree: lsmtree,
            transition_type: transition_type,
            btree: BTree::new(btree_dir, BTreeOptions::new()).unwrap(),
            pivot: None,
        }
    }

    fn get_target_for_key<'a>(&'a mut self, key: i32) -> &'a mut KVStore {
        match self.pivot {
            Some(val) =>
                if key < val {
                    &mut self.btree
                } else {
                    &mut self.lsmtree
                },
            None => &mut self.lsmtree,
        }
    }


    pub fn step(&mut self) -> StepResult {
        let batch = self.lsmtree.pop_lowest_n(STEP_SIZE);
        println!("{:?}", batch);
        if batch.len() > 0 {
            self.btree.insert_batch_right(batch);
            self.btree.debug_print();
            StepResult::Incomplete
        } else {
            StepResult::Complete
        }
    }

    pub fn into_btree(self) -> BTree {
        self.btree
    }
}

impl KVStore for TransitioningKVStore {
    fn get(&mut self, key: i32) -> Option<i32> {
        self.get_target_for_key(key).get(key)
    }

    fn delete(&mut self, key: i32) {
        self.get_target_for_key(key).delete(key)
    }

    fn put(&mut self, key: i32, val: i32) {
        self.get_target_for_key(key).put(key, val)
    }

    fn scan(&mut self, low: i32, high: i32) -> Vec<i32> {
        match self.pivot {
            Some(val) => {
                if high < val {
                    self.btree.scan(low, high)
                } else if low >= val {
                    self.lsmtree.scan(low, high)
                } else {
                    let mut result = self.btree.scan(low, val);
                    result.extend_from_slice(self.lsmtree.scan(val, high).as_slice());
                    result
                }
            },
            None => self.lsmtree.scan(low, high),
        }
    }

    fn debug_lookup(&mut self, key: i32) {
        self.get_target_for_key(key).debug_lookup(key);
    }

}