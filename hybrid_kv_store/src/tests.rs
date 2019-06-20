extern crate rand;

use std::fs;
use std::path::Path;

use btree::{BTree, BTreeOptions};
use lsmtree::LSMTree;
use kvstore::KVStore;

use transitioning_kvstore::{StepResult, TransitioningKVStore, TransitionType};

use self::rand::{thread_rng, Rng};
use self::rand::seq::SliceRandom;

const N_VALS: usize = 10_000;


pub fn make_btree(name: &'static str) -> BTree {
    let path_name = format!("bt_test_data/{}", name);
    BTree::new(&path_name, BTreeOptions::new()).unwrap()
}

pub fn make_lsm(name: &'static str) -> LSMTree {
    let path_name = format!("lsm_test_data/{}", name);
    LSMTree::new(&path_name)
}

pub fn rand_init_store(store: &mut KVStore, size: usize) -> (Vec<i32>, Vec<i32>) {
    let mut keys: Vec<i32> = (0..size as i32).collect();
    let mut vals: Vec<i32> = (0..size as i32).collect();
    let mut rng = thread_rng();
    keys.shuffle(&mut rng);
    vals.shuffle(&mut rng);

    for i in 0..size {
        // if i % 100 == 0 {
        //     println!("adding... {}", i);
        // }
        store.put(keys[i], vals[i]);
    }

    (keys, vals)
}

pub fn test_put(store: &mut KVStore) {
    let max_val = N_VALS;
    let (keys, vals) = rand_init_store(store, max_val);

    let mut lookup_idx: Vec<usize> = (0..max_val as usize).collect();
    lookup_idx.shuffle(&mut thread_rng());
    for i in lookup_idx {
        assert_eq!(store.get(keys[i]), Some(vals[i]));
    }
}

pub fn test_update(store: &mut KVStore) {
    // let mut store = make_btree();
    assert_eq!(store.get(1), None);
    store.put(1, 42);
    assert_eq!(store.get(1), Some(42));
    store.put(1, 13);
    assert_eq!(store.get(1), Some(13));
}

pub fn test_delete(store: &mut KVStore) {
    let max_val = N_VALS;
    let (keys, vals) = rand_init_store(store, max_val);

    let mut is_deleted = Vec::with_capacity(max_val);
    println!("phase 1");
    for i in 0..max_val {
        let delete = thread_rng().gen_bool(0.5);
        is_deleted.push(delete);

        if delete {
            store.delete(keys[i]);
            assert_eq!(store.get(keys[i]), None);
        }
    }

    println!("phase 2");
    for i in 0..max_val {
        let expected = if !is_deleted[i] { Some(vals[i]) } else { None };
        assert_eq!(store.get(keys[i]), expected);
    }
}

pub fn test_scan(store: &mut KVStore) {
    let mut keys : Vec<i32> = ((0 .. N_VALS as i32).collect());
    keys.shuffle(&mut thread_rng());

    for &i in &keys {
        store.put(i, i);
    }

    let scan_range_size : i32 = 500;
    assert!(scan_range_size < N_VALS as i32);

    for i in 0 .. (N_VALS / 10 - scan_range_size as usize) as i32 {
        let range = store.scan(i, i + scan_range_size);
        // assert_eq!(range.len(), scan_range_size as usize);
        for j in 0 .. scan_range_size {
            // assert_eq!(range[j as usize], i + j);
        }
    }
}

#[cfg(test)]
mod test_btree {
    use super::*;

    #[test]
    fn put() {
        test_put(&mut make_btree("put"));
    }

    #[test]
    fn update() {
        test_update(&mut make_btree("update"));
    }

    #[test]
    fn delete() {
        test_delete(&mut make_btree("delete"));
    }

    #[test]
    fn scan() {
        test_scan(&mut make_btree("scan"));
    }

    #[test]
    fn into_lsm() {
        let mut btree = make_btree("btree_to_lsm_1");
        println!("Initializing btree...");
        let (keys, vals) = rand_init_store(&mut btree, N_VALS);
        println!("Transitioning to LSM...");
        let mut lsm = btree.into_lsm_tree("btree_to_lsm_2");
        println!("Verifying...");
        for i in 0 .. keys.len() {
            println!("testing key {} ({} of {})", keys[i], i, keys.len());
            assert_eq!(lsm.get(keys[i]), Some(vals[i]));
        }
        println!("overwriting ...");

        // Overwrite keys and values to make sure updates work
        let (keys2, vals2) = rand_init_store(&mut lsm, N_VALS);
        for i in 0 .. keys2.len() {
            println!("overwriting {}", keys2[i]);
            assert_eq!(lsm.get(keys2[i]), Some(vals2[i]));
        }
    }
}

#[cfg(test)]
mod test_lsm {
    use super::*;

    #[test]
    fn put() {
        test_put(&mut make_lsm("put"));
    }

    #[test]
    fn update() {
        test_update(&mut make_lsm("update"));
    }

    #[test]
    fn delete() {
        test_delete(&mut make_lsm("delete"));
    }

    #[test]
    fn scan() {
        test_scan(&mut make_lsm("scan"));
    }

    #[test]
    fn into_btree() {
        let mut lsm = make_lsm("lsm_to_btree_1");
        println!("Initializing lsm...");
        let (keys, vals) = rand_init_store(&mut lsm, N_VALS);
        println!("Transitioning to B-Tree...");

        let mut transition = TransitioningKVStore::new(
            lsm,
            TransitionType::SortMerge,
            "btree_to_lsm_2");

        let n_transitions = 20;
        for i in 0..n_transitions {
            transition.step();
            transition.step();
            println!("Verifying... ({} of {})", i, n_transitions);
            for i in 0 .. keys.len() / 2 {
                // println!("testing key {} ({} of {})", keys[i], i, keys.len());
                assert_eq!(transition.get(keys[i]), Some(vals[i]));
            }
        }

        println!("Finishing transition...");
        loop {
            match transition.step() {
                StepResult::Complete => break,
                StepResult::Incomplete => (),
            }
        }

        // // Overwrite keys and values to make sure updates work
        // let (keys2, vals2) = rand_init_store(&mut lsm, N_VALS);
        // for i in 0 .. keys2.len() {
        //     println!("overwriting {}", keys2[i]);
        //     assert_eq!(lsm.get(keys2[i]), Some(vals2[i]));
        // }
    }
}
