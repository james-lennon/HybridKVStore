extern crate rand;

use std::fs;
use std::path::Path;

use btree::{BTree, BTreeOptions};
use lsmtree::LSMTree;
use kvstore::KVStore;

use self::rand::{thread_rng, Rng};
use self::rand::seq::SliceRandom;

fn make_btree(name: &'static str) -> BTree {
    let path_name = format!("bt_test_data/{}", name);
    BTree::new(&path_name, BTreeOptions::new()).unwrap()
}

fn make_lsm(name: &'static str) -> LSMTree {
    let path_name = format!("lsm_test_data/{}", name);
    LSMTree::new(&path_name)
}

fn rand_init_store(store: &mut KVStore, size: usize) -> (Vec<i32>, Vec<i32>) {
    let mut keys: Vec<i32> = (0..size as i32).collect();
    let mut vals: Vec<i32> = (0..size as i32).collect();
    let mut rng = thread_rng();
    keys.shuffle(&mut rng);
    vals.shuffle(&mut rng);

    for i in 0 .. size {
        store.put(keys[i], vals[i]);
    }

    (keys, vals)
}

fn test_put(store: &mut KVStore) {
    let max_val = 10_000;
    let (keys, vals) = rand_init_store(store, max_val);

    let mut lookup_idx: Vec<usize> = (0..max_val as usize).collect();
    lookup_idx.shuffle(&mut thread_rng());
    for i in lookup_idx {
        assert_eq!(store.get(keys[i]), Some(vals[i]));
    }
}

fn test_update(store: &mut KVStore) {
    // let mut store = make_btree();
    assert_eq!(store.get(1), None);
    store.put(1, 42);
    assert_eq!(store.get(1), Some(42));
    store.put(1, 13);
    assert_eq!(store.get(1), Some(13));
}

fn test_delete(store: &mut KVStore) {
    let max_val = 10_000;
    let (keys, vals) = rand_init_store(store, max_val);

    let mut is_deleted = Vec::with_capacity(max_val);
    for i in 0 .. max_val {
        let delete = thread_rng().gen_bool(0.5);
        is_deleted.push(delete);

        if delete {
            store.delete(keys[i]);
            assert_eq!(store.get(keys[i]), None);
        }
    }

    for i in 0 .. max_val {
        let expected = if !is_deleted[i] { Some(vals[i]) } else { None };
        assert_eq!(store.get(keys[i]), expected);
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
        fs::remove_dir_all("test_lsm").unwrap();
    }
}