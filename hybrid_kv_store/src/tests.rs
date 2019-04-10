extern crate rand;

use btree::{BTree, BTreeOptions};
use lsmtree::LSMTree;
use kvstore::KVStore;

use self::rand::thread_rng;
use self::rand::seq::SliceRandom;

fn make_btree() -> BTree {
    BTree::new("test_bt", BTreeOptions::new()).unwrap()
}

fn make_lsm() -> LSMTree {
    LSMTree::new("test_lsm")
}

fn test_put(store: &mut KVStore) {
    // let mut store = make_btree();
    let max_val = 10_000;
    let mut keys: Vec<i32> = (0..max_val).collect();
    let mut vals: Vec<i32> = (0..max_val).collect();
    let mut rng = thread_rng();
    keys.shuffle(&mut rng);
    vals.shuffle(&mut rng);

    for i in 0 .. max_val {
        store.put(keys[i as usize], vals[i as usize]);
    }

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

#[cfg(test)]
mod test_btree {
    use super::*;
    
    #[test]
    fn put() {
        test_put(&mut make_btree());
    }

    #[test]
    fn update() {
        test_update(&mut make_btree());
    }
}

#[cfg(test)]
mod test_lsm {
    use super::*;
    
    #[test]
    fn put() {
        test_put(&mut make_lsm());
    }

    #[test]
    fn update() {
        test_update(&mut make_lsm());
    }
}