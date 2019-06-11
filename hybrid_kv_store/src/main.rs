mod disk_location;
mod btree;
mod disk_allocator;
mod kvstore;
mod bit_vector;
mod lsmtree;
mod atomic_deque;
mod constants;
mod tests;

use std::thread;
use std::time::Duration;

use btree::{BTree, BTreeOptions};
use lsmtree::LSMTree;
use kvstore::KVStore;


fn main() {
    let mut btree = BTree::new("bt_data", BTreeOptions::new()).unwrap();
    btree.put(1, 1);
    btree.put(2, 2);
    btree.put(3, 3);
    btree.put(4, 4);
    btree.put(5, 5);
    btree.debug_print();
    let mut lsm = btree.into_lsm_tree("lsm_data");
    // into_lsm();
    // let mut bt = BTree::new("bt_data", BTreeOptions::new()).unwrap();
    // println!("Successfully created B-Tree");
    // test_kvstore(&mut bt);
    // let mut lsm = LSMTree::new("lsm_data");
    // println!("Successfully created LSM-Tree");
    // test_kvstore(&mut lsm);
    // println!("Testing transition to B-Tree");
    // test_transition_to_btree(lsm);
}

fn test_kvstore(store: &mut KVStore) {
    store.put(1, 42);
    store.put(2, 43);
    store.put(3, 44);
    store.put(4, 45);
    store.put(-1, 34);
    store.put(-2, 37);
    store.put(-3, 36);

    println!("testing...");
    assert_eq!(store.get(1), Some(42));
    assert_eq!(store.get(2), Some(43));
    assert_eq!(store.get(3), Some(44));
    assert_eq!(store.get(4), Some(45));
    assert_eq!(store.get(-1), Some(34));

    for i in 5..100 {
        store.put(i, i - 2);
    }
    // Wait for data to get compacted
    thread::sleep(Duration::from_secs(1));
    for i in 5..100 {
        assert_eq!(store.get(i), Some(i - 2));
    }
    println!("Key-value store passed simple test.");
}

fn test_transition_to_btree(lsm: LSMTree) {
    let mut btree = lsm.into_btree();
    test_kvstore(&mut btree);
}
