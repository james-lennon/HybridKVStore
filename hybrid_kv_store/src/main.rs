mod disk_location;
mod btree;
mod disk_allocator;
mod kvstore;
mod bit_vector;
mod lsmtree;
mod atomic_deque;
mod constants;
mod tests;
mod transitioning_kvstore;
mod workload;

use std::thread;
use std::time::Duration;

use btree::{BTree, BTreeOptions};
use lsmtree::LSMTree;
use transitioning_kvstore::{TransitioningKVStore, TransitionType};
use kvstore::KVStore;

use tests::{test_delete, test_put, test_scan, rand_init_store};


fn main() {
    let mut btree = BTree::new("bt_data", BTreeOptions::new()).unwrap();
    println!("Initializing BTree");
    rand_init_store(&mut btree, 10_000);
    workload::simulate_store(&mut btree, "btree_latencies.txt");
    let mut lsm = LSMTree::new("lsm_data");
    println!("Initializing LSMTree");
    rand_init_store(&mut lsm, 10_000);
    workload::simulate_store(&mut lsm, "lsm_latencies.txt");

    workload::simulate_transition("lsm_data", "bt_data", "transition_latencies.txt");


    // let mut lsm = LSMTree::new("lsm_data");
    // lsm.put(4, 4);
    // lsm.put(1, 1);
    // lsm.put(3, 3);
    // lsm.put(15, 5);
    // lsm.put(5, 5);
    // lsm.put(13, 3);
    // lsm.put(11, 1);
    // lsm.put(12, 2);
    // lsm.put(2, 2);
    // lsm.put(14, 4);

    // let mut transitioning_store = TransitioningKVStore::new(
    //     lsm,
    //     TransitionType::SortMerge,
    //     "bt_data");
    // transitioning_store.step();
    // transitioning_store.step();
    // transitioning_store.step();
    // transitioning_store.step();

    // let mut btree = transitioning_store.into_btree();
    // // btree.debug_print();

    // for i in 0 .. 20 {
    //     println!("{:?}", btree.get(i));
    // }
    // let mut btree = BTree::new("bt_data", BTreeOptions::new()).unwrap();
    // let mut btree = BTree::new("bt_data", BTreeOptions::new()).unwrap();
    // let mut btree = BTree::new("bt_data", BTreeOptions::new()).unwrap();
    // loop {
    // }
    // btree.debug_print();

    // println!("{:?}", btree.scan(2, 5));

    // let mut lsm = btree.into_lsm_tree("lsm_data");
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
