mod disk_location;
mod btree;
mod disk_allocator;
mod kvstore;
mod bit_vector;
mod lsmtree;
mod atomic_deque;
mod constants;


use btree::{BTree, BTreeOptions};
use kvstore::KVStore;


fn main() {
    let mut store = BTree::new("bt_data", BTreeOptions::new()).unwrap();
    println!("Successfully created B-Tree");
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
    for i in 5..100 {
        assert_eq!(store.get(i), Some(i - 2));
    }
    println!("Successfully inserted single value");
}
