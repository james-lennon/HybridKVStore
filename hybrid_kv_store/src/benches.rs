#[macro_use]
extern crate bencher;

mod disk_location;
mod btree;
mod disk_allocator;
mod kvstore;
mod bit_vector;
mod lsmtree;
mod atomic_deque;
mod constants;
mod tests;

use std::{thread, time};

use tests::{make_btree, make_lsm, rand_init_store};
use btree::{BTree, BTreeOptions};
use lsmtree::LSMTree;
use kvstore::KVStore;

use self::bencher::Bencher;


const N_VALS : usize = 100;

fn bench_bt_get(b: &mut Bencher) {
    let mut btree = make_btree("bench_get");
    let (keys, vals) = rand_init_store(&mut btree, N_VALS);

    b.iter(move || {
        for i in 0 .. keys.len() {
            btree.get(keys[i]);
        }
    });
}

fn bench_bt_update(b: &mut Bencher) {
    let mut btree = make_btree("bench_update");
    let (keys, vals) = rand_init_store(&mut btree, N_VALS);

    b.iter(move || {
        for i in 0 .. keys.len() {
            btree.put(keys[i], vals[(i + 1) % N_VALS]);
        }
    });
}

fn bench_bt_delete(b: &mut Bencher) {
    let mut btree = make_btree("bench_delete");
    let (keys, vals) = rand_init_store(&mut btree, N_VALS);

    b.iter(move || {
        for i in 0 .. keys.len() / 2 {
            btree.delete(keys[i]);
        }
    });
}

fn bench_lsm_get(b: &mut Bencher) {
    let mut lsm = make_lsm("bench_get");
    let (keys, vals) = rand_init_store(&mut lsm, N_VALS);

    b.iter(move || {
        for i in 0 .. keys.len() {
            lsm.get(keys[i]);
        }
    });
}

benchmark_group!(benches, bench_bt_get, bench_bt_update, bench_bt_delete);
benchmark_main!(benches);