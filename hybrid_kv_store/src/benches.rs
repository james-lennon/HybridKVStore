#[macro_use]
extern crate criterion;
extern crate rand;

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

use std::{thread, time};

use tests::{make_btree, make_lsm, rand_init_store};
use btree::{BTree, BTreeOptions};
use lsmtree::LSMTree;
use kvstore::KVStore;
use transitioning_kvstore::{TransitioningKVStore, TransitionType, StepResult};

use self::criterion::Criterion;
use self::criterion::black_box;
use self::rand::{thread_rng, Rng};
use self::rand::seq::SliceRandom;



const N_VALS : usize = 100;

fn bench_bt_get(c: &mut Criterion) {
    let mut btree = make_btree("bench_get");
    let (keys, vals) = rand_init_store(&mut btree, N_VALS);

    c.bench_function("bt_get", move |b| {
        b.iter(|| {
            let i = thread_rng().gen::<usize>() % N_VALS;
            btree.get(keys[i]);
        });
    });
}

fn bench_bt_update(c: &mut Criterion) {
    let mut btree = make_btree("bench_update");
    let (keys, vals) = rand_init_store(&mut btree, N_VALS);

    c.bench_function("bt_update", move |b| {
        b.iter(|| {
            let i = thread_rng().gen::<usize>() % N_VALS;
            btree.put(keys[i], vals[(i + 1) % N_VALS]);
        });
    });
}

fn bench_bt_delete(c: &mut Criterion) {
    let mut btree = make_btree("bench_delete");
    let (keys, vals) = rand_init_store(&mut btree, N_VALS);

    c.bench_function("bt_delete", move |b| {
        b.iter(|| {
            let i = thread_rng().gen::<usize>() % N_VALS;
            btree.delete(keys[i]);
        });
    });
}

fn bench_bt_scan(c: &mut Criterion) {
    let mut btree = make_btree("bench_scan");
    let n_vals = 10_000;
    let mut keys : Vec<i32> = ((0 .. n_vals as i32).collect());
    keys.shuffle(&mut thread_rng());

    for &i in &keys {
        btree.put(i, i);
    }

    let scan_range_size : i32 = 500;

    c.bench_function("bt_scan", move |b| {
        b.iter(|| {
            let start = thread_rng().gen::<i32>() % (n_vals - scan_range_size);
            btree.scan(start, start + scan_range_size);
        });
    });
}

fn bench_lsm_get(c: &mut Criterion) {
    let mut lsm = make_lsm("bench_get");
    let (keys, vals) = rand_init_store(&mut lsm, N_VALS);

    c.bench_function("lsm_get", move |b| {
        b.iter(|| {
            let i = thread_rng().gen::<usize>() % N_VALS;
            lsm.get(keys[i]);
        });
    });
}

fn bench_lsm_update(c: &mut Criterion) {
    let mut lsm = make_lsm("bench_update");
    let (keys, vals) = rand_init_store(&mut lsm, N_VALS);

    c.bench_function("lsm_update", move |b| {
        b.iter(|| {
            let i = thread_rng().gen::<usize>() % N_VALS;
            lsm.put(keys[i], vals[(i + 1) % N_VALS]);
        });
    });
}

fn bench_lsm_delete(c: &mut Criterion) {
    let mut lsm = make_lsm("bench_delete");
    let (keys, vals) = rand_init_store(&mut lsm, N_VALS);

    c.bench_function("lsm_delete", move |b| {
        b.iter(|| {
            let i = thread_rng().gen::<usize>() % N_VALS;
            lsm.delete(keys[i]);
        });
    });
}

fn bench_lsm_scan(c: &mut Criterion) {
    let mut lsm = make_lsm("bench_scan");
    let n_vals = 10_000;
    let mut keys : Vec<i32> = ((0 .. n_vals as i32).collect());
    keys.shuffle(&mut thread_rng());

    for &i in &keys {
        lsm.put(i, i);
    }

    let scan_range_size : i32 = 500;

    c.bench_function("lsm_scan", move |b| {
        b.iter(|| {
            let start = thread_rng().gen::<i32>() % (n_vals - scan_range_size);
            lsm.scan(start, start + scan_range_size);
        });
    });
}

fn bench_bt_get_after_transition(c: &mut Criterion) {
    let mut lsm = make_lsm("bench_bt_get_lsm");
    let (keys, vals) = rand_init_store(&mut lsm, N_VALS);
    let mut transition = TransitioningKVStore::new(lsm, TransitionType::SortMerge, "bench_bt_get_after_transition");

    loop {
        match transition.step() {
            StepResult::Complete => { break; },
            StepResult::Incomplete => (),
        }
    }

    let mut btree = transition.into_btree();

    c.bench_function("bt_get_after_transition", move |b| {
        b.iter(|| {
            let i = thread_rng().gen::<usize>() % N_VALS;
            btree.get(keys[i]);
        });
    });
}

criterion_group!(benches, bench_bt_get_after_transition, bench_bt_get, bench_bt_update, bench_bt_delete, bench_bt_scan,
                          bench_lsm_get, bench_lsm_update, bench_lsm_delete, bench_lsm_scan
                          );
criterion_main!(benches);