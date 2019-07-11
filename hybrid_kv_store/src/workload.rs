extern crate rand;
use self::rand::Rng;
use self::rand::distributions::{Weighted, WeightedChoice, Distribution};

use std::fs::File;
use std::io::Write;
use std::time::{Duration, Instant};

use lsmtree::LSMTree;
use kvstore::KVStore;
use tests::rand_init_store;
use transitioning_kvstore::{TransitioningKVStore, TransitionType};


const MAX_KEY : i32 = 10_000;
const SCAN_LEN : i32 = 3;


#[derive(Debug, Copy, Clone)]
enum QueryType {
    Get,
    Put,
    Scan,
}


fn run_query(store: &mut KVStore, qtype: QueryType) {
    let key1 = rand::thread_rng().gen::<i32>() % MAX_KEY;
    match qtype {
        QueryType::Get => { store.get(key1); },
        QueryType::Put => store.put(key1, rand::thread_rng().gen::<i32>() % MAX_KEY),
        QueryType::Scan => { store.scan(key1, key1 + SCAN_LEN); },
    }
}

fn run_workload(store: &mut KVStore, workload: WeightedChoice<QueryType>, num_queries: usize)
    -> Vec<u128> {
    let mut latencies = Vec::with_capacity(num_queries);
    for i in 0..num_queries {
        let qtype = workload.sample(&mut rand::thread_rng());
        let start = Instant::now();
        run_query(store, qtype);
        let latency = start.elapsed().as_nanos();
        latencies.push(latency);
    }
    latencies
}

fn save_latencies(latencies: Vec<u128>, filename: &'static str) {
    let mut file = File::create(filename).unwrap();
    for l in latencies {
        file.write_all(format!("{}\n", l).as_bytes()).unwrap();
    }
}


fn get_write_heavy_items() -> Vec<Weighted<QueryType>> {
    vec![
        Weighted { weight: 69, item: QueryType::Get },
        Weighted { weight: 30, item: QueryType::Put },
        Weighted { weight: 0, item: QueryType::Scan },
        // 1
    ]
}


fn get_read_heavy_items() -> Vec<Weighted<QueryType>> {
    vec![
        Weighted { weight: 80, item: QueryType::Get },
        Weighted { weight: 0, item: QueryType::Put },
        Weighted { weight: 19, item: QueryType::Scan },
        // 19
    ]
}


pub fn simulate_store(store: &mut KVStore, filename: &'static str) {
    let mut write_items = get_write_heavy_items();
    let write_workload = WeightedChoice::new(&mut write_items);
    let mut read_items = get_read_heavy_items();
    let read_workload = WeightedChoice::new(&mut read_items);

    let mut latencies = run_workload(store, write_workload, 1000);
    latencies.append(&mut run_workload(store, read_workload, 1000));

    let mut write_items2 = get_write_heavy_items();
    let write_workload2 = WeightedChoice::new(&mut write_items2);
    latencies.append(&mut run_workload(store, write_workload2, 1000));

    save_latencies(latencies, filename);
}

pub fn simulate_transition(lsm_filename: &'static str, btree_filename: &'static str, latencies_filename: &'static str) {
    let mut lsm = LSMTree::new(lsm_filename);
    rand_init_store(&mut lsm, 10_000);

    let mut write_items = get_write_heavy_items();
    let write_workload = WeightedChoice::new(&mut write_items);
    let mut read_items = get_read_heavy_items();
    // let read_workload = WeightedChoice::new(&mut read_items);

    let mut latencies = run_workload(&mut lsm, write_workload, 1000);
    let mut transition = TransitioningKVStore::new(lsm, TransitionType::SortMerge, btree_filename);
    for i in 0..33 {
        let start = Instant::now();
        transition.step();
        let transition_ns = start.elapsed().as_nanos();

        let mut read_items2 = get_read_heavy_items();
        let mut items = read_items.clone();
        let read_workload2 = WeightedChoice::new(&mut items);
        let mut transition_latencies = run_workload(&mut transition, read_workload2, 5);
        transition_latencies[0] += transition_ns;
        latencies.append(&mut transition_latencies);
    }
    let mut btree = transition.into_btree();
    let mut items = read_items.clone();
    let read_workload2 = WeightedChoice::new(&mut items);
    latencies.append(&mut run_workload(&mut btree, read_workload2, 835));

    let start = Instant::now();
    let mut lsm = btree.into_lsm_tree(lsm_filename);
    let transition_ns = start.elapsed().as_nanos();

    let mut write_items2 = get_write_heavy_items();
    let write_workload2 = WeightedChoice::new(&mut write_items2);
    let mut final_latencies = run_workload(&mut lsm, write_workload2, 1000);
    final_latencies[0] += transition_ns;
    latencies.append(&mut final_latencies);

    save_latencies(latencies, latencies_filename);
}
