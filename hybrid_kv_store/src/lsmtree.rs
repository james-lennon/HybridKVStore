use std::cell::{RefCell, UnsafeCell};
use std::cmp::min;
use std::collections::{BinaryHeap, HashSet};
use std::fs::{create_dir_all, File};
use std::io::{Write, Result};
use std::slice::Iter;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicPtr, Ordering};
use std::thread;

use atomic_deque::AtomicDeque;
use bit_vector::{BloomFilter, BitVector};
use btree::{BTree, BTreeOptions};
use constants;
use disk_allocator::{DiskAllocator, SingleFileBufferAllocator};
use disk_location::DiskLocation;
use kvstore::KVStore;
// use merge_executor::MergeExecutor;


pub const ENTRY_SIZE: usize = (4 + 4 + 1);
const ENTRIES_PER_PAGE: usize = constants::PAGE_SIZE / ENTRY_SIZE;


fn merge_entries_into(
    entries1: &Vec<(i32, i32, bool)>,
    entries2: &Vec<(i32, i32, bool)>,
    result: &mut Vec<(i32, i32, bool)>,
) {
    result.clear();

    let mut a = 0;
    let mut b = 0;

    while a < entries1.len() && b < entries2.len() {
        if entries1[a].0 < entries2[b].0 {
            result.push(entries1[a]);
            a += 1;
        } else if entries1[a].0 == entries2[b].0 {
            a += 1;
        } else {
            result.push(entries2[b]);
            b += 1;
        }
    }

    while a < entries1.len() {
        result.push(entries1[a]);
        a += 1;
    }
    while b < entries2.len() {
        result.push(entries2[b]);
        b += 1;
    }
}


fn search_levels(levels: &Vec<Run>, key: i32) -> Option<i32> {
    for run in levels {
        match run.search(key) {
            SearchResult::Found(val) => return Some(val),
            SearchResult::NotFound => (),
            SearchResult::Deleted => return None,
        };
    }
    None
}


#[derive(Debug, PartialEq)]
enum SearchResult {
    Found(i32),
    NotFound,
    Deleted,
}

enum MergeSource<'a, T>
where
    T: DiskLocation,
{
    Buffer(Iter<'a, (i32, i32, bool)>),
    Disk(Arc<T>),
}

#[derive(Debug)]
enum MergeResult {
    Merged(Run),
    Overflow,
}

#[derive(Clone, Debug)]
pub struct Run {
    disk_location: Arc<DiskLocation>,
    size: usize,
    capacity: usize,
    bloom_filter: Arc<RefCell<BloomFilter>>,
    fences: Vec<i32>,
}

impl Run {
    pub fn new(disk_location: Arc<DiskLocation>, capacity: usize) -> Run {
        Run {
            disk_location: disk_location,
            size: 0,
            capacity: capacity,
            bloom_filter: Arc::new(RefCell::new(BloomFilter::new(constants::BLOOM_CAPACITY))),
            fences: Vec::with_capacity(capacity),
        }
    }

    pub fn search(&self, key: i32) -> SearchResult {
        // println!("searching bloom...");
        if !self.bloom_filter.borrow_mut().get(&key) {
            // println!("\tnot found");
            return SearchResult::NotFound;
        }
        let num_fences = ((self.size as f64 / ENTRIES_PER_PAGE as f64).ceil() - 1.0) as usize;
        let mut i = 0;
        while i < num_fences {
            if key < self.fences[i] {
                break;
            }
            i += 1;
        }

        // println!("{:?}", min(ENTRIES_PER_PAGE, (self.size - i * ENTRIES_PER_PAGE)));
        for j in 0..min(ENTRIES_PER_PAGE, (self.size - i * ENTRIES_PER_PAGE)) {
            let read_key = self.disk_location
                .read_int(((i * ENTRIES_PER_PAGE + j) * ENTRY_SIZE) as u64)
                .unwrap();
            // println!("{:?} {}", read_key, key);
            if key == read_key {
                let read_val = self.disk_location
                    .read_int(((i * ENTRIES_PER_PAGE + j) * ENTRY_SIZE + 4) as u64)
                    .unwrap();
                let deleted = self.disk_location
                    .read_byte(((i * ENTRIES_PER_PAGE + j) * ENTRY_SIZE + 4 + 1) as u64)
                    .unwrap();
                if deleted == 1 {
                    return SearchResult::Deleted;
                } else {
                    return SearchResult::Found(read_val);
                }
            }
        }
        return SearchResult::NotFound;
    }

    pub fn read_all(&self) -> Vec<(i32, i32, bool)> {
        assert!(self.size <= self.capacity);
        let mut result = Vec::with_capacity(self.size);
        for i in 0..self.size {
            let key = self.disk_location
                .read_int((i * ENTRY_SIZE) as u64)
                .unwrap();
            let val = self.disk_location
                .read_int((i * ENTRY_SIZE + 4) as u64)
                .unwrap();
            let del = self.disk_location
                .read_byte((i * ENTRY_SIZE + 5) as u64)
                .unwrap();
            result.push((key, val, del == 1));
        }
        result
    }

    pub fn write_all(&mut self, entries: &Vec<(i32, i32, bool)>) {
        assert!(entries.len() <= self.capacity);
        let mut bloom = self.bloom_filter.borrow_mut();
        // println!("writing...");
        for i in 0..entries.len() {
            self.disk_location
                .write_int((i * ENTRY_SIZE) as u64, entries[i].0)
                .unwrap();
            self.disk_location
                .write_int((i * ENTRY_SIZE + 4) as u64, entries[i].1)
                .unwrap();
            let byte = if entries[i].2 { 1 } else { 0 };
            self.disk_location
                .write_byte((i * ENTRY_SIZE + 5) as u64, byte)
                .unwrap();

            // Add to bloom
            bloom.add(&entries[i].0);
            // Add fences
            if i > 0 && i % ENTRIES_PER_PAGE == 0 {
                self.fences.push(entries[i].0);
            }
        }
        self.size = entries.len();
    }
}


#[derive(Debug)]
pub struct LSMTree {
    /* Our LSM-Tree is leveled, so each level consists of a single run */
    levels: Arc<AtomicPtr<Vec<Run>>>,
    /* A deque of (key, value, deleted) tuples */
    buffer: Arc<UnsafeCell<AtomicDeque<(i32, i32, bool)>>>,
    disk_allocator: Arc<Mutex<SingleFileBufferAllocator>>,
}


impl LSMTree {
    pub fn new(directory: &str) -> LSMTree {
        // Create directory if not exists
        create_dir_all(directory).unwrap();

        let disk_allocator = SingleFileBufferAllocator::new(directory).unwrap();
        let result = LSMTree {
            levels: Arc::new(AtomicPtr::new(Box::into_raw(Box::new(Vec::new())))),
            buffer: Arc::new(UnsafeCell::new(AtomicDeque::with_capacity(
                constants::BUFFER_CAPACITY,
                (0, 0, false),
            ))),
            disk_allocator: Arc::new(Mutex::new(disk_allocator)),
        };
        result.start_buffer_thread();
        result
    }

    pub fn from_run(base_run: Run, directory: &str) -> LSMTree {
        let mut disk_allocator = SingleFileBufferAllocator::new(directory).unwrap();
        let mut cur_capacity = constants::BUFFER_CAPACITY * constants::TREE_RATIO;
        let mut levels = Vec::new();
        while cur_capacity < base_run.capacity {
            levels.push(Run::new(Arc::new(disk_allocator.allocate(0).unwrap()), 0));

            cur_capacity *= constants::TREE_RATIO;
        }
        levels.push(base_run);

        let mut result = LSMTree {
            levels: Arc::new(AtomicPtr::new(Box::into_raw(Box::new(levels)))),
            buffer: Arc::new(UnsafeCell::new(AtomicDeque::with_capacity(
                constants::BUFFER_CAPACITY,
                (0, 0, false)))),
            disk_allocator: Arc::new(Mutex::new(disk_allocator)),
        };
        result.start_buffer_thread();
        result
    }

    fn lowest_level_into_btree(&mut self) -> BTree {
        let levels_ptr = self.levels.load(Ordering::Acquire);
        let mut levels = unsafe { &mut *levels_ptr };
        let last_index = levels.len() - 1;
        let last_run = levels.remove(last_index);
        let disk_allocator = &self.disk_allocator;
        let mut btree = BTree::from_disk_location(
            last_run.disk_location,
            last_run.fences,
            last_run.size,
            Arc::clone(disk_allocator),
            BTreeOptions::new(),
        );

        self.levels.store(levels_ptr, Ordering::Release);
        btree
    }

    fn insert_entries_into_btree(&self, btree: &mut BTree) {
        // Head contains tuples of (negative key, run_index, offset, size, &disk_location)
        let mut heap = BinaryHeap::new();

        // TODO: disable flushing here
        let mut levels = unsafe { &*self.levels.load(Ordering::Acquire) };
        let mut disk_locations = Vec::with_capacity(levels.len());
        for i in 0..levels.len() {
            let run = &levels[i];
            if run.size == 0 {
                continue;
            }

            let first_key = run.disk_location.read_int(0).unwrap();
            heap.push((-first_key, i, 0, run.size));
            disk_locations.push(&run.disk_location);
        }

        while heap.len() > 0 {
            let (key, run_index, offset, size) = heap.pop().unwrap();
            let disk_location = disk_locations[run_index];
            let mut should_apply = true;

            // To ensure proper merging, skip this entry if there is a more recent
            // entry on the heap.  Otherwise, remove older entries.
            loop {
                match heap.peek() {
                    Some(&(peek_key, peek_run_index, peek_offset, peek_size)) => {
                        if (key == peek_key) {
                            if (peek_run_index > run_index) {
                                heap.pop();
                                if peek_offset < peek_size - 1 {
                                    // Re-insert next entry
                                    let next_key = disk_locations[peek_run_index]
                                        .read_int(((peek_offset + 1) * ENTRY_SIZE) as u64)
                                        .unwrap();
                                    heap.push(
                                        (next_key, peek_run_index, peek_offset + 1, peek_size),
                                    );
                                }
                            } else {
                                should_apply = false;
                                break;
                            }
                        } else {
                            break;
                        }
                    }
                    None => break,
                };
            }

            if offset < size - 1 {
                // Re-insert next entry
                let next_key = disk_location
                    .read_int(((offset + 1) * ENTRY_SIZE) as u64)
                    .unwrap();
                heap.push((next_key, run_index, offset + 1, size));
            }

            if !should_apply {
                continue;
            }

            let val = disk_location
                .read_int((ENTRY_SIZE * offset + 4) as u64)
                .unwrap();
            let is_deleted = disk_location
                .read_byte((ENTRY_SIZE * offset + 8) as u64)
                .unwrap() == 1;

            // Apply entry
            if is_deleted {
                btree.delete(key);
            } else {
                btree.put(key, val);
            }

        }
        // TODO: re-enable flushing here
    }

    pub fn into_btree(mut self) -> BTree {
        let mut btree = self.lowest_level_into_btree();
        self.insert_entries_into_btree(&mut btree);
        btree
    }

    fn search_buffer(&self, key: i32) -> SearchResult {
        let buffer = unsafe { &*self.buffer.get() };
        for i in (0..buffer.len()).rev() {
            if buffer[i].0 == key {
                if buffer[i].2 {
                    // delete if tombstone record
                    return SearchResult::Deleted;
                } else {
                    return SearchResult::Found(buffer[i].1);
                }
            }
        }
        SearchResult::NotFound
    }

    fn start_buffer_thread(&self) {
        let buffer = unsafe { &mut *self.buffer.get() };
        let levels_ptr = Arc::clone(&self.levels);
        let disk_allocator_ptr = Arc::clone(&self.disk_allocator);
        // let mut merge_executor = MergeExecutor::new(Arc::clone(&mut self.buffer), levels_ptr, disk_allocator_ptr);

        thread::spawn(move || {
            // merge_executor.start_merge_loop();
            loop {
                let mut sorted_buffer = Vec::with_capacity(constants::BUFFER_CAPACITY);
                buffer.clone_contents_into(&mut sorted_buffer);
                let num_read = sorted_buffer.len();

                if num_read == 0 {
                    continue;
                }
                // println!("moving from buffer: {:?}\n{:?}", sorted_buffer, buffer);

                let mut set = HashSet::new();
                let mut i = sorted_buffer.len() - 1;
                let mut remove_bit_vec = BitVector::new(sorted_buffer.len());
                {
                    loop {
                        let key = sorted_buffer[i].0;
                        if set.contains(&key) {
                            remove_bit_vec.set(i);
                        }
                        set.insert(key);
                        if i == 0 {
                            break;
                        } else {
                            i -= 1;
                        }
                    }
                }

                let mut write_pt = 0;
                for read_pt in 0..sorted_buffer.len() {
                    // Only write if not overwritten
                    if !remove_bit_vec.get(read_pt) {
                        // Sinc it's in-place, we only have to overwrite when there's an offset
                        if write_pt != read_pt {
                            sorted_buffer[write_pt] = sorted_buffer[read_pt];
                        }
                        write_pt += 1;
                    }
                }
                sorted_buffer.truncate(write_pt);

                sorted_buffer.sort();

                // TODO: make so that it re-uses vector between iterations
                let mut levels = unsafe { &*levels_ptr.load(Ordering::Acquire) };
                let mut disk_allocator = disk_allocator_ptr.lock().unwrap();
                let mut level = 0;
                let mut new_levels = Vec::new();
                let mut buffer_to_merge = Some(sorted_buffer);
                loop {
                    if level >= levels.len() && buffer_to_merge.is_none() {
                        break;
                    }
                    let capacity = constants::BUFFER_CAPACITY *
                        ((constants::TREE_RATIO).pow((level + 1) as u32));
                    if level >= levels.len() {
                        let disk_location = disk_allocator.allocate(ENTRY_SIZE * capacity).unwrap();
                        let mut new_run = Run::new(Arc::new(disk_location), capacity);
                        buffer_to_merge = match buffer_to_merge {
                            Some(buf) => {
                                if buf.len() <= capacity {
                                    new_run.write_all(&buf);
                                    None
                                } else {
                                    Some(buf)
                                }
                            }
                            None => panic!("impossible"),
                        };
                        new_levels.push(new_run);
                    } else {
                        buffer_to_merge = match buffer_to_merge {
                            Some(buf) => {
                                let mut new_buf = Vec::new();
                                merge_entries_into(&levels[level].read_all(), &buf, &mut new_buf);

                                let disk_location =
                                    disk_allocator.allocate(ENTRY_SIZE * capacity).unwrap();
                                let mut new_run = Run::new(Arc::new(disk_location), capacity);
                                let mut new_buffer_to_merge = None;
                                if new_buf.len() <= capacity {
                                    new_run.write_all(&new_buf);
                                } else {
                                    new_buffer_to_merge = Some(new_buf);
                                }
                                new_levels.push(new_run);
                                new_buffer_to_merge
                            }
                            None => {
                                new_levels.push(levels[level].clone());
                                None
                            }
                        }
                    }

                    level += 1;
                }
                levels_ptr.store(Box::into_raw(Box::new(new_levels)), Ordering::Release);
                buffer.drop_first(num_read);
            }
        });
    }

    fn push_to_buffer(&mut self, key: i32, val: i32, deleted: bool) {
        let mut buffer = unsafe { &mut *self.buffer.get() };
        while buffer.len() == constants::BUFFER_CAPACITY {
            // Spin until thread empties buffer
        }
        buffer.push((key, val, deleted));
    }
}


impl KVStore for LSMTree {
    fn get(&mut self, key: i32) -> Option<i32> {
        match self.search_buffer(key) {
            SearchResult::Found(val) => Some(val),
            SearchResult::Deleted => None,
            SearchResult::NotFound => {
                let levels = unsafe { &*self.levels.load(Ordering::Relaxed) };
                search_levels(levels, key)
            }
        }
    }

    fn delete(&mut self, key: i32) {
        self.push_to_buffer(key, 0, true)
    }

    fn put(&mut self, key: i32, val: i32) {
        self.push_to_buffer(key, val, false)
    }

    fn scan(&self, low: i32, high: i32) -> Vec<i32> {
        Vec::new()
    }

    fn debug_lookup(&mut self, key: i32) {
        let levels = unsafe { &*self.levels.load(Ordering::Relaxed) };
        for i in 0..levels.len() {
            let run = &levels[i];
            let lookup_result = run.search(key);
            println!("Level {}: {:?}", i, lookup_result);
        }
    }
}
