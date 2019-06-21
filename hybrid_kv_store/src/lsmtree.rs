use std::cell::{RefCell, UnsafeCell};
use std::cmp::min;
use std::collections::{BinaryHeap, HashSet};
use std::fs::{create_dir_all, File};
use std::io::{Write, Result};
use std::slice::Iter;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, AtomicUsize, AtomicPtr, Ordering};
use std::thread;
use std::rc::Rc;
use std::ops::Drop;

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
        // println!("searching level...");
        match run.search(key) {
            SearchResult::Found(val) => return Some(val),
            SearchResult::NotFound => (),
            SearchResult::Deleted => return None,
        };
    }
    None
}


fn sort_and_compress_buffer(buffer: &mut Vec<(i32, i32, bool)>) {
    let mut set = HashSet::new();
    let mut i = buffer.len() - 1;
    let mut remove_bit_vec = BitVector::new(buffer.len());
    {
        loop {
            let key = buffer[i].0;
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
    for read_pt in 0..buffer.len() {
        // Only write if not overwritten
        if !remove_bit_vec.get(read_pt) {
            // Sinc it's in-place, we only have to overwrite when there's an offset
            if write_pt != read_pt {
                buffer[write_pt] = buffer[read_pt];
            }
            write_pt += 1;
        }
    }
    buffer.truncate(write_pt);
    buffer.sort();
}


#[derive(Debug, PartialEq)]
enum SearchResult {
    Found(i32),
    NotFound,
    Deleted,
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
    start_offset: Arc<AtomicUsize>,
}

impl Run {
    pub fn new(disk_location: Arc<DiskLocation>, capacity: usize) -> Run {
        Run {
            disk_location: disk_location,
            size: 0,
            capacity: capacity,
            bloom_filter: Arc::new(RefCell::new(BloomFilter::new(constants::BLOOM_CAPACITY))),
            fences: Vec::with_capacity(capacity),
            start_offset: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn search(&self, key: i32) -> SearchResult {
        if !self.bloom_filter.borrow_mut().get(&key) {
            return SearchResult::NotFound;
        }
        if self.size == 0 {
            return SearchResult::NotFound;
        }
        let num_fences = ((self.size as f64 / ENTRIES_PER_PAGE as f64).ceil() - 1.0) as usize;
        let start = self.start_offset.load(Ordering::Relaxed);
        let start_fence = start / ENTRIES_PER_PAGE;
        let start_fence_residual = start % ENTRIES_PER_PAGE;
        let mut i = start_fence;
        while i < num_fences {
            if key < self.fences[i] {
                break;
            }
            i += 1;
        }

        // println!("{:?}", min(ENTRIES_PER_PAGE, (self.size - i * ENTRIES_PER_PAGE)));
        let max_idx =
            if i * ENTRIES_PER_PAGE > self.size || i * ENTRIES_PER_PAGE > ENTRIES_PER_PAGE {
                min(ENTRIES_PER_PAGE, self.size)
            } else {
                (self.size - i * ENTRIES_PER_PAGE)
            };
        let min_idx =
	    if i == start_fence {
	        start_fence_residual
	    } else {
                0
            };
        for j in min_idx..max_idx {
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

    pub fn find_lowest_index_ge_key(&self, key : i32) -> usize {
        let num_fences = ((self.size as f64 / ENTRIES_PER_PAGE as f64).ceil() - 1.0) as usize;
        let start = self.start_offset.load(Ordering::Relaxed);
        let start_fence = start / ENTRIES_PER_PAGE;
        let start_fence_residual = start % ENTRIES_PER_PAGE;
        let mut i = start_fence;
        while i < num_fences {
            if key < self.fences[i] {
                break;
            }
            i += 1;
        }

        // println!("{:?}", min(ENTRIES_PER_PAGE, (self.size - i * ENTRIES_PER_PAGE)));
        let max_idx =
            if i * ENTRIES_PER_PAGE > self.size || i * ENTRIES_PER_PAGE > ENTRIES_PER_PAGE {
                min(ENTRIES_PER_PAGE, self.size)
            } else {
                (self.size - i * ENTRIES_PER_PAGE)
            };
        let min_idx =
            if i == start_fence {
                start_fence_residual
            } else {
                0
            };
        for j in min_idx..max_idx {
            let read_key = self.disk_location
                .read_int(((i * ENTRIES_PER_PAGE + j) * ENTRY_SIZE) as u64)
                .unwrap();
            // println!("{:?} {}", read_key, key);
            if read_key >= key {
                return i * ENTRIES_PER_PAGE + j;
            }
        }
	i * ENTRIES_PER_PAGE + max_idx
    }

    pub fn read_all(&self) -> Vec<(i32, i32, bool)> {
        assert!(self.size <= self.capacity);
        let mut result = Vec::with_capacity(self.size);
        let start = self.start_offset.load(Ordering::Relaxed);
        for i in start..self.size {
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

    pub fn construct_bloom_and_fences(&mut self) {
        let mut bloom = self.bloom_filter.borrow_mut();
        self.size = self.capacity / ENTRY_SIZE;
        for i in 0 .. self.size {
            let key = self.disk_location
                .read_int((i * ENTRY_SIZE) as u64)
                .unwrap();

            bloom.add(&key);
            if i > 0 && i % ENTRIES_PER_PAGE == 0 {
                self.fences.push(key);
            }
        }
    }

    pub fn peek_lowest(&self) -> (i32, i32) {
        let start = self.start_offset.load(Ordering::Relaxed);
        let key = self.disk_location
            .read_int((start * ENTRY_SIZE) as u64)
            .unwrap();
        let val = self.disk_location
            .read_int((start * ENTRY_SIZE + 4) as u64)
            .unwrap();
        (key, val)
    }

    pub fn increment_offset(&mut self) {
        let start = self.start_offset.load(Ordering::Relaxed);
        self.start_offset.store(start + 1, Ordering::Relaxed);
        self.size -= 1;
   }
}


#[derive(Debug)]
pub struct LSMTree {
    /* Our LSM-Tree is leveled, so each level consists of a single run */
    levels: Arc<AtomicPtr<Vec<Run>>>,
    /* A deque of (key, value, deleted) tuples */
    buffer: Arc<UnsafeCell<AtomicDeque<(i32, i32, bool)>>>,
    disk_allocator: Arc<Mutex<SingleFileBufferAllocator>>,
    /* Manage thread */
    should_merge: Arc<AtomicBool>,
    join_handle: Option<Rc<thread::JoinHandle<()>>>,
}


impl LSMTree {
    pub fn new(directory: &str) -> LSMTree {
        // Create directory if not exists
        create_dir_all(directory).unwrap();

        let disk_allocator = SingleFileBufferAllocator::new(directory).unwrap();
        let mut result = LSMTree {
            levels: Arc::new(AtomicPtr::new(Box::into_raw(Box::new(Vec::new())))),
            buffer: Arc::new(UnsafeCell::new(AtomicDeque::with_capacity(
                constants::BUFFER_CAPACITY,
                (0, 0, false),
            ))),
            disk_allocator: Arc::new(Mutex::new(disk_allocator)),
            should_merge: Arc::new(AtomicBool::new(true)),
            join_handle: None,
        };
        result.start_buffer_thread();
        result
    }

    pub fn from_run(base_run: Run, directory: &str) -> LSMTree {
        // Create directory if not exists
        create_dir_all(directory).unwrap();
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
                (0, 0, false),
            ))),
            disk_allocator: Arc::new(Mutex::new(disk_allocator)),
            should_merge: Arc::new(AtomicBool::new(true)),
            join_handle: None,
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

    fn start_buffer_thread(&mut self) {
        let buffer = unsafe { &mut *self.buffer.get() };
        let should_merge = Arc::clone(&self.should_merge);
        let levels_ptr = Arc::clone(&self.levels);
        let disk_allocator_ptr = Arc::clone(&self.disk_allocator);
        // let mut merge_executor = MergeExecutor::new(Arc::clone(&mut self.buffer), levels_ptr, disk_allocator_ptr);

        let join_handle = thread::spawn(move || {
            // merge_executor.start_merge_loop();
            while should_merge.load(Ordering::Relaxed) {
                let mut sorted_buffer = Vec::with_capacity(constants::BUFFER_CAPACITY);
                buffer.clone_contents_into(&mut sorted_buffer);
                let num_read = sorted_buffer.len();

                if num_read == 0 {
                    continue;
                }
                // println!("moving from buffer: {:?}\n{:?}", sorted_buffer, buffer);

                sort_and_compress_buffer(&mut sorted_buffer);
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
                if should_merge.load(Ordering::Relaxed) {
                    levels_ptr.store(Box::into_raw(Box::new(new_levels)), Ordering::Release);
                    buffer.drop_first(num_read);
                }
            }
        });
        self.join_handle = Some(Rc::new(join_handle));
    }

    fn push_to_buffer(&mut self, key: i32, val: i32, deleted: bool) {
        let mut buffer = unsafe { &mut *self.buffer.get() };
        while buffer.len() == constants::BUFFER_CAPACITY {
            // Spin until thread empties buffer
        }
        buffer.push((key, val, deleted));
    }

    pub fn pop_lowest_n(&mut self, num: usize) -> Vec<(i32, i32)> {
        // Stop buffer thread
        self.should_merge.store(false, Ordering::Relaxed);

        let result = extract_lowest_from_lsm_tree(self, num);
        // Restart buffer thread
        self.start_buffer_thread();
        result
    }
}


impl Drop for LSMTree {
    fn drop(&mut self) {
        self.should_merge.store(false, Ordering::Relaxed);
        let mut join_handle_ptr = None;
        self.join_handle = match self.join_handle {
            Some(ref mut handle) => {
                join_handle_ptr = Some(Rc::clone(handle));
                // let handle_clone = Rc::clone(handle);
                // handle_clone.join();
                None
            },
            None => None,
        };

        let mut join_handle = (Rc::try_unwrap(join_handle_ptr.unwrap()).unwrap());
        join_handle.join().unwrap();
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

    fn scan(&mut self, low: i32, high: i32) -> Vec<i32> {
        let mut heap = BinaryHeap::new();   // Heap to execute scan (key, rank, current index)
        let mut high_indices = Vec::new();  // Vector to track high indices

        // Create a sorted, compressed buffer to use
        let buffer = unsafe { &mut *self.buffer.get() };
        let mut sorted_buffer = Vec::with_capacity(buffer.len());
        if (buffer.len() > 0)  {
            buffer.clone_contents_into(&mut sorted_buffer);
            sort_and_compress_buffer(&mut sorted_buffer);
        }
        let sorted_buffer = sorted_buffer.iter().map(|(k, v, _)| (*k, *v)).collect();

	      // Find low and high indices of query in this sorted buffer
        // Add buffer element to heap
        // Add high_buffer_index to high_indices
        let mut i = 0;
        while (i < sorted_buffer.len() && sorted_buffer[i].0 < low) {
            i += 1;
        }
        let low_buffer_index = i;
        while (i < sorted_buffer.len() && sorted_buffer[i].0 < high) {
            i += 1;
        }
        let high_buffer_index = i;
        if (high_buffer_index > low_buffer_index)  {
            heap.push((-sorted_buffer[low_buffer_index].0, 0, low_buffer_index));
        }
        high_indices.push(high_buffer_index);

        // Get low and high indices of run
        // Add run element to heap
        // Add high index to high_indices
        let levels = unsafe { &*self.levels.load(Ordering::Relaxed) };
        for i in 0..levels.len() {
            let run = &levels[i];
            let low_index = run.find_lowest_index_ge_key(low);
            let high_index = run.find_lowest_index_ge_key(high);
            if (high_index > low_index) {
                heap.push((run.disk_location.read_int((low_index * ENTRY_SIZE) as u64).unwrap(),
                           -(i as i32 + 1),
                           low_index));
            }
            high_indices.push(high_index);
        }

        // Iterate extracting lowest entries
        let mut range_result = Vec::new();
        let mut last_key = None;
        while heap.len() > 0 {
            let (key, raw_rank, index) = heap.pop().unwrap();
            let should_push =
                match last_key {
                    Some(val) => val != key,
                    None => true,
                };
            last_key = Some(key);

            if should_push {
                if (raw_rank == 0)  {
                    range_result.push(sorted_buffer[index].1);
                } else {
                    let level_num = -(raw_rank + 1) as usize;
                    let insert_val = levels[level_num].disk_location
                                         .read_int((index * ENTRY_SIZE + 4) as u64)
                                         .unwrap();
                    range_result.push(insert_val);
                }
            }

            if (index == high_indices[-raw_rank as usize]) {
                continue;
            }

            if (raw_rank == 0) {
                heap.push((-sorted_buffer[index].0, 0, index + 1));
            } else {
                let level_num = -(raw_rank + 1) as usize;
                heap.push((levels[level_num].disk_location
                               .read_int((index * ENTRY_SIZE) as u64)
                               .unwrap(),
                           raw_rank,
                           index + 1));
            }
        }

        range_result
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


/* ########################## */
/*   MERGING FOR TRANSITIONS  */
/* ########################## */


trait MergeSource {
    fn peek(&self) -> Option<(i32, i32)>;
    fn pop(&mut self);
}


#[derive(Debug)]
struct BufferMergeSource {
    buffer: Vec<(i32, i32)>,
    index: usize,
    removed_keys: Rc<RefCell<HashSet<i32>>>,
}

impl MergeSource for BufferMergeSource {
    fn peek(&self) -> Option<(i32, i32)> {
        if self.index < self.buffer.len() {
            Some(self.buffer[self.index])
        } else {
            None
        }
    }

    fn pop(&mut self) {
        if self.index < self.buffer.len() {
            self.removed_keys.borrow_mut().insert(self.buffer[self.index].0);
        }
        self.index += 1;
    }
}


#[derive(Debug)]
struct RunMergeSource<'a> {
    run: &'a mut Run,
}


impl<'a> MergeSource for RunMergeSource<'a> {
    fn peek(&self) -> Option<(i32, i32)> {
        if self.run.size() > 0 {
            Some(self.run.peek_lowest())
        } else {
            None
        }
    }

    fn pop(&mut self) {
        self.run.increment_offset();
    }
}

fn extract_lowest_from_lsm_tree(lsmtree: &mut LSMTree, count: usize) -> Vec<(i32, i32)> {
    let mut heap = BinaryHeap::new();
    let mut merge_sources : Vec<Box<MergeSource>> = Vec::new();
    let buffer = unsafe { &mut *lsmtree.buffer.get() };

    let buffer_removed_keys = Rc::new(RefCell::new(HashSet::new()));

    // Add buffer to merge sources
    if buffer.len() > 0 {
        let mut sorted_buffer = Vec::with_capacity(buffer.len());
        buffer.clone_contents_into(&mut sorted_buffer);
        sort_and_compress_buffer(&mut sorted_buffer);

        let sorted_buffer = sorted_buffer.iter().map(|(k, v, _)| (*k, *v)).collect();

        let buffer_merge_source = BufferMergeSource {
            buffer: sorted_buffer,
            index: 0,
            removed_keys: Rc::clone(&buffer_removed_keys),
        };
        heap.push((-buffer_merge_source.peek().unwrap().0, 0));
        merge_sources.push(Box::new(buffer_merge_source));
    }

    // Add each level to merge sources
    let levels = lsmtree.levels.load(Ordering::Relaxed);
    let num_levels = (unsafe{ &*levels }).len();
    for i in 0..num_levels {
        let mut run = &mut (unsafe { &mut *levels })[i];
        if run.size() > 0 {
            let mut source = RunMergeSource {
                run: run,
            };
            heap.push((-source.peek().unwrap().0, -(i as i32 + 1)));
            merge_sources.push(Box::new(source));
        }
    }

    // Use heapsort to extract lowest entries
    let mut result = Vec::new();
    let mut last_key = None;
    while heap.len() > 0 {
        let (key, raw_index) = heap.pop().unwrap();
        let index = (-raw_index) as usize;
        let should_push =
            match last_key {
                Some(val) => val != key,
                None => true,
            };
        last_key = Some(key);

        if should_push {
            if result.len() == count {
                break;
            }
            result.push(merge_sources[index].peek().unwrap());
        }

        merge_sources[index].pop();
        match merge_sources[index].peek() {
            Some((k, _)) => heap.push((-k, raw_index)),
            None => (),
        };
    }

    // Tricky: remove popped entries from buffer
    let mut read_idx = 0;
    let mut write_idx = 0;
    let buffer_removed_keys = buffer_removed_keys.borrow_mut();
    while read_idx < buffer.len() {
        buffer[write_idx] = buffer[read_idx];
        if !buffer_removed_keys.contains(&buffer[read_idx].0) {
            write_idx += 1;
        }
        read_idx += 1;
    }
    buffer.truncate(write_idx);

    result
}
