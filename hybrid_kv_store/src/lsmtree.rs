use std::cell::{RefCell, UnsafeCell};
use std::cmp::min;
use std::collections::HashSet;
use std::fs::create_dir_all;
use std::io::Result;
use std::slice::Iter;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicPtr, Ordering};
use std::thread;

use atomic_deque::AtomicDeque;
use bit_vector::{BloomFilter, BitVector};
use constants;
use disk_allocator::{DiskAllocator, SingleFileBufferAllocator};
use disk_location::DiskLocation;
use kvstore::KVStore;


const ENTRY_SIZE: usize = (4 + 4 + 1);
const ENTRIES_PER_PAGE: usize = constants::PAGE_SIZE / ENTRY_SIZE;


fn merge_entries_into(entries1: &Vec<(i32, i32, bool)>, entries2: &Vec<(i32, i32, bool)>,
                      result: &mut Vec<(i32, i32, bool)>) {
    result.clear();

    let mut a = 0;
    let mut b = 0;
    loop {
        
        // Populate with entries from entries1
        while a < entries1.len() && (b >= entries2.len() || entries1[a].0 < entries2[b].0) {
            result.push(entries1[a]);
            a += 1;
        }
        // Populate with entries from entries2
        while b < entries2.len() && (a >= entries1.len() || entries2[b].0 <= entries1[a].0) {
            // Overwrite entries from first vector with entries of second vector if same key
            if a < entries1.len() && entries2[b].0 == entries1[a].0 {
                a += 1;
            }
            result.push(entries2[b]);
            b += 1;
        }

        if a >= entries1.len() && b >= entries2.len() {
            break;
        }
    }
    // println!("\nMERGING\n{:?}\nand\n{:?}\n=>\n{:?}", entries1, entries2, result);
}



#[derive(Debug)]
enum SearchResult {
    Found(i32),
    NotFound,
    Deleted,
}

enum MergeSource<'a> {
    Buffer(Iter<'a, (i32, i32, bool)>),
    Disk(DiskLocation),
}

#[derive(Debug)]
enum MergeResult {
    Merged(Run),
    Overflow,
}

#[derive(Clone, Debug)]
struct Run {
    disk_location: DiskLocation,
    size: usize,
    capacity: usize,
    bloom_filter: Arc<RefCell<BloomFilter>>,
    fences: Vec<i32>,
}

impl Run {

    fn new(disk_location: DiskLocation, capacity: usize) -> Run {
        Run {
            disk_location: disk_location,
            size: 0,
            capacity: capacity,
            bloom_filter: Arc::new(RefCell::new(BloomFilter::new(constants::BLOOM_CAPACITY))),
            fences: Vec::with_capacity(capacity),
        }
    }

    fn search(&self, key: i32) -> SearchResult {
        // println!("searching bloom...");
        if !self.bloom_filter.borrow_mut().get(&key) {
            // println!("\tnot found");
            return SearchResult::NotFound;
        }
        let num_fences = ((self.size as f64 / ENTRIES_PER_PAGE as f64).ceil() - 1.0) as usize;
        let mut i = 0;
        while i < num_fences {
            if key > self.fences[i] {
                break;
            }
            i += 1;
        }

        // println!("{:?}", (self.size - i * ENTRIES_PER_PAGE));
        for j in 0..min(ENTRIES_PER_PAGE, (self.size - i * ENTRIES_PER_PAGE)) {
            let read_key = self.disk_location.read_int(((i * ENTRIES_PER_PAGE + j) * ENTRY_SIZE) as u64).unwrap();
            // println!("{:?} {}", read_key, key);
            if key == read_key {
                let read_val = self.disk_location.read_int(((i * ENTRIES_PER_PAGE + j) * ENTRY_SIZE + 4) as u64).unwrap();
                let deleted = self.disk_location.read_byte(((i * ENTRIES_PER_PAGE + j) * ENTRY_SIZE + 4 + 1) as u64).unwrap();
                if deleted == 1 {
                    return SearchResult::Deleted;
                } else {
                    return SearchResult::Found(read_val);
                }
            }
        }
        return SearchResult::NotFound;
    }

    fn read_all(&self) -> Vec<(i32, i32, bool)> {
        assert!(self.size <= self.capacity);
        let mut result = Vec::with_capacity(self.size);
        for i in 0..self.size {
            let key = self.disk_location.read_int((i * ENTRY_SIZE) as u64).unwrap();
            let val = self.disk_location.read_int((i * ENTRY_SIZE + 4) as u64).unwrap();
            let del = self.disk_location.read_byte((i * ENTRY_SIZE + 5) as u64).unwrap();
            result.push((key, val, del == 1));
        }
        result
    }

    fn write_all(&mut self, entries: &Vec<(i32, i32, bool)>) {
        assert!(entries.len() <= self.capacity);
        // println!("writing...");
        for i in 0..entries.len() {
            self.disk_location.write_int((i * ENTRY_SIZE) as u64, entries[i].0).unwrap();
            self.disk_location.write_int((i * ENTRY_SIZE + 4) as u64, entries[i].1).unwrap();
            let byte = if entries[i].2 { 1 } else { 0 };
            self.disk_location.write_byte((i * ENTRY_SIZE + 5) as u64, byte).unwrap();

            // println!("wrote {}", entries[i].0);
            // Add to bloom
            self.bloom_filter.borrow_mut().add(&entries[i].0);
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
    

    pub fn new(directory: &'static str) -> LSMTree {
        // Create directory if not exists
        create_dir_all(directory).unwrap();

        let disk_allocator = SingleFileBufferAllocator::new(directory).unwrap();
        let result = LSMTree {
            levels: Arc::new(AtomicPtr::new(Box::into_raw(Box::new(Vec::new())))),
            buffer: Arc::new(UnsafeCell::new(AtomicDeque::with_capacity(constants::BUFFER_CAPACITY, (0,0,false)))),
            disk_allocator: Arc::new(Mutex::new(disk_allocator)),
        };
        result.start_buffer_thread();
        result
    }

    fn search_buffer(&self, key: i32) -> Option<i32> {
        let buffer = unsafe { &*self.buffer.get() };
        let mut result = None;
        for i in 0..buffer.len() {
            if buffer[i].0 == key {
                if buffer[i].2 {
                    // delete if tombstone record
                    result = None;
                } else {
                    result = Some(buffer[i].1);
                }
            }
        }
        return result;
    }

    fn start_buffer_thread(&self) {
        let buffer = unsafe { &mut *self.buffer.get() };
        let levels_ptr = Arc::clone(&self.levels);
        let disk_allocator_ptr = Arc::clone(&self.disk_allocator);

        thread::spawn(move || {
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

                let mut offset = 0;
                i = 0;
                while i < sorted_buffer.len() - offset {
                    // If keys are the same, only keep most recent entry
                    if remove_bit_vec.get(i) {
                        offset += 1;
                    } else {
                        sorted_buffer[i - offset] = sorted_buffer[i];
                    }
                    i += 1;
                }
                sorted_buffer.truncate(i);

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
                    let capacity = constants::BUFFER_CAPACITY * ((constants::TREE_RATIO).pow((level + 1) as u32));
                    if level >= levels.len() {
                        let disk_location = disk_allocator.allocate(ENTRY_SIZE * capacity).unwrap();
                        let mut new_run = Run::new(disk_location, capacity);
                        buffer_to_merge =
                            match buffer_to_merge {
                                Some(buf) => {
                                    if buf.len() <= capacity {
                                        new_run.write_all(&buf);
                                        None
                                    } else {
                                        Some(buf)
                                    }
                                },
                                None => panic!("impossible"),
                            };
                        new_levels.push(new_run);
                    } else {
                        buffer_to_merge =
                            match buffer_to_merge {
                                Some(buf) => {
                                    let mut new_buf = Vec::new();
                                    merge_entries_into(&levels[level].read_all(), &buf, &mut new_buf);

                                    let disk_location = disk_allocator.allocate(ENTRY_SIZE * capacity).unwrap();
                                    let mut new_run = Run::new(disk_location, capacity);
                                    let mut new_buffer_to_merge = None;
                                    if new_buf.len() <= capacity {
                                        new_run.write_all(&new_buf);
                                    } else {
                                        new_buffer_to_merge = Some(new_buf);
                                    }
                                    new_levels.push(new_run);
                                    new_buffer_to_merge
                                },
                                None => {
                                    new_levels.push(levels[level].clone());
                                    None
                                },
                            }
                    }

                    level += 1;
                }

                levels_ptr.store(Box::into_raw(Box::new(new_levels)), Ordering::Release);
                buffer.drop_first(num_read);
            }
        });
    }

}


impl KVStore for LSMTree {
    
    fn get(&mut self, key : i32) -> Option<i32> {
        // println!("searching for {}", key);
        // println!("{:?}", unsafe {&*self.buffer.get()});
        match self.search_buffer(key) {
            Some(val) => Some(val),
            None => {
                let levels = unsafe { &*self.levels.load(Ordering::Relaxed) };
                for run in levels {
                    match run.search(key) {
                        SearchResult::Found(val) => return Some(val),
                        SearchResult::NotFound => (),
                        SearchResult::Deleted => return None,
                    };
                }
                None
            }
        }
    }

    fn delete(&mut self, key: i32) {
        // Append a tombstone record
        unsafe { &mut *self.buffer.get() }.push((key, 0, true));
    }

    fn put(&mut self, key : i32, val : i32) {
        let mut buffer = unsafe { &mut *self.buffer.get() };

        while buffer.len() == constants::BUFFER_CAPACITY {
            // Spin until thread empties buffer
        }
        buffer.push((key, val, false));
    }

    fn scan(&self, low : i32, high : i32) -> Vec<i32> {
        Vec::new()
    }

}
