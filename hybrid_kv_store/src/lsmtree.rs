use std::cell::RefCell;
use std::cmp::min;
use std::io::Result;
use std::rc::Rc;

use atomic_deque::AtomicDeque;
use bit_vector::BloomFilter;
use constants;
use disk_allocator::DiskAllocator;
use disk_location::DiskLocation;
use kvstore::KVStore;


const ENTRY_SIZE: usize = (4 + 4 + 1);
const ENTRIES_PER_PAGE: usize = constants::PAGE_SIZE / ENTRY_SIZE;


#[derive(Debug)]
enum SearchResult {
    Found(i32),
    NotFound,
    Deleted,
}


#[derive(Debug)]
struct Run {
    location: DiskLocation,
    size: usize,
    bloom_filter: BloomFilter,
    fences: Vec<i32>,
}

impl Run {

    fn new(disk_allocator: Rc<RefCell<DiskAllocator>>, capacity: usize) -> Result<Run> {
        Ok(Run {
            location: disk_allocator.borrow_mut().allocate(capacity)?,
            size: 0,
            bloom_filter: BloomFilter::new(constants::BLOOM_CAPACITY),
            fences: Vec::with_capacity(capacity),
        })
    }

    fn search(&self, key: i32) -> SearchResult {
        if !self.bloom_filter.get(&key) {
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

        for j in 0..min(ENTRIES_PER_PAGE, (self.size - i * ENTRIES_PER_PAGE)) {
            let read_key = self.location.read_int(((i + j) * ENTRY_SIZE) as u64).unwrap();
            if key == read_key {
                let read_val = self.location.read_int(((i + j) * ENTRY_SIZE + 4) as u64).unwrap();
                let deleted = self.location.read_byte(((i + j) * ENTRY_SIZE + 4 + 1) as u64).unwrap();
                if deleted == 1 {
                    return SearchResult::Deleted;
                } else {
                    return SearchResult::Found(read_val);
                }
            }
        }
        return SearchResult::NotFound;
    }
}


#[derive(Debug)]
struct LSMTree {
    /* Our LSM-Tree is leveled, so each level consists of a single run */
    levels: Vec<Run>,
    /* A deque of (key, value, deleted) tuples */
    buffer: AtomicDeque<(i32, i32, bool)>,
}


impl LSMTree {
    

    pub fn new() -> LSMTree {
        LSMTree {
            levels: Vec::new(),
            buffer: AtomicDeque::with_capacity(constants::BUFFER_CAPACITY, (0,0,false)),
        }
    }

    fn search_buffer(&self, key: i32) -> Option<i32> {
        let mut result = None;
        for i in 0..self.buffer.len() {
            if self.buffer[i].0 == key {
                if self.buffer[i].2 {
                    // delete if tombstone record
                    result = None;
                } else {
                    result = Some(self.buffer[i].1);
                }
            }
        }
        return result;
    }

}


impl KVStore for LSMTree {
    
    fn get(&mut self, key : i32) -> Option<i32> {
        match self.search_buffer(key) {
            Some(val) => Some(val),
            None => {
                for run in &self.levels {
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
        self.buffer.push((key, 0, true));
    }

    fn put(&mut self, key : i32, val : i32) {
        self.buffer.push((key, val, false))
    }

    fn scan(&self, low : i32, high : i32) -> Vec<i32> {
        Vec::new()
    }

}
