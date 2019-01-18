use std::boxed::Box;
use std::vec::Vec;
use std::io::Result;
use std::fs::create_dir_all;
use std::sync::{Arc, Mutex};

use constants;
use disk_location::DiskLocation;
use disk_allocator::{DiskAllocator, SingleFileBufferAllocator};
use kvstore::KVStore;


/* Each BTree entry will contain one unused byte at end to be compatible with LSM-Tree */
const ENTRY_SIZE : usize = (4 + 4 + 1);


fn build_tree_from_run(disk_location: DiskLocation, fences: Vec<i32>,
                       size: usize, disk_allocator: &Arc<Mutex<SingleFileBufferAllocator>>)
                       -> Box<IntermediateNode> {
    let mut cur_nodes = Vec::with_capacity(fences.len() + 1);
    let entry_size = 4 + 4 + 1;
    let leaf_bytes = constants::FANOUT * entry_size;

    // First build leaf nodes
    for i in 0..fences.len() {
        let leaf_disk_location = disk_location.clone_with_offset((i * entry_size) as u64);
        let mut leaf_node = LeafNode::new(leaf_disk_location, Arc::clone(disk_allocator));
        if i == fences.len() - 1 {
            leaf_node.size = size - i * constants::FANOUT;
        } else {
            leaf_node.size = constants::FANOUT;
        }
        cur_nodes.push(BTreeNode::Leaf(Box::new(leaf_node)));
    }

    if fences.len() == 0 {
        let leaf_disk_location = disk_location.clone();
        let mut leaf_node = LeafNode::new(leaf_disk_location, Arc::clone(disk_allocator));
        leaf_node.size = size;
        cur_nodes.push(BTreeNode::Leaf(Box::new(leaf_node)));
    }

    let mut cur_fences = fences;
    let mut is_first = true;

    while is_first || cur_nodes.len() > 1 {
        is_first = false;

        // Build next level of tree
        let level_count = ((cur_nodes.len() as f64) / (constants::FANOUT as f64)).ceil() as usize;
        let mut i = cur_nodes.len() - 1;
        let mut cur_children = Vec::new();
        let mut cur_keys = Vec::new();

        let mut next_fences = Vec::new();
        let mut next_nodes = Vec::new();
        loop {

            cur_children.push(cur_nodes.remove(i));
            if i % constants::FANOUT == 0 && cur_children.len() > 0 {
                let mut intermediate_node = IntermediateNode::new();

                cur_keys.reverse();
                cur_children.reverse();

                intermediate_node.keys = cur_keys;
                intermediate_node.children = cur_children;

                if i > 0 {
                    next_fences.push(cur_fences[i - 1]);
                }
                next_nodes.push(BTreeNode::Intermediate(Box::new(intermediate_node)));

                cur_keys = Vec::new();
                cur_children = Vec::new();
            } else {
                cur_keys.push(cur_fences[i - 1]);
            }
            if i == 0 {
                break;
            }
            i -= 1;
        }

        next_fences.reverse();
        next_nodes.reverse();
        cur_fences = next_fences;
        cur_nodes = next_nodes;
    }

    let last_node = cur_nodes.remove(0);
    match last_node {
        BTreeNode::Intermediate(node) => node,
        BTreeNode::Leaf(_) => panic!("Got a leaf node at end of building root."),
    }
}


enum InsertResult {
    NoSplit,
    Split(BTreeNode, BTreeNode, i32),
}


#[derive(Clone)]
struct IntermediateNode {
    keys: Vec<i32>,
    children: Vec<BTreeNode>,
}

/**
 * An intermediate node of the B-Tree. Fence pointers, stored in `keys`,
 * contain the *lowest* value in the corresponding child's keys.
 */
impl IntermediateNode {

    fn new() -> IntermediateNode {
        IntermediateNode {
            keys: Vec::with_capacity(constants::FANOUT),
            children: Vec::with_capacity(constants::FANOUT),
        }
    }

    fn find_child(&mut self, key: i32) -> (&mut BTreeNode, usize) {
        assert!(self.children.len() > 0);
        let mut i = 0;
        // Find index of appropriate child
        while i < self.keys.len() {
            if key < self.keys[i] {
                break;
            }
            i += 1;
        }
        (&mut self.children[i], i)
    }

    fn lookup(&mut self, key: i32) -> Result<Option<i32>> {
        if self.children.len() == 0 {
            return Ok(None);
        }
        let (child, _) = self.find_child(key);
        match *child {
            BTreeNode::Intermediate(ref mut node) => node.lookup(key),
            BTreeNode::Leaf(ref mut node) => node.lookup(key),
        }
    }

    fn delete(&mut self, key: i32) -> Result<()> {
        if self.children.len() == 0 {
            return Ok(());
        }
        let mut is_child_empty = false;
        let mut index = 0;
        {
            let (child, i) = self.find_child(key);
            index = i;
            // TODO: figure out how to potentially remove child here
            match *child {
                BTreeNode::Intermediate(ref mut node) => {
                    node.delete(key)?;
                    is_child_empty = node.children.len() == 0
                },
                BTreeNode::Leaf(ref mut node) => {
                    node.delete(key)?;
                    is_child_empty = node.size == 0
                },
            };
        }
        // Remove child if necessary
        if is_child_empty {
            if index > 0 {
                self.keys.remove(index - 1);
            }
            self.children.remove(index);
            // TODO: free file back to OS if deleting a leaf node
        }
        Ok(())
    }

    // Returns two nodes that would result from a split, but
    // doesn't change the node itself.
    fn split(&self) -> (IntermediateNode, IntermediateNode, i32) {
        let num_children = self.children.len();
        assert_eq!(num_children, constants::FANOUT);

        let mut child1 = IntermediateNode::new();
        let mut child2 = IntermediateNode::new();

        child1.children.extend_from_slice(&self.children[0..constants::FANOUT / 2]);
        child2.children.extend_from_slice(&self.children[constants::FANOUT / 2..constants::FANOUT]);

        child1.keys.extend_from_slice(&self.keys[0..(constants::FANOUT / 2 - 1)]);
        child2.keys.extend_from_slice(&self.keys[(constants::FANOUT / 2)..constants::FANOUT - 1]);

        let new_fence = self.keys[constants::FANOUT / 2 - 1];
        (child1, child2, new_fence)
    }

    fn insert_children(&mut self, child1: BTreeNode, child2: BTreeNode, new_fence: i32, index: usize) {
        // Update children
        self.children[index] = child1;
        self.children.insert(index + 1, child2);

        // Update fence pointers
        self.keys.insert(index, new_fence);
    }
    
    fn insert(&mut self, key: i32, val: i32) -> Result<InsertResult> {
        let mut index = 0;
        let mut insert_result = InsertResult::NoSplit;
        {
            let (child, i) = self.find_child(key);
            index = i;
            insert_result =
                match *child {
                    BTreeNode::Intermediate(ref mut node) => {
                        node.insert(key, val)?
                    },
                    BTreeNode::Leaf(ref mut node) => {
                        node.insert(key, val)?
                    },
                };
        }
        match insert_result {
            InsertResult::NoSplit => Ok(InsertResult::NoSplit),
            InsertResult::Split(child1, child2, new_fence) => {
                if self.children.len() == constants::FANOUT {
                    // Split this node
                    let (mut c1, mut c2, return_fence) = self.split();
                    if index < constants::FANOUT / 2 {
                        c1.insert_children(child1, child2, new_fence, index);
                    } else {
                        c2.insert_children(child1, child2, new_fence, index - (constants::FANOUT / 2));
                    }

                    // let return_fence = c2.keys[0];
                    Ok(InsertResult::Split(BTreeNode::Intermediate(Box::new(c1)),
                                           BTreeNode::Intermediate(Box::new(c2)),
                                           return_fence))
                } else {
                    // Add new children
                    self.insert_children(child1, child2, new_fence, index);
                    Ok(InsertResult::NoSplit)
                }
            },
        }
    }

}


#[derive(Clone)]
struct LeafNode {
    location: DiskLocation,
    allocator: Arc<Mutex<SingleFileBufferAllocator>>,
    size: usize,
    next: Option<Box<LeafNode>>,
    prev: Option<Box<LeafNode>>,
}

impl LeafNode {
    
    fn new(location: DiskLocation, allocator: Arc<Mutex<SingleFileBufferAllocator>>) -> LeafNode {
        LeafNode {
            location: location,
            size: 0,
            next: None,
            prev: None,
            allocator: allocator,
        }
    }

    fn split(&mut self) -> Result<(LeafNode, LeafNode, i32)> {
        assert_eq!(self.size, constants::FANOUT);

        let mut allocator = self.allocator.lock().unwrap();
        let loc1 = allocator.allocate(ENTRY_SIZE * constants::FANOUT)?;
        let loc2 = allocator.allocate(ENTRY_SIZE * constants::FANOUT)?;
        // let loc1 = DiskLocation::new(&"asdf".to_string(), 0);
        // let loc2 = DiskLocation::new(&"asdf".to_string(), 0);
        let mut child1 = LeafNode::new(loc1, Arc::clone(&self.allocator));
        let mut child2 = LeafNode::new(loc2, Arc::clone(&self.allocator));
        let mut fence = 0;

        for i in 0..constants::FANOUT {
            let key = self.location.read_int((ENTRY_SIZE * i) as u64)?;
            let val = self.location.read_int((ENTRY_SIZE * i + 4) as u64)?;
            // Write to selected partition
            if i < constants::FANOUT / 2 {
                child1.location.write_int((ENTRY_SIZE * i) as u64, key)?;
                child1.location.write_int((ENTRY_SIZE * i + 4) as u64, val)?;
            } else {
                child2.location.write_int((ENTRY_SIZE * (i - constants::FANOUT / 2)) as u64, key)?;
                child2.location.write_int((ENTRY_SIZE * (i - constants::FANOUT / 2) + 4) as u64, val)?;
            }
            // Save new fence key
            if i == constants::FANOUT / 2 {
                fence = key;
            }
        }
        child1.size = constants::FANOUT / 2;
        child2.size = constants::FANOUT - (constants::FANOUT / 2);
        Ok((child1, child2, fence))
    }

    fn insert(&mut self, key: i32, val: i32) -> Result<InsertResult> {
        if self.size == constants::FANOUT {
            let (mut c1, mut c2, f) = self.split()?;
            if key < f {
                c1.insert(key, val)?;
            } else {
                c2.insert(key, val)?;
            }
            Ok(InsertResult::Split(
                BTreeNode::Leaf(Box::new(c1)),
                BTreeNode::Leaf(Box::new(c2)),
                f))
        } else {
            let mut i = 0;
            // Scan for insertion point
            // TODO: potentially use binary search here
            while i < self.size {
                let read_key = self.location.read_int((ENTRY_SIZE * i) as u64)?;
                if key == read_key {
                    // Replace value and return
                    self.location.write_int((ENTRY_SIZE * i + 4) as u64, val)?;
                    return Ok(InsertResult::NoSplit);
                }
                if key < read_key {
                    break;
                }
                i += 1;
            }
            // Rewrite entries to apply insertion
            let mut next_key = key;
            let mut next_val = val;
            while i < self.size {
                let tmp_key = self.location.read_int((ENTRY_SIZE * i) as u64)?;
                let tmp_val = self.location.read_int((ENTRY_SIZE * i + 4) as u64)?;
                self.location.write_int((ENTRY_SIZE * i) as u64, next_key)?;
                self.location.write_int((ENTRY_SIZE * i + 4) as u64, next_val)?;
                next_key = tmp_key;
                next_val = tmp_val;
                i += 1;
            }
            // Write last entry without reading to prevent error
            self.location.write_int((ENTRY_SIZE * i) as u64, next_key)?;
            self.location.write_int((ENTRY_SIZE * i + 4) as u64, next_val)?;
            // Update size
            self.size += 1;
            Ok(InsertResult::NoSplit)
        }
    }

    fn delete(&mut self, key: i32) -> Result<()> {
        let mut i = 0;
        let mut found = false;
        // Scan for key to delete
        // TODO: potentially use binary search here
        while i < self.size {
            let read_key = self.location.read_int((ENTRY_SIZE * i) as u64)?;
            if key == read_key {
                found = true;
                break;
            }
            i += 1;
        }
        if !found {
            return Ok(())
        }
        // Rewrite entries to apply deletion
        while i < (self.size - 1) {
            let tmp_key = self.location.read_int((ENTRY_SIZE * (i + 1)) as u64)?;
            let tmp_val = self.location.read_int((ENTRY_SIZE * (i + 1) + 4) as u64)?;
            self.location.write_int((ENTRY_SIZE * i) as u64, tmp_key)?;
            self.location.write_int((ENTRY_SIZE * i + 4) as u64, tmp_val)?;
            i += 1;
        }
        // Update size
        self.size -= 1;
        Ok(())
    }

    fn lookup(&self, key: i32) -> Result<Option<i32>> {
        let mut i = 0;
        // Scan for matching key
        while i < self.size {
            let read_key = self.location.read_int((ENTRY_SIZE * i) as u64)?;
            if key == read_key {
                let val = self.location.read_int((ENTRY_SIZE * i + 4) as u64)?;
                return Ok(Some(val));
            }
            i += 1;
        }
        Ok(None)
    }

}


#[derive(Clone)]
enum BTreeNode {
    Intermediate(Box<IntermediateNode>),
    Leaf(Box<LeafNode>),
}


pub struct BTreeOptions {
    // TODO: add fields
}

impl BTreeOptions {

    pub fn new() -> BTreeOptions {
        BTreeOptions { }
    }

}


pub struct BTree {
    disk_allocator: Arc<Mutex<SingleFileBufferAllocator>>,
    options: BTreeOptions,
    root: Box<IntermediateNode>,
}

impl BTree {

    pub fn new(directory: &'static str, options: BTreeOptions) -> Result<BTree> {
        // Create directory if not exists
        create_dir_all(directory)?;

        let root = Box::new(IntermediateNode::new());
        let disk_allocator = Arc::new(Mutex::new(SingleFileBufferAllocator::new(directory)?));
        Ok(BTree {
            disk_allocator: disk_allocator,
            options: options,
            root: root,
        })
    }

    pub fn from_disk_location(disk_location: DiskLocation, fences: Vec<i32>, size: usize,
                              disk_allocator: Arc<Mutex<SingleFileBufferAllocator>>, options: BTreeOptions) -> BTree {
        let root = build_tree_from_run(disk_location, fences, size, &disk_allocator);
        BTree {
            disk_allocator: disk_allocator,
            options: options,
            root: root,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.root.children.len() == 0
    }

    fn allocate_leaf_node(&mut self) -> Result<LeafNode> {
        let mut disk_allocator = self.disk_allocator.lock().unwrap();
        let location = disk_allocator.allocate(ENTRY_SIZE * constants::FANOUT)?;
        Ok(LeafNode::new(location, Arc::clone(&self.disk_allocator)))
    }

}

impl KVStore for BTree {
    
    fn get(&mut self, key: i32) -> Option<i32> {
        self.root.lookup(key).unwrap()
    }

    fn delete(&mut self, key: i32) -> () {
        self.root.delete(key).unwrap();
    }


    fn put(&mut self, key: i32, val: i32) -> () {
        if self.is_empty() {
            let mut leaf_node = self.allocate_leaf_node().unwrap();
            leaf_node.insert(key, val).unwrap();
            self.root.children.push(BTreeNode::Leaf(Box::new(leaf_node)));
        } else {
            let insert_result = (*self.root).insert(key, val).unwrap();
            match insert_result {
                InsertResult::NoSplit => (),
                InsertResult::Split(child1, child2, new_fence) => {
                    let mut new_root = IntermediateNode::new();
                    new_root.children.push(child1);
                    new_root.children.push(child2);

                    new_root.keys.push(new_fence);
                    self.root = Box::new(new_root);
                },
            }
        };
    }

    fn scan(&self, low : i32, high : i32) -> Vec<i32> {
        vec![0, 1, 2, 3]
    }
}