extern crate rand;

use std::boxed::Box;
use std::cell::RefCell;
use std::rc::Rc;
use std::vec::Vec;
use std::io::Result;
use std::fs::create_dir_all;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use self::rand::{thread_rng, Rng};

use constants;
use disk_location::{DiskLocation, FragmentedDiskLocation};
use disk_allocator::{DiskAllocator, SingleFileBufferAllocator};
use kvstore::KVStore;
use lsmtree::{LSMTree, Run};


/* Each BTree entry will contain one unused byte at end to be compatible with LSM-Tree */
const ENTRY_SIZE: usize = (4 + 4 + 1);


fn build_tree_from_run(
    disk_location: Arc<DiskLocation>,
    fences: Vec<i32>,
    size: usize,
    disk_allocator: &Arc<Mutex<SingleFileBufferAllocator>>,
) -> Box<IntermediateNode> {
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
        cur_nodes.push(BTreeNode::Leaf(Rc::new(RefCell::new(leaf_node))));
    }

    if fences.len() == 0 {
        let leaf_disk_location = disk_location.clone();
        let mut leaf_node = LeafNode::new(leaf_disk_location, Arc::clone(disk_allocator));
        leaf_node.size = size;
        cur_nodes.push(BTreeNode::Leaf(Rc::new(RefCell::new(leaf_node))));
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

fn find_key_leaf_location(key: i32, cur_node: &mut BTreeNode) -> Rc<RefCell<LeafNode>> {
    match cur_node {
        BTreeNode::Intermediate(ref mut node) => {
            let (new_node, _) = node.find_child(key);
            find_key_leaf_location(key, new_node)
        },
        BTreeNode::Leaf(ref mut node) => {
            node.clone()
        },
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

#[doc = "
 * An intermediate node of the B-Tree. Fence pointers, stored in `keys`,
 * contain the *lowest* value in the corresponding child's keys.
 * Note: we maintain that `keys.len()` is equal to `children.len() - 1`.
 "]
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
            BTreeNode::Leaf(ref mut node) => { node.borrow_mut().lookup(key) },
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

            match *child {
                BTreeNode::Intermediate(ref mut node) => {
                    node.delete(key)?;
                    is_child_empty = node.children.len() == 0
                }
                BTreeNode::Leaf(ref mut node) => {
                    let mut node_ref = node.borrow_mut();
                    node_ref.delete(key)?;
                    is_child_empty = node_ref.size == 0
                }
            };
        }
        // Remove child if necessary
        if is_child_empty {
            if self.keys.len() > 0 {
                let remove_idx = if index > 0 { index - 1 } else { 0 };
                self.keys.remove(remove_idx);
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

        child1.children.extend_from_slice(
            &self.children[0..constants::FANOUT / 2],
        );
        child2.children.extend_from_slice(
            &self.children[constants::FANOUT / 2..
                               constants::FANOUT],
        );

        child1.keys.extend_from_slice(
            &self.keys[0..(constants::FANOUT / 2 - 1)],
        );
        child2.keys.extend_from_slice(
            &self.keys[(constants::FANOUT / 2)..
                           constants::FANOUT - 1],
        );

        let new_fence = self.keys[constants::FANOUT / 2 - 1];
        (child1, child2, new_fence)
    }

    fn insert_children(
        &mut self,
        child1: BTreeNode,
        child2: BTreeNode,
        new_fence: i32,
        index: usize,
    ) {
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
            insert_result = match *child {
                BTreeNode::Intermediate(ref mut node) => node.insert(key, val)?,
                BTreeNode::Leaf(ref mut node) => node.borrow_mut().insert(key, val)?,
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
                        c2.insert_children(
                            child1,
                            child2,
                            new_fence,
                            index - (constants::FANOUT / 2),
                        );
                    }

                    Ok(InsertResult::Split(
                        BTreeNode::Intermediate(Box::new(c1)),
                        BTreeNode::Intermediate(Box::new(c2)),
                        return_fence,
                    ))
                } else {
                    // Add new children
                    self.insert_children(child1, child2, new_fence, index);
                    Ok(InsertResult::NoSplit)
                }
            }
        }
    }
}


#[derive(Clone)]
struct LeafNode {
    location: Arc<DiskLocation>,
    allocator: Arc<Mutex<SingleFileBufferAllocator>>,
    size: usize,
    next: Option<Rc<RefCell<LeafNode>>>,
    prev: Option<Rc<RefCell<LeafNode>>>,
    id: u16,
}

impl LeafNode {
    fn new(
        location: Arc<DiskLocation>,
        allocator: Arc<Mutex<SingleFileBufferAllocator>>,
    ) -> LeafNode {
        LeafNode {
            location: location,
            size: 0,
            next: None,
            prev: None,
            allocator: allocator,
            id: thread_rng().gen::<u16>(),
        }
    }

    fn split(&mut self) -> Result<(Rc<RefCell<LeafNode>>, Rc<RefCell<LeafNode>>, i32)> {
        assert_eq!(self.size, constants::FANOUT);

        let mut allocator = self.allocator.lock().unwrap();
        let loc1 = allocator.allocate(ENTRY_SIZE * constants::FANOUT)?;
        let loc2 = allocator.allocate(ENTRY_SIZE * constants::FANOUT)?;
        let child1 = Rc::new(RefCell::new(
            LeafNode::new(Arc::new(loc1), Arc::clone(&self.allocator)),
        ));
        let child2 = Rc::new(RefCell::new(
            LeafNode::new(Arc::new(loc2), Arc::clone(&self.allocator)),
        ));

        /* adjust next and previous references to this node */
        match self.prev {
            Some(ref node) => { node.borrow_mut().next = Some(child1.clone()); },
            None => {},
        }
        match self.next {
            Some(ref node) => { node.borrow_mut().prev = Some(child2.clone()); },
            None => {},
        }

        let mut fence = 0;
        {
            let mut child1_ref = child1.borrow_mut();
            let mut child2_ref = child2.borrow_mut();

            /* Set up next and prev pointers */
            child1_ref.prev = self.prev.clone();
            child1_ref.next = Some(child2.clone());
            child2_ref.prev = Some(child1.clone());
            child2_ref.next = self.next.clone();

            for i in 0..constants::FANOUT {
                let key = self.location.read_int((ENTRY_SIZE * i) as u64)?;
                let val = self.location.read_int((ENTRY_SIZE * i + 4) as u64)?;
                // Write to selected partition
                if i < constants::FANOUT / 2 {
                    child1_ref.location.write_int((ENTRY_SIZE * i) as u64, key)?;
                    child1_ref.location.write_int(
                        (ENTRY_SIZE * i + 4) as u64,
                        val,
                    )?;
                } else {
                    child2_ref.location.write_int(
                        (ENTRY_SIZE * (i - constants::FANOUT / 2)) as
                            u64,
                        key,
                    )?;
                    child2_ref.location.write_int(
                        (ENTRY_SIZE * (i - constants::FANOUT / 2) +
                             4) as u64,
                        val,
                    )?;
                }
                // Save new fence key
                if i == constants::FANOUT / 2 {
                    fence = key;
                }
            }
            child1_ref.size = constants::FANOUT / 2;
            child2_ref.size = constants::FANOUT - (constants::FANOUT / 2);

            child1_ref.location.write_byte((ENTRY_SIZE * child1_ref.size - 1) as u64, 0)?;
            child2_ref.location.write_byte((ENTRY_SIZE * child2_ref.size - 1) as u64, 0)?;
        }
        Ok((child1, child2, fence))
    }

    fn insert(&mut self, key: i32, val: i32) -> Result<InsertResult> {
        if self.size == constants::FANOUT {
            let (mut c1, mut c2, f) = self.split()?;
            {
                let mut c1_ref = c1.borrow_mut();
                let mut c2_ref = c2.borrow_mut();
                if key < f {
                    c1_ref.insert(key, val)?;
                } else {
                    c2_ref.insert(key, val)?;
                }
            }
            Ok(InsertResult::Split(
                BTreeNode::Leaf(c1),
                BTreeNode::Leaf(c2),
                f,
            ))
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
                self.location.write_int(
                    (ENTRY_SIZE * i + 4) as u64,
                    next_val,
                )?;
                next_key = tmp_key;
                next_val = tmp_val;
                i += 1;
            }
            // Write last entry without reading to prevent error
            self.location.write_int((ENTRY_SIZE * i) as u64, next_key)?;
            self.location.write_int(
                (ENTRY_SIZE * i + 4) as u64,
                next_val,
            )?;
            self.location.write_byte((ENTRY_SIZE * i + 4 + 4) as u64, 0)?;
            // Update size
            self.size += 1;
            Ok(InsertResult::NoSplit)
        }
    }

    fn write_all(&mut self, entries: &[(i32, i32)]) -> Result<()> {
        let converted_entries : Vec<(i32, i32, bool)> = (0..entries.len()).map(|i| (entries[i].0, entries[i].1, false)).collect();
        self.location.write_entries(0, &converted_entries);
        // // TODO: make more efficient
        // for i in 0..entries.len() {
        //     self.location.write_int((ENTRY_SIZE * i) as u64, entries[i].0)?;
        //     self.location.write_int(
        //         (ENTRY_SIZE * i + 4) as u64,
        //         entries[i].1,
        //     )?;
        // }
        self.size = entries.len();
        Ok(())
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
            return Ok(());
        }
        // Rewrite entries to apply deletion
        while i < (self.size - 1) {
            let tmp_key = self.location.read_int((ENTRY_SIZE * (i + 1)) as u64)?;
            let tmp_val = self.location.read_int((ENTRY_SIZE * (i + 1) + 4) as u64)?;
            self.location.write_int((ENTRY_SIZE * i) as u64, tmp_key)?;
            self.location.write_int(
                (ENTRY_SIZE * i + 4) as u64,
                tmp_val,
            )?;
            i += 1;
        }
        // Update size
        self.size -= 1;
        Ok(())
    }

    fn lookup(&self, key: i32) -> Result<Option<i32>> {
        // let start = Instant::now();
        let entries = self.location.read_entries(0, self.size)?;
        // let nanos = start.elapsed().as_nanos();
        // println!("{} {:?}", self.size, nanos);
        // Binary search time
        let mut lo = 0;
        let mut hi = self.size - 1;
        while lo <= hi {
            let mid = (lo + hi) / 2;
            // let read_key = self.location.read_int((ENTRY_SIZE * mid) as u64)?;
            let read_key = entries[mid].0;
            if read_key > key {
                // Prevent underflow
                if mid == 0 {
                    break;
                }
                hi = mid - 1;
            } else if read_key < key {
                lo = mid + 1;
            } else {
                // let val = self.location.read_int((ENTRY_SIZE * mid + 4) as u64)?;
                let val = entries[mid].1;
                return Ok(Some(val))
            }

        }

        Ok(None)
    }

    fn get_entry(&self, index: usize) -> Result<(i32, i32)> {
        let key = self.location.read_int((ENTRY_SIZE * index) as u64)?;
        let val = self.location.read_int((ENTRY_SIZE * index + 4) as u64)?;
        Ok((key, val))
    }
}


#[derive(Clone)]
enum BTreeNode {
    Intermediate(Box<IntermediateNode>),
    Leaf(Rc<RefCell<LeafNode>>),
}


pub struct BTreeOptions {
    // TODO: add fields
}

impl BTreeOptions {
    pub fn new() -> BTreeOptions {
        BTreeOptions {}
    }
}


pub struct BTree {
    disk_allocator: Arc<Mutex<SingleFileBufferAllocator>>,
    options: BTreeOptions,
    root: Box<IntermediateNode>,
    last_leaf: Option<Rc<RefCell<LeafNode>>>,
}

impl BTree {
    pub fn new(directory: &str, options: BTreeOptions) -> Result<BTree> {
        // Create directory if not exists
        create_dir_all(directory)?;

        let root = Box::new(IntermediateNode::new());
        let disk_allocator = Arc::new(Mutex::new(SingleFileBufferAllocator::new(directory)?));
        Ok(BTree {
            disk_allocator: disk_allocator,
            options: options,
            root: root,
            last_leaf: None,
        })
    }

    pub fn from_disk_location(
        disk_location: Arc<DiskLocation>,
        fences: Vec<i32>,
        size: usize,
        disk_allocator: Arc<Mutex<SingleFileBufferAllocator>>,
        options: BTreeOptions,
    ) -> BTree {
        let root = build_tree_from_run(disk_location, fences, size, &disk_allocator);
        BTree {
            disk_allocator: disk_allocator,
            options: options,
            root: root,
            last_leaf: None,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.root.children.len() == 0
    }

    fn allocate_leaf_node(&mut self) -> Result<LeafNode> {
        let mut disk_allocator = self.disk_allocator.lock().unwrap();
        let location = disk_allocator.allocate(ENTRY_SIZE * constants::FANOUT)?;
        Ok(LeafNode::new(
            Arc::new(location),
            Arc::clone(&self.disk_allocator),
        ))
    }

    pub fn into_lsm_tree(self, directory: &str) -> LSMTree {
        /* Find lowest left leaf node */
        let mut cur_node = BTreeNode::Intermediate(self.root);
        loop {
            match cur_node {
                BTreeNode::Intermediate(node) => {
                    cur_node = node.children[0].clone();
                },
                BTreeNode::Leaf(node) => {
                    cur_node = BTreeNode::Leaf(node);
                    break;
                },
            }
        }

        let mut cur_offset: u64 = 0;
        let mut disk_locations = Vec::new();
        let mut offset_fences = Vec::new();
        let mut cur_leaf = match cur_node {
            BTreeNode::Leaf(node) => Some(node),
            _ => None,
        };

        loop {
            match cur_leaf {
                Some(node) => {
                    let mut node_ref = node.borrow_mut();
                    disk_locations.push(node_ref.location.clone());
                    cur_offset += (node_ref.size * ENTRY_SIZE) as u64;
                    offset_fences.push(cur_offset);
                    cur_leaf = node_ref.next.clone();
                }
                None => break,
            }
        }

        // Remove last fence because it's unnecessary
        let new_len = offset_fences.len() - 1;
        offset_fences.truncate(new_len);

        let frag_location = Arc::new(FragmentedDiskLocation::new(offset_fences, disk_locations));
        let mut base_level = Run::new(frag_location, cur_offset as usize);
        base_level.construct_bloom_and_fences();
        LSMTree::from_run(base_level, directory)
    }

    fn print_node(&self, node: &BTreeNode) {
        println!("\n============================");
        match node {
            BTreeNode::Intermediate(ref int_node) => {
                println!("INTERMEDIATE");
                println!("{:?} children", int_node.children.len());
                for ref f in &int_node.keys {
                    println!("\t{:?}", f);
                }
                println!("============================");
                for ref c in &int_node.children {
                    self.print_node(c);
                }
                println!("END INTERMEDIATE");
            },
            BTreeNode::Leaf(ref leaf_node) => {
                let leaf_node_ref = leaf_node.borrow_mut();
                println!("LEAF {}", leaf_node_ref.id);
                println!("size = {}", leaf_node_ref.size);
                let next_id = match leaf_node_ref.next {
                    Some(ref node) => Some(node.borrow_mut().id),
                    None => None,
                };
                let prev_id = match leaf_node_ref.prev {
                    Some(ref node) => Some(node.borrow_mut().id),
                    None => None,
                };
                println!("prev: {:?}", prev_id);
                println!("next: {:?}", next_id);
                println!("============================");
            },
        }
    }

    pub fn debug_print(&self) {
        let num_children = self.root.children.len();
        for i in 0..num_children {
            if i > 0 {
                println!("ROOT KEY {}", self.root.keys[i-1]);
            }
            self.print_node(&self.root.children[i]);
        }
    }


    pub fn insert_batch_right(&mut self, batch: &[(i32, i32)]) {
        if batch.len() == 0 {
            return;
        }

        let fence_val = batch[0].0;
        let mut leaf_node = self.allocate_leaf_node().unwrap();
        leaf_node.write_all(&batch).unwrap();

        // Update leaf linked-list
        leaf_node.prev =
            match &self.last_leaf {
                Some(ref val) => Some(Rc::clone(val)),
                None => None,
            };
        leaf_node.next = None;
        
        let leaf_node = Rc::new(RefCell::new(leaf_node));
        match self.last_leaf {
            Some(ref node) => node.borrow_mut().next = Some(Rc::clone(&leaf_node)),
            None => (),
        }
        self.last_leaf = Some(Rc::clone(&leaf_node));
        let leaf_node = BTreeNode::Leaf(leaf_node);

        // Recursive helper function
        fn insert_leaf_node_right(node: &mut BTreeNode, leaf_node: BTreeNode, fence_val: i32) -> Option<BTreeNode> {
            match node {
                BTreeNode::Intermediate(int_node) => {
                    let num_children = int_node.children.len();
                    let insert_node = insert_leaf_node_right(
                        &mut int_node.children[num_children - 1],
                        leaf_node,
                        fence_val);
                    match insert_node {
                        Some(insert_node) => {
                            if int_node.children.len() < constants::FANOUT {
                                int_node.children.push(insert_node);
                                int_node.keys.push(fence_val);
                                None
                            } else {
                                Some(insert_node)
                            }
                        },
                        None => None,
                    }
                },
                BTreeNode::Leaf(_) => Some(leaf_node),
            }
        };

        // Update starting at root
        // This is complicated since we want the tree to be
        // as balanced as possible.
        if self.is_empty() {
            let mut new_intermediate_child = IntermediateNode::new();
            new_intermediate_child.children.push(leaf_node);
            self.root.children.push(
                BTreeNode::Intermediate(Box::new(new_intermediate_child)));
        } else {
            let num_children = self.root.children.len();
            let insert_node = insert_leaf_node_right(
                &mut self.root.children[num_children - 1],
                leaf_node,
                fence_val);
            match insert_node {
                Some(insert_node) => {
                    if self.root.children.len() < constants::FANOUT {
                        let mut new_intermediate_child = IntermediateNode::new();
                        new_intermediate_child.children.push(insert_node);
                        self.root.children.push(
                            BTreeNode::Intermediate(Box::new(new_intermediate_child)));
                        self.root.keys.push(fence_val);
                    } else {
                        let mut new_intermediate_child1 = IntermediateNode::new();
                        new_intermediate_child1.children = self.root.children
                            .drain(0..constants::FANOUT).collect();
                        new_intermediate_child1.keys = self.root.keys
                            .drain(0..constants::FANOUT - 1).collect();

                        self.root.children = vec![
                            BTreeNode::Intermediate(Box::new(new_intermediate_child1)),
                            insert_node];
                        self.root.keys = vec![fence_val];
                    }
                },
                None => (),
            }
        }
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
            self.root.children.push(BTreeNode::Leaf(
                Rc::new(RefCell::new(leaf_node)),
            ));
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
                }
            }
        };
    }

    fn scan(&mut self, low: i32, high: i32) -> Vec<i32> {
        let mut leaf_node = find_key_leaf_location(low, self.root.find_child(low).0);
        let mut list = Vec::new();

        loop {
            let mut next_leaf_node = None;
            {
                let mut cur_index = 0;
                let leaf_node_ref = leaf_node.borrow_mut();
                let entries = leaf_node_ref.location.read_entries(0, leaf_node_ref.size).unwrap();
                loop {
                    // let (key, val) = leaf_node_ref.get_entry(cur_index).unwrap();
                    let (key, val, _) = entries[cur_index];
                    if key >= high {
                        break;
                    }
                    if key >= low {
                        list.push(val);
                    }
                    cur_index += 1;
                    if cur_index >= leaf_node_ref.size {
                        next_leaf_node = leaf_node_ref.next.clone();
                        break;
                    }
                }
            }
            match next_leaf_node {
                Some(node) => {
                    leaf_node = node.clone();
                },
                None => {
                    break;
                },
            }
        }

        list
    }

    fn debug_lookup(&mut self, key: i32) {
        // Unimplemented
    }
}
