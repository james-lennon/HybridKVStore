use std::boxed::Box;
use std::vec::Vec;
use std::io::Result;

use disk_location::DiskLocation;
use disk_allocator::{DiskAllocator, SingleFileBufferAllocator};


// TODO: pick a more exact value for this
const FANOUT : usize = 1500;


struct IntermediateNode {
    keys: Vec<i32>,
    children: Vec<BTreeNode>,
}

impl IntermediateNode {

    fn new() -> IntermediateNode {
        IntermediateNode {
            keys: Vec::with_capacity(FANOUT),
            children: Vec::with_capacity(FANOUT),
        }
    }

}


struct LeafNode {
    keys: Vec<i32>,
    location: DiskLocation,
}


enum BTreeNode {
    Intermediate(Box<IntermediateNode>),
    Leaf(Box<LeafNode>),
}


pub struct BTreeOptions {
    // TODO: add fields
}

impl BTreeOptions {

    fn new() -> BTreeOptions {
        BTreeOptions { }
    }

}


pub struct BTree {
    directory: &'static str,
    disk_allocator: SingleFileBufferAllocator,
    options: BTreeOptions,
    root: BTreeNode,
}

impl BTree {

    fn new(directory: &'static str, options: BTreeOptions) -> Result<BTree> {
        let root = BTreeNode::Intermediate(Box::new(IntermediateNode::new()));
        let disk_allocator = SingleFileBufferAllocator::new(directory)?;
        Ok(BTree {
            directory: directory,
            disk_allocator: disk_allocator,
            options: options,
            root: root,
        })
    }

}

// impl KVStore for BTree {
    
// }