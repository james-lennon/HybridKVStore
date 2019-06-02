use std::io::Result;
use std::fs::File;
use std::sync::Arc;
use disk_location::{DiskLocation, ContiguousDiskLocation};


pub trait DiskAllocator {
    fn allocate(&mut self, size: usize) -> Result<ContiguousDiskLocation>;
}


#[derive(Clone, Debug)]
pub struct SingleFileBufferAllocator {
    filename: Box<String>,
    cur_offset: usize,
}

impl SingleFileBufferAllocator {
    
    pub fn new(directory: &str) -> Result<SingleFileBufferAllocator> {
        let filename = format!("{}/{}", directory.to_string(), "file_buffer.data".to_string());
        let file = File::create(&filename)?;
        Ok(SingleFileBufferAllocator {
            filename: Box::new(filename),
            cur_offset: 0,
        })
    }

}

impl DiskAllocator for SingleFileBufferAllocator {

    fn allocate(&mut self, size: usize) -> Result<ContiguousDiskLocation> {
        let prev_offset = self.cur_offset;
        self.cur_offset += size;
        Ok(ContiguousDiskLocation::new(&*self.filename, prev_offset as u64))
    }

}