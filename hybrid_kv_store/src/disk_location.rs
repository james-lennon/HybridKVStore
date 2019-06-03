extern crate byteorder;

use std::cmp::Ordering;
use std::vec::Vec;
use std::io;
use std::io::{Result, Read};
use std::fs::{File, OpenOptions};
use std::fmt::Debug;
use std::io::SeekFrom;
use std::io::Seek;
use std::io::Write;
use std::sync::Arc;

use self::byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};


fn read_bytes_from_file(file: &mut File, offset: u64, count: usize) -> Result<Vec<u8>> {
    let mut buffer = vec![0; count];
    file.seek(SeekFrom::Start(offset))?;
    file.read(&mut buffer)?;
    Ok(buffer)
}

fn read_ints_from_file(file: &mut File, offset: u64, count: usize) -> Result<Vec<i32>> {
    let mut buffer = vec![0; count];
    file.seek(SeekFrom::Start(offset))?;
    file.read_i32_into::<BigEndian>(&mut buffer)?;
    Ok(buffer)
}

fn read_int_from_file(file: &mut File, offset: u64) -> Result<i32> {
    file.seek(SeekFrom::Start(offset))?;
    file.read_i32::<BigEndian>()
}

fn write_byte_to_file(file: &mut File, offset: u64, val: u8) -> Result<()> {
    file.seek(SeekFrom::Start(offset))?;
    file.write(&[val])?;
    Ok(())
}

fn write_int_to_file(file: &mut File, offset: u64, val: i32) -> Result<()> {
    file.seek(SeekFrom::Start(offset))?;
    file.write_i32::<BigEndian>(val)?;
    Ok(())
}

fn write_ints_to_file(file: &mut File, offset: u64, buffer: &[i32]) -> Result<()> {
    file.seek(SeekFrom::Start(offset))?;
    for v in buffer {
        file.write_i32::<BigEndian>(*v)?;
    }
    Ok(())
}


pub trait DiskLocation: Debug {
    fn read_int(&self, offset: u64) -> Result<i32>;
    fn read_byte(&self, offset: u64) -> Result<u8>;
    fn write_int(&self, offset: u64, value: i32) -> Result<()>;
    fn write_byte(&self, offset: u64, value: u8) -> Result<()>;
    fn clone_with_offset(&self, offset: u64) -> Arc<DiskLocation>;
}


#[derive(Clone, Debug)]
pub struct ContiguousDiskLocation {
    filename: String,
    offset: u64,
}

impl ContiguousDiskLocation {
    pub fn new(filename: &String, offset: u64) -> ContiguousDiskLocation {
        ContiguousDiskLocation {
            filename: filename.clone(),
            offset: offset,
        }
    }

    pub fn read_ints(&self, offset: u64, count: usize) -> Result<Vec<i32>> {
        let mut file = File::open(&self.filename)?;
        read_ints_from_file(&mut file, offset + self.offset, count)
    }

    pub fn write_all(&self, offset: u64, values: &[i32]) -> Result<()> {
        let mut file = File::open(&self.filename)?;
        write_ints_to_file(&mut file, offset + self.offset, values)
    }

    pub fn append(&self, offset: u64, value: i32) -> Result<()> {
        let mut file = File::open(&self.filename)?;
        write_int_to_file(&mut file, offset + self.offset, value)
    }

    pub fn append_all(&self, offset: u64, values: &[i32]) -> Result<()> {
        let mut file = File::open(&self.filename)?;
        write_ints_to_file(&mut file, offset + self.offset, values)
    }

    pub fn get_offset(&self) -> u64 {
        self.offset
    }
}

impl DiskLocation for ContiguousDiskLocation {
    fn read_int(&self, offset: u64) -> Result<i32> {
        let mut file = File::open(&self.filename)?;
        read_int_from_file(&mut file, offset + self.offset)
    }


    fn read_byte(&self, offset: u64) -> Result<u8> {
        let mut file = File::open(&self.filename)?;
        Ok(read_bytes_from_file(&mut file, offset + self.offset, 1)?[0])
    }


    fn write_int(&self, offset: u64, value: i32) -> Result<()> {
        let mut file = OpenOptions::new().write(true).read(true).open(
            &self.filename,
        )?;
        write_int_to_file(&mut file, offset + self.offset, value)
    }


    fn write_byte(&self, offset: u64, value: u8) -> Result<()> {
        let mut file = OpenOptions::new().write(true).read(true).open(
            &self.filename,
        )?;
        write_byte_to_file(&mut file, offset + self.offset, value)
    }

    fn clone_with_offset(&self, offset: u64) -> Arc<DiskLocation> {
        Arc::new(ContiguousDiskLocation {
            filename: self.filename.clone(),
            offset: self.offset + offset,
        })
    }
}


#[derive(Debug)]
struct FragmentedDiskLocation {
    offset_fences: Vec<u64>,
    disk_locations: Vec<Arc<DiskLocation>>,
}

impl FragmentedDiskLocation {
    pub fn new(
        offset_fences: Vec<u64>,
        disk_locations: Vec<Arc<DiskLocation>>,
    ) -> FragmentedDiskLocation {

        FragmentedDiskLocation {
            offset_fences: offset_fences,
            disk_locations: disk_locations,
        }
    }

    fn fragment_for_offset(&self, offset: u64) -> usize {
        let mut index = 0;
        while index < self.offset_fences.len() {
            if offset < self.offset_fences[index] {
                break;
            }
        }
        index
    }
}

impl DiskLocation for FragmentedDiskLocation {
    fn read_int(&self, offset: u64) -> Result<i32> {
        let index = self.fragment_for_offset(offset);
        let disk_location = &self.disk_locations[index];
        let read_offset =
            if index > 0 { offset - self.offset_fences[index - 1]}
            else { offset };
        disk_location.read_int(read_offset)
    }


    fn read_byte(&self, offset: u64) -> Result<u8> {
        let index = self.fragment_for_offset(offset);
        let disk_location = &self.disk_locations[index];
        let read_offset =
            if index > 0 { offset - self.offset_fences[index - 1]}
            else { offset };
        disk_location.read_byte(read_offset)
    }


    fn write_int(&self, offset: u64, value: i32) -> Result<()> {
        panic!("Writing not implemented on fragmented disk locations.");
    }


    fn write_byte(&self, offset: u64, value: u8) -> Result<()> {
        panic!("Writing not implemented on fragmented disk locations.");
    }

    fn clone_with_offset(&self, offset: u64) -> Arc<DiskLocation> {
        // Implementing this is necessary to handle an edge case where
        // a transition is reversed shortly after it is executed.
        // Not implemented for now.
        panic!("Cloning with offset not implemented yet on fragmented disk locations.");
        // Arc::new(FragmentedDiskLocation {
        //     filename: self.fences.clone(),
        //     offset: self.offset + offset,
        // })
    }
}