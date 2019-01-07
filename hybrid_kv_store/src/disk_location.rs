extern crate byteorder;

use std::vec::Vec;
use std::io;
use std::io::Read;
use std::fs::{File, OpenOptions};
use std::io::SeekFrom;
use std::io::Seek;

use self::byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};


fn read_bytes_from_file(file: &mut File, offset: u64, count: usize) -> io::Result<Vec<u8>> {
    let mut buffer = vec![0; count];
    file.seek(SeekFrom::Start(offset))?;
    file.read(&mut buffer)?;
    Ok(buffer)
}

fn read_ints_from_file(file: &mut File, offset: u64, count: usize) -> io::Result<Vec<i32>> {
    let mut buffer = vec![0; count];
    file.seek(SeekFrom::Start(offset))?;
    file.read_i32_into::<BigEndian>(&mut buffer)?;
    Ok(buffer)
}

fn read_int_from_file(file: &mut File, offset: u64) -> io::Result<i32> {
    file.seek(SeekFrom::Start(offset))?;
    file.read_i32::<BigEndian>()
}


fn write_int_to_file(file: &mut File, offset: u64, val: i32) -> io::Result<()> {
    file.seek(SeekFrom::Start(offset))?;
    file.write_i32::<BigEndian>(val)?;
    Ok(())
}

fn write_ints_to_file(file: &mut File, offset: u64, buffer: &[i32]) -> io::Result<()> {
    file.seek(SeekFrom::Start(offset))?;
    for v in buffer {
        file.write_i32::<BigEndian>(*v)?;
    }
    Ok(())
}


#[derive(Clone, Debug)]
pub struct DiskLocation {
    filename: String,
    offset: u64,
}

impl DiskLocation {
    
    pub fn new(filename: &String, offset: u64) -> DiskLocation {
        DiskLocation {
            filename: filename.clone(),
            offset: offset,
        }
    }

    pub fn read_ints(&self, offset: u64, count: usize) -> io::Result<Vec<i32>> {
        let mut file = File::open(&self.filename)?;
        read_ints_from_file(&mut file, offset + self.offset, count)
    }

    pub fn read_int(&self, offset: u64) -> io::Result<i32> {
        let mut file = File::open(&self.filename)?;
        read_int_from_file(&mut file, offset + self.offset)
    }

    pub fn read_byte(&self, offset: u64) -> io::Result<u8> {
        let mut file = File::open(&self.filename)?;
        Ok(read_bytes_from_file(&mut file, offset + self.offset, 1)?[0])
    }

    pub fn write(&self, offset: u64, value: i32) -> io::Result<()> {
        let mut file = OpenOptions::new().write(true).read(true).open(&self.filename)?;
        write_int_to_file(&mut file, offset + self.offset, value)
    }

    pub fn write_all(&self, offset: u64, values: &[i32]) -> io::Result<()> {
        let mut file = File::open(&self.filename)?;
        write_ints_to_file(&mut file, offset + self.offset, values)
    }

    pub fn append(&self, offset: u64, value: i32) -> io::Result<()> {
        let mut file = File::open(&self.filename)?;
        write_int_to_file(&mut file, offset + self.offset, value)
    }

    pub fn append_all(&self, offset: u64, values: &[i32]) -> io::Result<()> {
        let mut file = File::open(&self.filename)?;
        write_ints_to_file(&mut file, offset + self.offset, values)
    }

}
