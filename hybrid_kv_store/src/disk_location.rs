extern crate byteorder;

use std::vec::Vec;
use std::io;
use std::io::Read;
use std::fs::File;
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


#[derive(Debug)]
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

    pub fn read(&self, offset: u64, count: usize) -> io::Result<Vec<i32>> {
        let mut file = File::open(&self.filename)?;
        read_ints_from_file(&mut file, offset + self.offset, count)
    }

    pub fn write(&self, offset: u64, value: i32) -> io::Result<()> {
        let mut file = File::open(&self.filename)?;
        write_int_to_file(&mut file, offset + self.offset, value)
    }

    pub fn write_all(&self, offset: u64, values: &[i32]) -> io::Result<()> {
        let mut file = File::open(&self.filename)?;
        write_ints_to_file(&mut file, offset + self.offset, values)
    }

}
