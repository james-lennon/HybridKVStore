use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

#[derive(Clone, Debug)]
pub struct BitVector {
    size: usize,
    data: Vec<u8>,
}

fn vector_bit_positions(index: usize) -> (usize, usize) {
    (index / 8, index % 8)
}

impl BitVector {
    pub fn new(size: usize) -> BitVector {
        let mut data = Vec::with_capacity(size);
        for _ in 0..size {
            data.push(0);
        }
        BitVector {
            size: size,
            data: data,
        }
    }

    pub fn set(&mut self, index: usize) {
        let pos = vector_bit_positions(index);
        self.data[pos.0] |= 1 << pos.1;
    }

    pub fn unset(&mut self, index: usize) {
        let pos = vector_bit_positions(index);
        self.data[pos.0] &= !(1 << pos.1);
    }

    pub fn get(&self, index: usize) -> bool {
        let pos = vector_bit_positions(index);
        (self.data[pos.0] & 1 << pos.1) != 0
    }
}

#[derive(Clone, Debug)]
pub struct BloomFilter {
    bit_vector: BitVector,
    count: usize,
}


impl BloomFilter {
    pub fn new(capacity: usize) -> BloomFilter {
        BloomFilter {
            bit_vector: BitVector::new(capacity),
            count: 0,
        }
    }

    fn get_position<T: Hash>(&self, value: &T) -> usize {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        (hasher.finish() as usize) % self.bit_vector.size
    }

    pub fn add<T: Hash>(&mut self, value: &T) {
        let pos = self.get_position(value);
        self.bit_vector.set(pos);
        self.count += 1;
    }

    pub fn get<T: Hash>(&self, value: &T) -> bool {
        self.bit_vector.get(self.get_position(value))
    }

    pub fn clear(&mut self) {
        if self.count > 0 {
            for i in 0..self.bit_vector.size {
                self.bit_vector.data[i] = 0;
            }
        }
    }

    pub fn len(&self) -> usize {
        self.count
    }
}
