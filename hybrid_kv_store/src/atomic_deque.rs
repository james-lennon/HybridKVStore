use std::ops::Index;
use std::sync::atomic::{AtomicUsize, Ordering};


#[derive(Debug)]
pub struct AtomicDeque<T> where T: Clone {
    buffer: Vec<T>,
    start: AtomicUsize,
    end: AtomicUsize,
}

impl<T> AtomicDeque<T> where T: Clone {

    pub fn with_capacity(capacity: usize, default_value: T) -> AtomicDeque<T> {
        AtomicDeque {
            buffer: vec![default_value; capacity + 1],
            start: AtomicUsize::new(0),
            end: AtomicUsize::new(0),
        }
    }

    pub fn capacity(&self) -> usize {
        self.buffer.len()
    }

    pub fn len(&self) -> usize {
        let start = self.start.load(Ordering::Relaxed) as i16;
        let end = self.end.load(Ordering::Relaxed) as i16;
        if end >= start {
            return (end - start) as usize;
        } else {
            return (self.capacity() - (start - end) as usize) as usize;
        }
    }
    
    pub fn push(&mut self, val: T) {
        if self.len() >= self.buffer.len() {
            panic!("Attempted to push into full atomic buffer; resizing not yet supported.");
        }
        let old_end = self.end.load(Ordering::Acquire);
        self.buffer[old_end] = val;
        self.end.store((old_end + 1) % self.capacity(), Ordering::Release);
    }

    pub fn drop_first(&mut self, n: usize) {
        let start = self.start.load(Ordering::Acquire);
        self.start.store((start + n) % self.capacity(), Ordering::Release);
    }

    pub fn clone_contents_into(&self, dest: &mut Vec<T>) {
        let start = self.start.load(Ordering::Relaxed);
        let end = self.end.load(Ordering::Relaxed);

        let mut i : usize = start;
        while i != end {
            dest.push(self.buffer[i].clone());
            i = (i + 1) % self.capacity();
        }
    }

}

impl<T> Index<usize> for AtomicDeque<T> where T: Clone {
    
    type Output = T;

    fn index(&self, i: usize) -> &T {
        let pos = (self.start.load(Ordering::Relaxed) + i) % self.capacity();
        &self.buffer[pos]
    }

}