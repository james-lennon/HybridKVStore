use std::ops::{Index, IndexMut};
use std::sync::atomic::{AtomicUsize, Ordering};


#[derive(Debug)]
pub struct AtomicDeque<T>
where
    T: Clone,
{
    buffer: Vec<T>,
    start: AtomicUsize,
    end: AtomicUsize,
    capacity: usize,
}

impl<T> AtomicDeque<T>
where
    T: Clone,
{
    pub fn with_capacity(capacity: usize, default_value: T) -> AtomicDeque<T> {
        AtomicDeque {
            buffer: vec![default_value; capacity + 1],
            capacity: capacity + 1,
            start: AtomicUsize::new(0),
            end: AtomicUsize::new(0),
        }
    }

    pub fn len(&self) -> usize {
        let start = self.start.load(Ordering::Relaxed) as usize;
        let end = self.end.load(Ordering::Relaxed) as usize;
        if end >= start {
            return (end - start) as usize;
        } else {
            return (self.capacity - (start - end) as usize) as usize;
        }
    }

    pub fn push(&mut self, val: T) {
        assert!(self.len() < self.buffer.len());
        let old_end = self.end.load(Ordering::Acquire);
        self.buffer[old_end] = val;
        self.end.store(
            (old_end + 1) % self.capacity,
            Ordering::Release,
        );
    }

    pub fn drop_first(&mut self, n: usize) {
        let start = self.start.load(Ordering::Acquire);
        self.start.store(
            (start + n) % self.capacity,
            Ordering::Release,
        );
    }

    pub fn truncate(&mut self, n: usize) {
        let start = self.start.load(Ordering::Acquire);
        self.end.store(
            (start + n) % self.capacity,
            Ordering::Release,
        );
    }

    pub fn clone_contents_into(&self, dest: &mut Vec<T>) {
        let start = self.start.load(Ordering::Relaxed);
        let end = self.end.load(Ordering::Relaxed);

        let mut i = start;
        while i != end {
            dest.push(self.buffer[i].clone());
            i = (i + 1) % self.capacity;
        }
    }
}

impl<T> Index<usize> for AtomicDeque<T>
where
    T: Clone,
{
    type Output = T;

    fn index(&self, i: usize) -> &T {
        let pos = (self.start.load(Ordering::Relaxed) + i) % self.capacity;
        &self.buffer[pos]
    }
}

impl<T> IndexMut<usize> for AtomicDeque<T>
where
    T: Clone
{
    fn index_mut<'a>(&'a mut self, i: usize) -> &'a mut Self::Output {
        let pos = (self.start.load(Ordering::Relaxed) + i) % self.capacity;
        &mut self.buffer[pos]
    }
}
