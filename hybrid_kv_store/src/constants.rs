
/* Disk Page size, in bytes */
pub const PAGE_SIZE: usize = 356 * 9;
// pub const PAGE_SIZE: usize = 3 * 9;


/* B-Tree Constants */
// pub const FANOUT : usize = 1500;
pub const FANOUT: usize = 3;

/* LSM-Tree Constants */
pub const BLOOM_CAPACITY: usize = 100_000;
pub const BUFFER_CAPACITY: usize = 100_000;
pub const TREE_RATIO: usize = 3;
