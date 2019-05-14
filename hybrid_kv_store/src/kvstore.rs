/**
 * A trait describing the methods a KVStore must implement. To be implemented
 * for B-Tree, LSM-Tree, and Hybrid data stores.
 */
pub trait KVStore {
    fn get(&mut self, key : i32) -> Option<i32>;
    fn delete(&mut self, key: i32);
    fn put(&mut self, key : i32, val : i32);
    fn scan(&self, low : i32, high : i32) -> Vec<i32>;
    fn debug_lookup(&mut self, key: i32);
}
