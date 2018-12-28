/**
 * A trait describing the methods a KVStore must implement. To be implemented
 * for B-Tree, LSM-Tree, and Hybrid data stores.
 */
pub trait KVStore {
    fn get(&self, key : i32) -> i32;
    fn put(&mut self, key : i32, val : i32);
    fn scan(&self, low : i32, high : i32) -> [i32];
}
