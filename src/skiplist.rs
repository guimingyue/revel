// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::borrow::BorrowMut;
use std::cell::RefCell;
use std::iter::Iterator;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use rand::prelude::ThreadRng;
use rand::Rng;
use rand::rngs::StdRng;
use crate::random::Random;

const MAX_HEIGHT: usize = 12;

struct Node<K> {
    
    key: K,
    
    next: Vec<AtomicPtr<Node<K>>>,
    
}

pub struct SkipList<K> where K: Default {
    
    head: Node<K>,
    
    max_height: AtomicUsize,
    
    rand: RefCell<Random>,
    
    comparator: fn(a: &K, b: &K) -> std::cmp::Ordering
    
}

pub struct Iter<'a, K> where K: Default {
    
    list: &'a SkipList<K>,
    
    node: Option<&'a Node<K>>
    
}

impl <K> Node<K> {
    fn new_node(key: K, max_height: usize) -> Self {
        Self {
            key, 
            next: std::iter::repeat_with(||AtomicPtr::default()).take(max_height).collect::<Vec<_>>()
        }
    }
    
    fn no_barrier_set_next(&mut self, n: usize, node: *const Node<K>) {
        self.next[n].store(node as *mut Node<K>, Ordering::Relaxed);
    }
    
    fn next(&self, n: usize) -> *mut Node<K> {
        assert!(n >= 0);
        self.next[n].load(Ordering::Acquire)
    }

    fn set_next(&self, n: usize, node: *mut Node<K>) {
        self.next[n].store(node, Ordering::Release)
    }

    fn no_barrier_next(&self, n: usize) -> *const Node<K> {
        self.next[n].load(Ordering::Relaxed)
    }
}

impl<K> SkipList<K> where K: Default {
    
    pub fn new(comparator: fn(a: &K, b: &K) -> std::cmp::Ordering) -> Self {
        SkipList {
            comparator,
            max_height: AtomicUsize::new(1),
            head: Node::new_node(K::default(), MAX_HEIGHT),
            rand: RefCell::new(Random::new(0xdeadbeef))
        }
    }
    
    pub fn insert(&self, key: K) {
        let (_, mut prev) = self.find_greater_or_equal(&key, true);
        let height = self.random_height();
        if height > self.get_max_height() {
            for i in self.get_max_height()..height {
                prev[i] = &self.head as *const Node<K> as *mut Node<K>;
            }
            self.max_height.store(height, Ordering::Relaxed);
        }
        let mut new_node = Node::new_node(key, height);
        for i in 0..height {
            unsafe {
                let pre_next = (*prev[i]).no_barrier_next(i);
                new_node.no_barrier_set_next(i, pre_next);
                let new_node_ptr: *mut Node<K> = &mut new_node as *mut Node<K>;
                (&mut *(prev[i] as *mut Node<K>)).no_barrier_set_next(i, new_node_ptr);
            }
        }
    }
    
    pub fn contains(&self, key: &K) -> bool {
        let (x, _) = self.find_greater_or_equal(key, false);
        match x {
            None => false,
            Some(node) => self.equal(key, &node.key)
        }
    }

    fn find_greater_or_equal(&self, key: &K, ret_prev: bool) -> (Option<&Node<K>>, Box<Vec<*const Node<K>>>) {
        let mut prev = vec![std::ptr::null(); MAX_HEIGHT];
        let mut x = &self.head as *const Node<K>;
        let mut level = self.get_max_height() - 1;
        loop {
            let next: *const Node<K> = unsafe {(*x).no_barrier_next(level)};
            if !next.is_null() {
                let next_key = unsafe{&(*next).key};
                println!("")
            }

            if self.key_is_after_node(key, next) {
                x = next;
            } else {
                if ret_prev {
                    prev[level] = x;
                }
                if level == 0 {
                    return if x.is_null() {
                        (None, Box::new(prev))
                    } else {
                        unsafe { (Some(&*x), Box::new(prev)) }
                    }
                }
                level -= 1;
            }
        }
    }
    
    fn random_height(&self) -> usize {
        const kBranching: usize = 4;
        let mut height: usize = 1;
        while height < MAX_HEIGHT && self.rand.borrow_mut().one_in(kBranching as i32) {
            height += 1;
        }
        assert!(height > 0);
        assert!(height <= MAX_HEIGHT);
        return 3;
    }
    
    fn get_max_height(&self) -> usize {
        self.max_height.load(Ordering::Relaxed)
    }

    fn key_is_after_node(&self, key: &K, n: *const Node<K>) -> bool {
        // todo!()
        let is_null = n.is_null();
        unsafe {
            let a = &(*n).key;
            let b = key;
            if !n.is_null() {
                let eq = self.compare(&(*n).key, key);
                println!("")
            }
            !n.is_null() && self.compare(&(*n).key, key) == std::cmp::Ordering::Less
        }
    }
    
    fn find_less_than(&self, key: &K) -> Option<&Node<K>> {
        let mut x = &self.head as *const Node<K>;
        let mut level = self.get_max_height() - 1;
        loop {
            // todo!() assert x is head or compare(x.key, k) < 0
            unsafe {
                let next =  (*x).next(level);
                if next.is_null() || self.compare(&(*next).key, key) == std::cmp::Ordering::Less {
                    if level == 0 {
                        return Some(&*x);
                    } else {
                        level -= 1;
                    }
                } else {
                    x = next;
                }
            }
        }
    }
    
    fn find_last(&self) -> Option<&Node<K>> {
        let mut x = &self.head as *const Node<K>;
        let mut level = self.get_max_height() - 1;
        loop {
            unsafe {
                let next =  (*x).next(level);
                if next.is_null() {
                    if level == 0 {
                        return Some(&*x);
                    } else {
                        level -= 1;
                    }
                } else {
                    x = next;
                }
            }
        }
    }
    
    fn compare(&self, a: &K, b: &K) -> std::cmp::Ordering {
        (self.comparator)(a, b)
    }
    
    fn equal(&self, a: &K, b: &K) -> bool {
        self.compare(a, b) == std::cmp::Ordering::Equal
    }
}

impl<'a, K> Iter<'a, K> where K: Default {
    
    pub fn new(list: &'a SkipList<K>) -> Self {
        Iter {
            list,
            node: None
        }
    }

    /// Returns true iff the iterator is positioned at a valid node.
    pub fn valid(&self) -> bool {
        self.node.is_some()
    }

    /// Returns the key at the current position.
    /// REQUIRES: Valid()
    pub fn key(&self) -> &K {
        assert!(self.valid());
        &self.node.unwrap().key
    }

    /// Advances to the next position.
    /// REQUIRES: Valid()
    pub fn next(&mut self) {
        assert!(self.valid());
        let ptr = self.node.unwrap().next(0);
        if ptr.is_null() {
            self.node = None
        } else {
            self.node = unsafe {Some(&(*ptr))}
        }
    }

    /// Advances to the previous position.
    /// REQUIRES: Valid()
    pub fn prev(&mut self) {
        assert!(self.valid());
        let key = &self.node.unwrap().key;
        self.node = self.list.find_less_than(key);
        if let Some(n) = self.node {
            if Self::ref_eq(n, &self.list.head) {
                self.node = None;
            }
        }
    }

    /// Advance to the first entry with a key >= target
    pub fn seek(&mut self, target: &K) {
        let (node, _) = self.list.find_greater_or_equal(target, false);
        self.node = node;
    }

    /// Position at the first entry in list.
    /// Final state of iterator is Valid() iff list is not empty.
    pub fn seek_to_first(&mut self) {
        let node = self.list.head.next(0);
        if node.is_null() {
            self.node = None;
        } else {
            self.node = unsafe {Some(&(*node))};    
        }
    }

    /// Position at the last entry in list.
    /// Final state of iterator is Valid() iff list is not empty.
    pub fn seek_to_last(&mut self) {
        self.node = self.list.find_last();
        if let Some(n) = self.node {
            if Self::ref_eq(n, &self.list.head) {
                self.node = None;
            }
        }
    }
    
    fn ref_eq<T>(r1: &T, r2: &T) -> bool {
        std::ptr::eq(r1, r2)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use crate::random::Random;
    use super::*;

    const cmp_func: fn(&i32, &i32) -> std::cmp::Ordering = |a: &i32, b: &i32| a.cmp(b);

    #[test]
    fn test_skiplist_empty() {
        let list = SkipList::new(cmp_func);
        assert!(!list.contains(&10));

        let mut iter = Iter::new(&list);
        assert!(!iter.valid());
        iter.seek_to_first();
        assert!(!iter.valid());
        iter.seek(&100);
        assert!(&iter.valid());
        iter.seek_to_last();
        assert!(!iter.valid());
    }

    #[test]
    fn test_skiplist_insert_and_lookup() {
        const N:i32 = 2000;
        const R:i32 = 5000;
        let mut rnd = Random::new(1000);
        let mut keys = HashSet::new();
        let skiplist = SkipList::new(cmp_func);

        let new_key = 2000;
        keys.insert(new_key);
        skiplist.insert(new_key);
        /*let mut no = Node::new_node(1, 2);
        let ptr_no: *mut Node<i32> = &mut no as *mut Node<i32>;
        /*keys.insert(1432);
        skiplist.contains(&1432);*/
        unsafe {
            let kv = (*ptr_no).key;
            println!("{}", kv);
        }*/


        let new_key = 1000;
        let (x, prev) = skiplist.find_greater_or_equal(&new_key, true);
        let mut null_count = 0;
        for i in 0..12 {
            let n = skiplist.head.no_barrier_next(i);
            if n.is_null() {
                null_count += 1
            } else {
                unsafe {
                let vv = &(*n).key;
                println!("{}", vv)};
            }

        }
        let n = skiplist.head.next(1);
        let n_null = n.is_null();
        let v = unsafe {(*n).key};
        let nr = unsafe{&(*n)};
        let v1 = &nr.key;
        println!("{}", v);
        let n = skiplist.head.next(1);
        let v = unsafe {&(*n).key};
        println!("{}", v);


        for i in 0..N {
            let n = rnd.next();
            let key = n as i32 % R;
            if keys.insert(key) {
                let contains = skiplist.contains(&key);
                skiplist.insert(key);
            }
        }

        for i in 0..R {
            if skiplist.contains(&i) {
                assert!(keys.contains(&i));
            } else {
                assert!(!keys.contains(&i));
            }
        }

    }
}

