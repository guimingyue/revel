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

use std::cell::RefCell;
use std::iter::Iterator;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use rand::prelude::ThreadRng;
use rand::Rng;
use rand::rngs::StdRng;

const MAX_HEIGHT: usize = 12;

struct Node<K> {
    
    key: K,
    
    next: Vec<AtomicPtr<Node<K>>>,
    
}

pub struct SkipList<K> where K: Default {
    
    head: Node<K>,
    
    max_height: AtomicUsize,
    
    rand: RefCell<ThreadRng>,
    
    comparator: fn(a: &K, b: &K) -> std::cmp::Ordering
    
}

struct Iter<'a, K> where K: Default {
    
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
    
    fn no_barrier_set_next(&mut self, n: usize, node: *mut Node<K>) {
        self.next[n] = AtomicPtr::new(node);
    }
    
    fn next(&self, n: usize) -> *const Node<K> {
        assert!(n >= 0);
        self.next[n].load(Ordering::Acquire)
    } 
}

impl<K> SkipList<K> where K: Default {
    
    pub fn new(comparator: fn(a: &K, b: &K) -> std::cmp::Ordering) -> Self {
        SkipList {
            comparator,
            max_height: AtomicUsize::new(1),
            head: Node::new_node(K::default(), MAX_HEIGHT),
            // ?
            rand: RefCell::new(ThreadRng::default())
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
                new_node.no_barrier_set_next(i, prev[i] as *mut Node<K>);
                prev[i] = &new_node as *const Node<K> as *mut Node<K>;
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
            let next = unsafe {(*x).next(level)};
            if self.key_is_after_node(key, next) {
                x = next;
            } else {
                if ret_prev {
                    prev[level] = x as *const Node<K>
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
        self.rand.borrow_mut().gen_range((1..MAX_HEIGHT))
    }
    
    fn get_max_height(&self) -> usize {
        self.max_height.load(Ordering::Relaxed)
    }

    fn key_is_after_node(&self, key: &K, n: *const Node<K>) -> bool {
        unsafe {
            !n.is_null() && self.compare(key, &(*n).key) == std::cmp::Ordering::Less
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
    
    pub fn new(list: &SkipList<K>) -> Self {
        Iter {
            list,
            node: None
        }
    }

    /// Returns true iff the iterator is positioned at a valid node.
    pub fn valid(&self) -> bool {
        self.node.is_none()
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
        let pre = self.list.find_less_than(key);
        // todo!() fix this
        if pre.unwrap() == &self.list.head {
            self.node = None;
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
        // todo!() fix this
        if let Some(n) = self.node {
            if n == &self.list.head {
                self.node = None;
            }
        }
    }
}

