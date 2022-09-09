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
    
    pub fn find_greater_or_equal(&self, key: &K, ret_prev: bool) -> (&Node<K>, Box<Vec<*const Node<K>>>) {
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
                    return unsafe {(&*x, Box::new(prev))};
                }
                level -= 1;
            }
        }
    }
    
    pub fn insert(&self, key: K) {
        let (x, mut prev) = self.find_greater_or_equal(&key, true);
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
    
    fn random_height(&self) -> usize {
        self.rand.borrow_mut().gen_range((1..MAX_HEIGHT))
    }
    
    fn get_max_height(&self) -> usize {
        self.max_height.load(Ordering::Relaxed)
    }

    fn key_is_after_node(&self, key: &K, n: *const Node<K>) -> bool {
        unsafe {
            !n.is_null() && (self.comparator)(key, &(*n).key) == std::cmp::Ordering::Less
        }
    }
}

