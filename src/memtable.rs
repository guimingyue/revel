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

//! memtable

use crate::skiplist::SkipList;

type Table = SkipList<Vec<u8>>;

pub struct MemTable {
    
    table: Box<Table>
    
}

impl MemTable {
    
    pub fn new(comparator: fn(a: &Vec<u8>, b: &Vec<u8>) -> std::cmp::Ordering) -> Self {
        MemTable {
            table: Box::new(Table::new(comparator))
        }
    }
    
    pub fn add(mut self, value: Vec<u8>) {
        self.table.insert(value)
    }
}

#[test]
fn test() {
    let func = |a: &Vec<u8>, b: &Vec<u8>| a.cmp(b);
    let mut mem = MemTable::new(func);
    mem.add(vec!['a' as u8])
}