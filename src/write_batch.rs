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

use crate::coding::{decode_fix32, decode_fixed64, encode_fixed32, encode_fixed64, get_length_prefixed_slice, put_length_prefixed_slice};
use crate::dbformat::{SequenceNumber, ValueType};
use crate::memtable::MemTable;
use crate::slice::Slice;

const K_HEADER:usize = 12;

pub struct WriteBatch {
    rep: Vec<u8>
}

pub trait Handler {

    fn put(&mut self, key: &Slice, value: &Slice);

    fn delete(&mut self, key: &Slice);
}

impl WriteBatch {

    pub fn new() -> Self {
        WriteBatch {
            rep: vec![0; K_HEADER]
        }
    }

    pub fn clear(&mut self) {
        self.rep.clear();
        self.rep.resize(K_HEADER, 0);
    }

    pub fn put(&mut self, key: &Slice, value: &Slice) {
        set_count(self, count(self) + 1);
        self.rep.push(ValueType::KTypeValue as u8);
        put_length_prefixed_slice(self.rep.as_mut(), key);
        put_length_prefixed_slice(self.rep.as_mut(), value);
    }

    pub fn delete(&mut self, key: &Slice) {
        set_count(self, count(self) + 1);
        self.rep.push(ValueType::KTypeDeletion as u8);
        put_length_prefixed_slice(self.rep.as_mut(), key);
    }

    pub fn approximate_size(&self) -> usize {
        self.rep.len()
    }

    pub fn append(&mut self, source: &Self) {
        set_count(self, count(self) + count(source));
        let length = source.rep.len() - K_HEADER;
        self.rep.extend_from_slice(&source.rep[K_HEADER..K_HEADER + length]);
    }

    pub fn set_sequence(&mut self, seq: SequenceNumber) {
        encode_fixed64(&mut self.rep, seq, 0);
    }

    pub fn count(&self) -> u32 {
        count(self)
    }

    pub fn iterate(&self, handler: &mut dyn Handler) {
        let mut input = Slice::from_bytes(&self.rep);
        input.remove_prefix(K_HEADER);
        let mut found = 0;
        while input.empty() {
            found += 1;
            let data = input.data();
            let tag = data[0];
            let mut offset = 1;
            match ValueType::from(tag) {
                ValueType::KTypeValue => {
                    match get_length_prefixed_slice(&data[offset..]) {
                        Ok(key) => {
                            offset += key.size();
                            match get_length_prefixed_slice(&data[offset..]) {
                                Ok(value) => handler.put(&key, &value),
                                Err(_) => {

                                }
                            }
                        },
                        Err(_) => {
                            //
                        }
                    };
                },
                ValueType::KTypeDeletion => {
                    match get_length_prefixed_slice(input.data()) {
                        Ok(key) => handler.delete(&key),
                        Err(_) => {

                        }
                    }
                }
            }
        }
        if found != count(self) {
            //
        } else {

        }
    }
}

struct MemTableInserter<'a> {

    sequence: SequenceNumber,

    mem: &'a mut MemTable
}

impl <'a> MemTableInserter<'a> {

    pub fn new(mem: &'a mut MemTable, sequence: SequenceNumber) -> Self {
        MemTableInserter {
            mem,
            sequence
        }
    }
}

impl <'a> Handler for MemTableInserter<'a> {
    fn put(&mut self, key: &Slice, value: &Slice) {
        self.mem.add(self.sequence, ValueType::KTypeValue, key, value);
        self.sequence += 1;
    }

    fn delete(&mut self, key: &Slice) {
        self.mem.add(self.sequence, ValueType::KTypeDeletion, key, &Slice::from_empty());
        self.sequence += 1;
    }
}

pub fn count(b: &WriteBatch) -> u32 {
    decode_fix32(&b.rep[8..])
}

pub fn set_count(b: &mut WriteBatch, n: u32) {
    encode_fixed32(&mut b.rep[8..], n, 0);
}

pub fn sequence(b: &WriteBatch) -> SequenceNumber {
    decode_fixed64(&b.rep[8..], 0)
}

pub fn append(dst: &mut WriteBatch, src: &WriteBatch) {
    set_count(dst, count(dst) + count(src));
    let length = src.rep.len() - K_HEADER;
    dst.rep.extend_from_slice(&src.rep[K_HEADER..K_HEADER + length]);
}

pub fn insert_into(b: &WriteBatch, mem: &mut MemTable) {
    let mut inserter = MemTableInserter::new(mem, sequence(b));
    b.iterate(&mut inserter);
}

pub fn set_contents(b: &mut WriteBatch, contents: &Slice) {
    assert!(contents.size() >= K_HEADER);
    b.rep.clear();
    b.rep.extend_from_slice(contents.data());
}

pub fn byte_size(batch: &WriteBatch) -> usize {
    batch.rep.len()
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {

    }
}