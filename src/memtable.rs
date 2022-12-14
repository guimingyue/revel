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
use std::cmp::Ordering;
use std::cmp::Ordering::Less;
use std::rc::Rc;
use crate::coding::{decode_fixed64, encode_fixed64, encode_varint32, get_varint32, varint_length};
use crate::comparator::Comparator;
use crate::dbformat::{compare, InternalKeyComparator, LookupKey, SequenceNumber, ValueType};
use crate::{comparator, Error};
use crate::Error::NotFound;
use crate::skiplist::{Cmp, Iter, SkipList};
use crate::slice::Slice;

#[inline]
fn get_length_prefixed_slice(buf: &[u8], offset: usize) -> Slice {
    // todo!("fix unwrap")
    let (key_length, new_offset) = get_varint32(buf, offset, offset + 5).unwrap();
    Slice::from_bytes(&buf[offset + new_offset..(offset + new_offset + key_length as usize)])
}

type Table = SkipList<Vec<u8>>;

struct KeyComparator {
    comparator: Rc<InternalKeyComparator>
}

impl KeyComparator {
    pub fn new(comparator: Rc<InternalKeyComparator>) -> Self {
        KeyComparator {
            comparator
        }
    }
}

impl Cmp<Vec<u8>> for KeyComparator {
    fn compare(&self, akey: &Vec<u8>, bkey: &Vec<u8>) -> Ordering {
        let a = get_length_prefixed_slice(akey, 0);
        let b = get_length_prefixed_slice(bkey, 0);
        self.comparator.compare(&a, &b)
    }
}

pub struct MemTable {
    
    table: Box<Table>,

    comparator: Rc<InternalKeyComparator>
}

impl MemTable {
    
    pub fn new(comparator: InternalKeyComparator) -> Self {
        let cmp = Rc::new(comparator);
        let key_comparator = KeyComparator::new(cmp.clone());
        MemTable {
            table: Box::new(Table::new(Box::new(key_comparator))),
            comparator: cmp.clone()
        }
    }

    /// Format of an entry is concatenation of:
    /// 
    ///  key_size     : varint32 of internal_key.size()
    /// 
    ///  key bytes    : char[internal_key.size()]
    /// 
    ///  tag          : uint64((sequence << 8) | type)
    /// 
    ///  value_size   : varint32 of value.size()
    /// 
    ///  value bytes  : char[value.size()]
    pub fn add(&mut self, seq: SequenceNumber, valueType: ValueType, key: &Slice, value: &Slice) {
        let key_size = key.size();
        let val_size = value.size();
        let internal_key_size = key_size + 8;
        let encoded_len = varint_length(internal_key_size as u64) 
            + internal_key_size 
            + varint_length(val_size as u64) 
            + val_size;
        let mut buf = vec![0; encoded_len];
        
        let mut offset = encode_varint32(&mut buf, internal_key_size as u32, 0);
        unsafe {
            std::ptr::copy(key.data().as_ptr(), buf.as_mut_ptr().offset(offset as isize), key_size)
        }
        offset += key_size;
        encode_fixed64(&mut buf, (seq << 8) | valueType as u64, offset);
        offset += 8;
        offset += encode_varint32(&mut buf, val_size as u32, offset);
        unsafe {
            std::ptr::copy(value.data().as_ptr(), buf.as_mut_ptr().offset(offset as isize), val_size);
        }
        
        assert_eq!(offset + val_size, encoded_len);
        self.table.insert(buf)
    }

    /// If memtable contains a value for key, return (true, Ok(Vec<u8)).
    /// If memtable contains a deletion for key, return (true, Err(NotFound))
    /// Else, return (false,Err(NotFound).
    pub fn get(&self, key: &LookupKey) -> (bool, Result<Vec<u8>, Error>) {
        let memkey = key.memtable_key();
        let mut iter = Iter::new(&self.table);
        let data = memkey.data();
        // todo!() consider an unsafe method Vec::from_raw_parts_in(), with which copy action is unnecessary 
        iter.seek(&data.to_vec());
        if iter.valid() {
            // entry format is:
            //    klength  varint32
            //    userkey  char[klength]
            //    tag      uint64
            //    vlength  varint32
            //    value    char[vlength]
            // Check that it belongs to same user key.  We do not check the
            // sequence number since the Seek() call above should have skipped
            // all entries with overly large sequence numbers.
            let buf = iter.key();
            let result = get_varint32(buf, 0, 5);
            return match result {
                Ok((key_length, mut offset)) => {
                    if (self.comparator.user_comparator())(&Slice::from_bytes(&buf[offset..=(key_length-8) as usize]), &key.user_key()) == Ordering::Equal {
                        let tag = decode_fixed64(buf, offset + key_length as usize - 8);
                        return match ValueType::from((tag & 0xff) as u8) {
                            ValueType::KTypeValue => {
                                let slice = get_length_prefixed_slice(buf, offset + key_length as usize);
                                (true, Ok(slice.data().to_vec()))
                            },
                            ValueType::KTypeDeletion => {
                                (true, Err(NotFound))
                            }
                        }
                    }
                    return (false, Err(NotFound))
                },
                Err(_) => (false, Err(NotFound))
            }
        }
        (false, Err(NotFound))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        static user_comparator: fn(a: &Slice, b: &Slice) -> Ordering = |a: &Slice, b: &Slice| {
            a.data().cmp(b.data())
        };
        let internalKeyComparator = InternalKeyComparator::new(user_comparator);
        let mut mem = MemTable::new(internalKeyComparator);
        let (key, value) = ("key", "value");
        mem.add(1, ValueType::KTypeValue, &Slice::from_str(key), &Slice::from_str(value));
        let result = mem.get(&LookupKey::new(&Slice::from_str(key), 1 as SequenceNumber));
        assert!(result.0);
        assert_eq!(value, unsafe {String::from_utf8_unchecked(result.1.expect("unexpected result"))});
        let result = mem.get(&LookupKey::new(&Slice::from_str("yek"), 1 as SequenceNumber));
        assert!(!result.0);
        let err = result.1.expect_err("unexpect");
        assert_eq!(NotFound, err);
    }
}
