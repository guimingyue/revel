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

use std::cmp::Ordering;
use crate::coding::{decode_fixed64, encode_fixed64, encode_varint32};
use crate::comparator::Comparator;
use crate::slice::Slice;

pub type SequenceNumber = u64;

static kMaxSequenceNumber: SequenceNumber = ((1 as u64) << 56) - 1;

#[derive(Clone, Copy, PartialEq, PartialOrd)]
pub enum ValueType {
    
    KTypeDeletion = 0x0,
    
    KTypeValue = 0x1
}

impl ValueType {
    
    pub fn from(ordinal: u8) -> Self {
        match ordinal { 
            ox0 => ValueType::KTypeDeletion,
            0x1 => ValueType::KTypeValue,
            _ => panic!("Unknown ValueType ordinal")
        }
    }
}

static kValueTypeForSeek: ValueType = ValueType::KTypeValue;

pub struct InternalKeyComparator {

    user_comparator: fn(a: &Slice, b: &Slice) -> Ordering

}

impl InternalKeyComparator {

    pub const fn new(comparator: fn(a: &Slice, b: &Slice) -> Ordering) -> Self {
        InternalKeyComparator {
            user_comparator: comparator
        }
    }
}

impl Comparator for InternalKeyComparator {

    fn compare(&self, akey: &Slice, bkey: &Slice) -> Ordering {
        let mut r = (self.user_comparator)(akey, bkey);
        if r == Ordering::Equal {
            let anum = decode_fixed64(akey.data(), akey.size() - 8);
            let bnum = decode_fixed64(bkey.data(), bkey.size() - 8);
            if anum > bnum {
                r = Ordering::Less
            } else {
                r = Ordering::Greater
            }
        }
        r
    }

    fn name(&self) -> &str {
        "revel.InternalKeyComparator"
    }
}

unsafe impl Sync for InternalKeyComparator {

}

pub struct LookupKey {
    
    buf: Vec<u8>,
    
    start: usize,
    
    kstart: usize,
    
    end: usize
}

impl LookupKey {
    
    pub fn new(user_key: &Slice, s: SequenceNumber) -> Self {
        let usize = user_key.size();
        let needed = usize + 13;
        let mut buf = vec![0; needed];
        let start = 0;
        let writed = encode_varint32(&mut buf, usize as u32 + 8, 0);
        let kstart = writed;
        unsafe {
            std::ptr::copy(user_key.data().as_ptr(), buf.as_mut_ptr().offset(kstart as isize), usize);
        }
        let pak = pack_sequence_and_type(s, kValueTypeForSeek);
        encode_fixed64(&mut buf, pak, kstart + usize);
        let end = kstart + usize + 8;
        LookupKey{
            buf, 
            start, 
            kstart, 
            end}
    }
    
    pub fn memtable_key(&self) -> Slice {
        Slice::from_bytes(&self.buf[self.start..self.end])
    }
    
    pub fn user_key(&self) -> Slice {
        Slice::from_bytes(&self.buf[self.kstart..self.end])
    }
}

fn pack_sequence_and_type(seq: u64, t: ValueType) -> u64 {
    assert!(seq <= kMaxSequenceNumber);
    assert!(t <= kValueTypeForSeek);
    (seq << 8) | t as u64
}




pub fn compare(akey: &Slice, bkey: &Slice) -> std::cmp::Ordering {
    // todo!()
    std::cmp::Ordering::Equal
}