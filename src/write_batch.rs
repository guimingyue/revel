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

use crate::coding::{decode_fix32, decode_fixed64, encode_fixed32, encode_fixed64, put_length_prefixed_slice};
use crate::dbformat::{SequenceNumber, ValueType};
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
        append(self, source)
    }

    pub fn iterate(&self, handler: &dyn Handler) {

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

pub fn set_sequence(b: &mut WriteBatch, seq: SequenceNumber) {
    encode_fixed64(&mut b.rep, seq, 0);
}

pub fn append(dst: &mut WriteBatch, src: &WriteBatch) {
    set_count(dst, count(dst) + count(src));
    let length = src.rep.len() - K_HEADER;
    dst.rep.extend_from_slice(&src.rep[K_HEADER..K_HEADER + length]);
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {

    }
}