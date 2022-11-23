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

use std::fs::File;
use std::io::Write;
use crate::slice::Slice;

pub fn encode_varint32(buf: &mut [u8], v: u32, offset: usize) -> usize {
    const B: i32 = 128;
    let ptr = buf[offset..].as_mut_ptr();
    unsafe {
        if v < (1 << 7) {
            ptr.write(v as u8);
            1
        } else if v < (1 << 14) {
            ptr.write((v as i32 | B) as u8);
            ptr.offset(1).write((v >> 7) as u8);
            2
        } else if v < (1 << 21) {
            ptr.write((v as i32 | B) as u8);
            ptr.offset(1).write(((v >> 7) as i32 | B) as u8);
            ptr.offset(2).write((v >> 14) as u8);
            3
        } else if v < (1 << 28) {
            ptr.write((v as i32 | B) as u8);
            ptr.offset(1).write(((v >> 7) as i32 | B) as u8);
            ptr.offset(2).write(((v >> 14) as i32 | B) as u8);
            ptr.offset(3).write((v >> 21) as u8);
            4
        } else {
            ptr.write((v as i32 | B) as u8);
            ptr.offset(1).write(((v >> 7) as i32 | B) as u8);
            ptr.offset(2).write(((v >> 14) as i32 | B) as u8);
            ptr.offset(3).write(((v >> 21) as i32 | B) as u8);
            ptr.offset(4).write((v >> 28) as u8);
            5
        }
    }
}

pub fn encode_fixed32(buf: &mut [u8], value: u32, offset: usize) -> usize {
    let buffer = buf[offset..].as_mut_ptr();

    // Recent clang and gcc optimize this to a single mov / str instruction.
    unsafe {
        *buffer.offset(0) = value as u8;
        *buffer.offset(1) = (value >> 8) as u8;
        *buffer.offset(2) = (value >> 16) as u8;
        *buffer.offset(3) = (value >> 24) as u8;
    }
    4
}

/// todo!() inline this function
pub fn encode_fixed64(buf: &mut [u8], value: u64, offset: usize) -> usize {
    let buffer = buf[offset..].as_mut_ptr();
    
    // Recent clang and gcc optimize this to a single mov / str instruction.
    unsafe {
        *buffer.offset(0) = value as u8;
        *buffer.offset(1) = (value >> 8) as u8;
        *buffer.offset(2) = (value >> 16) as u8;
        *buffer.offset(3) = (value >> 24) as u8;
        *buffer.offset(4) = (value >> 32) as u8;
        *buffer.offset(5) = (value >> 40) as u8;
        *buffer.offset(6) = (value >> 48) as u8;
        *buffer.offset(7) = (value >> 56) as u8;
    }
    8
}

/// Returns the length of the varint32 or varint64 encoding of "v"
pub fn varint_length(mut v: u64) -> usize {
    let mut len = 1;
    while v >= 128 {
        v = v >> 7;
        len += 1
    }
    len
}

/// Reference-based variants of get_varint...  These either return a value
/// plus the number of bytes that read , or return
/// an error. These routines only look at bytes in the range
/// [p..limit-1]
pub fn get_varint32(buf: &[u8], offset: usize, limit: usize) -> Result<(u32, usize), &str> {
    if offset < limit {
        let result = buf[offset];
        if result & 128 == 0 {
            return Ok((result as u32, 1));
        }
    }
    get_varint32_fallback(buf, offset, limit)
}

/// fallback path of get_varint32
fn get_varint32_fallback(buf: &[u8], offset: usize, limit: usize) -> Result<(u32, usize), &str> {
    let mut result: u32 = 0;
    let mut new_offset = offset;
    let mut shift = 0;
    while shift <= 28 && new_offset < limit {
        let byte = buf[new_offset] as u32;
        new_offset += 1;
        if byte & 128 != 0 {
            result |= (byte & 127) << shift
        } else {
            result |= byte << shift;
            return Ok((result, new_offset - offset));
        }
        shift += 7;
    }
    Err("")
}

pub fn decode_fixed64(buf: &[u8], offset: usize) -> u64 {
    let buffer = buf[offset..].as_ptr();
    unsafe {
        (*buffer.offset(0) as u64) |
            (*buffer.offset(1) as u64) << 8 |
            (*buffer.offset(2) as u64) << 16 |
            (*buffer.offset(3) as u64) << 24 |
            (*buffer.offset(4) as u64) << 32 |
            (*buffer.offset(5) as u64) << 40 |
            (*buffer.offset(6) as u64) << 48 |
            (*buffer.offset(7) as u64) << 56
    }
}

pub fn decode_fix32(buf: &[u8]) -> u32 {
    return buf[0] as u32 |
        ((buf[1] as u32) << 8) |
        ((buf[2] as u32) << 16) |
        ((buf[3] as u32) << 24);
}

pub fn put_varint32(dst: &mut Vec<u8>, v: u32) -> usize {
    let mut buf = vec![0;5];
    let size = encode_varint32(&mut buf, v, 0);
    dst.write(&buf[..size]).expect("put varint32 failed")
}

pub fn put_length_prefixed_slice(dst: &mut Vec<u8>, value: &Slice) {
    put_varint32(dst, value.size() as u32);
    dst.extend_from_slice(value.data());
}

#[cfg(test)]
mod tests {
    use std::env::var;
    use std::fmt::format;
    use super::*;
    
    #[test]
    fn test_coding_varint32() {
        let mut s = Vec::new();
        let mut offset = 0;
        for i in 0..32 * 32 {
            let v = (i / 32) << (i % 32);
            let put_size = put_varint32(&mut s, v);
            offset += put_size;
        }
        
        let limit = s.len();
        offset = 0;
        for i in 0..32 * 32 {
            let expected = (i / 32) << (i % 32);
            let (actual, var_size) = get_varint32(&s, offset, limit).expect(format!("get varint32 failed, index: {}", i).as_str());
            assert_eq!(expected, actual, "failed, index: {}", i);
            assert_eq!(varint_length(actual as u64), var_size, "failed, index: {}", i);
            offset += var_size;
        }
    }

    #[test]
    fn test_coding_varint32_overflow() {
        let input = vec![129, 130, 131, 132, 133, 17];
        let result = get_varint32(input.as_slice(), 0, input.len());
        assert!(result.is_err());
    }

    #[test]
    fn test_coding_varint32_truncation() {
        let large_value = (1u32 << 31) + 100;
        let mut buf = vec![];
        put_varint32(&mut buf, large_value);
        for len in 0..buf.len() {
            let result = get_varint32(buf.as_slice(), 0, len);
            assert!(result.is_err());
        }
        let result = get_varint32(buf.as_slice(), 0, buf.len()).expect("large value truncation failed");
        assert_eq!(large_value, result.0)
    }
}