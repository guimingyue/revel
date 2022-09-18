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

pub fn encode_varint32(buf: &mut [u8], v: usize, offset: usize) -> usize {
    const B: usize = 128;
    let ptr = buf[offset..].as_mut_ptr();
    unsafe {
        if v < (1 << 7) {
            ptr.write(v as u8);
            1
        } else if v < (1 << 14) {
            ptr.write((v | B) as u8);
            ptr.offset(1).write((v >> 7) as u8);
            2
        } else if v < (1 << 21) {
            ptr.write((v | B) as u8);
            ptr.offset(1).write(((v >> 7) | B) as u8);
            ptr.offset(2).write((v >> 14) as u8);
            3
        } else if v < (1 << 28) {
            ptr.write((v | B) as u8);
            ptr.offset(1).write(((v >> 7) | B) as u8);
            ptr.offset(2).write(((v >> 14) | B) as u8);
            ptr.offset(3).write((v >> 21) as u8);
            4
        } else {
            ptr.write((v | B) as u8);
            ptr.offset(1).write(((v >> 7) | B) as u8);
            ptr.offset(2).write(((v >> 14) | B) as u8);
            ptr.offset(3).write(((v >> 21) | B) as u8);
            ptr.offset(4).write((v >> 28) as u8);
            5
        }
    }
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
pub fn get_varint32(buf: &[u8], offset: usize, limit: usize) -> Result<(usize, usize), &str> {
    if buf.len() + offset < limit {
        let result = buf[0];
        if result & 128 == 0 {
            return Ok((result as usize, 1));
        }
    }
    get_varint32_fallback(buf, offset, limit)
}

fn get_varint32_fallback(buf: &[u8], offset: usize, limit: usize) -> Result<(usize, usize), &str> {
    Err("todo!()")
}

pub fn decode_fixed64(buf: &[u8], offset: usize) -> u64 {
    let buffer = buf.as_ptr();
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