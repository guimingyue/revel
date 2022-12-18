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

use std::thread::sleep;
use crate::coding::{put_length_prefixed_slice, put_varint32, put_varint64};
use crate::dbformat::{InternalKey, SequenceNumber};
use crate::slice::Slice;

enum Tag {
    kComparator = 1,
    kLogNumber = 2,
    kNextFileNumber = 3,
    kLastSequence = 4,
    kCompactPointer = 5,
    kDeletedFile = 6,
    kNewFile = 7,
    // 8 was used for large value refs
    kPrevLogNumber = 9,

}

pub struct FileMetaData {
    refs: i32,
    // Seeks allowed until compaction
    allowed_seeks: i32,
    number: u64,
    // File size in bytes
    file_size: u64,
    // Smallest internal key served by table
    smallest: InternalKey,
    // Largest internal key served by table
    largest: InternalKey
}

pub struct VersionEdit {
    comparator: String,
    log_number: u64,
    prev_log_number: u64,
    next_file_number: u64,
    last_sequence: SequenceNumber,
    has_comparator: bool,
    has_log_number: bool,
    has_pre_log_number: bool,
    has_next_file_number: bool,
    has_last_sequence: bool,

    compact_pointers: Vec<(i32, InternalKey)>,
    deleted_files: Vec<(i32, u64)>,
    new_files: Vec<(i32, FileMetaData)>

}

impl VersionEdit {

    pub fn new(user_comparator: &str, log_number: u64, next_file_number: u64, last_sequence: SequenceNumber) -> Self {
        VersionEdit {
            comparator: user_comparator.to_string(),
            has_comparator: true,
            log_number,
            has_log_number: true,
            next_file_number,
            has_next_file_number: true,
            last_sequence,
            has_last_sequence: true,
            prev_log_number: 0,
            has_pre_log_number: false,
            compact_pointers: vec![],
            deleted_files: vec![],
            new_files: vec![]

        }
    }

    pub fn encode_to(&mut self, dst: &mut Vec<u8>) {
        if self.has_comparator {
            put_varint32(dst, Tag::kComparator as u32);
            put_length_prefixed_slice(dst, &Slice::from_str(self.comparator.as_str()))
        }
        if self.has_log_number {
            put_varint32(dst, Tag::kLogNumber as u32);
            put_varint64(dst, self.log_number);
        }
        if self.has_pre_log_number {
            put_varint32(dst, Tag::kPrevLogNumber as u32);
            put_varint64(dst, self.prev_log_number);
        }
        if self.has_next_file_number {
            put_varint32(dst, Tag::kNextFileNumber as u32);
            put_varint64(dst, self.next_file_number);
        }
        if self.has_last_sequence {
            put_varint32(dst, Tag::kLastSequence as u32);
            put_varint64(dst, self.last_sequence);
        }

        for (k, v) in &self.compact_pointers {
            put_varint32(dst, Tag::kCompactPointer as u32);
            put_varint32(dst, *k as u32); // level
            put_length_prefixed_slice(dst, &v.encode())
        }

        for (k, v) in &self.deleted_files {
            put_varint32(dst, Tag::kDeletedFile as u32);
            put_varint32(dst, *k as u32); // level
            put_varint64(dst, *v);// file number
        }

        for (k, v) in &self.new_files {
            put_varint32(dst, Tag::kNewFile as u32);
            put_varint32(dst, *k as u32);
            put_varint64(dst, v.number);
            put_varint64(dst, v.file_size);
            put_length_prefixed_slice(dst, &v.smallest.encode());
            put_length_prefixed_slice(dst, &v.largest.encode());
        }
    }
}