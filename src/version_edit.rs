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

use crate::coding::{get_length_prefixed_slice, get_varint32, get_varint64, put_length_prefixed_slice, put_varint32, put_varint64};
use crate::{dbformat, version_set};
use crate::dbformat::{InternalKey, SequenceNumber};
use crate::error::Error;
use crate::slice::Slice;
use enum_ordinalize::Ordinalize;


#[derive(Debug, PartialEq, Eq, Ordinalize)]
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

impl Tag {
    /*pub fn from(ordinal: u8) -> Self {
        match ordinal {
            1 => Tag::kComparator,
            2 => Tag::kLogNumber,
            _ => panic!("Unknown ValueType ordinal")
        }
    }*/
}

#[derive(Default)]
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

#[derive(Default)]
pub(crate) struct VersionEdit {
    pub(crate) comparator: String,
    pub(crate) log_number: u64,
    pub(crate) prev_log_number: u64,
    pub(crate) next_file_number: u64,
    pub(crate) last_sequence: SequenceNumber,
    pub(crate) has_comparator: bool,
    pub(crate) has_log_number: bool,
    pub(crate) has_pre_log_number: bool,
    pub(crate) has_next_file_number: bool,
    pub(crate) has_last_sequence: bool,

    compact_pointers: Vec<(u32, InternalKey)>,
    deleted_files: Vec<(u32, u64)>,
    new_files: Vec<(i32, FileMetaData)>

}

fn get_level(input: &[u8]) -> Option<(u32, usize)> {
    match get_varint32(input, 0, input.len()) {
        Ok((val, len)) if val < dbformat::config::kNumLevels => Some((val, len)),
        _ => None
    }
}

fn get_internal_key(input: &[u8]) -> Option<(InternalKey, usize)> {
    match get_length_prefixed_slice(input) {
        Ok((slice, len)) => {
            let mut internal_key = InternalKey::default();
            internal_key.decode_from(slice.data());
            Some((internal_key, len))
        },
        _ => None
    }
}

impl VersionEdit {

    pub fn new(user_comparator: &str, log_number: u64, next_file_number: u64, last_sequence: SequenceNumber) -> Self {
        VersionEdit {
            comparator: user_comparator.to_string(),
            has_comparator: !user_comparator.is_empty(),
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

    pub fn decode_from(input: &[u8]) -> crate::Result<Self>{
        let mut msg;
        let mut offset = 0;
        let limit = input.len();

        let mut version_edit = VersionEdit::default();
        loop {
            let tag = match get_varint32(input, 0, limit) {
                Ok((val, len)) => {
                    offset += len;
                    if let Some(t) = Tag::from_ordinal(val as i8) {
                        t
                    } else {
                        break;
                    }
                },
                Err(_) => {
                    break;
                }
            };
            match tag {
                Tag::kComparator => {
                    match get_length_prefixed_slice(&input[offset..]) {
                        Ok((slice, len)) => {
                            version_edit.comparator = unsafe {String::from_utf8_unchecked(slice.data().to_vec())};
                            version_edit.has_comparator = !version_edit.comparator.is_empty();
                            offset += len;
                        },
                        Err(_) => {
                            msg = "comparator name";
                        }
                    }
                },
                Tag::kLogNumber => {
                    match get_varint64(input, offset, limit) {
                        Ok((val, len)) => {
                            version_edit.log_number = val;
                            version_edit.has_log_number = true;
                            offset += len;
                        },
                        Err(_) => {
                            msg = "log number";
                        }
                    }
                },
                Tag::kPrevLogNumber => {
                    match get_varint64(input, offset, limit) {
                        Ok((val, len)) => {
                            version_edit.prev_log_number = val;
                            version_edit.has_pre_log_number = true;
                            offset += len;
                        },
                        Err(_) => {
                            msg = "previous log number";
                        }
                    }
                },
                Tag::kNextFileNumber => {
                    match get_varint64(input, offset, limit) {
                        Ok((val, len)) => {
                            version_edit.next_file_number = val;
                            version_edit.has_next_file_number = true;
                            offset += len;
                        },
                        Err(_) => {
                            msg = "previous log number";
                        }
                    }
                },
                Tag::kLastSequence => {
                    match get_varint64(input, offset, limit) {
                        Ok((val, len)) => {
                            version_edit.last_sequence = val;
                            version_edit.has_last_sequence = true;
                            offset += len;
                        },
                        Err(_) => {
                            msg = "last sequence number";
                        }
                    }
                },
                Tag::kCompactPointer => {
                    let error = if let Some((level, len)) = get_level(&input[offset..]) {
                        offset += len;
                        if let Some((key, len)) = get_internal_key(&input[offset..]) {
                            offset += len;
                            version_edit.compact_pointers.push((level, key));
                            true
                        } else {
                            false
                        }
                    } else {
                        false
                    };
                    if !error {
                        msg = "compaction pointer";
                    }
                },
                Tag::kDeletedFile => {
                    let error = if let Some((level, len)) = get_level(&input[offset..]) {
                        offset += len;
                        if let Ok((key, len)) = get_varint64(input, offset, limit) {
                            offset += len;
                            version_edit.deleted_files.push((level, key));
                            true
                        } else {
                            false
                        }
                    } else {
                        false
                    };
                    if !error {
                        msg = "deleted files";
                    }
                },
                Tag::kNewFile => {
                    let mut level = 0;
                    let mut f = FileMetaData::default();
                    if let Some(len) = Self::parse_new_file(&input[offset..], &mut level, &mut f) {
                        offset += len;
                    } else {
                        msg = "new-file entry";
                    }
                }
            }
        }

        Err(Error::Corruption)
    }

    fn parse_new_file(input: &[u8], level: &mut u32, f: &mut FileMetaData) -> Option<usize> {
        let mut l = 0;
        if let Some((val, len)) = get_level(&input[l..]) {
            l += len;
            *level = val;
        } else {
            return None
        }

        if let Ok((val, len)) = get_varint64(&input, l, input.len()) {
            l += len;
            f.number = val;
        } else {
            return None;
        }

        if let Ok((val, len)) = get_varint64(&input, l, input.len()) {
            l += len;
            f.number = val;
        } else {
            return None;
        }

        Some(l)
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