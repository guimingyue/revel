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
use std::fs::read;
use std::io::Write;
use crate::coding::decode_fix32;
use crate::env::SequentialFile;
use crate::Error::IOError;
use crate::log_format::{kBlockSize, kHeaderSize, kMaxRecordType, RecordType};
use crate::log_format::RecordType::{kLastType, kMiddleType, kZeroType};

use crate::slice::Slice;
use crate::util::crc;
use crate::util::crc::extend;

const K_FULL_TYPE: u32 = RecordType::kFullType as u32;

const K_FIRST_TYPE: u32 = RecordType::kFirstType as u32;

const K_MIDDLE_TYPE: u32 = RecordType::kMiddleType as u32;

const K_LAST_TYPE: u32 = RecordType::kLastType as u32;

const kEof: u32 = (kMaxRecordType + 1) as u32;

const kBadRecord: u32 = (kMaxRecordType + 2) as u32;

pub struct Reader {

    file: Box<dyn SequentialFile>,

    checksum: bool,

    buffer: Vec<u8>,

    eof: bool,

    last_record_offset: u64,

    end_of_buffer_offset: u64,

    initial_offset: u64,

    resyncing: bool,

    skip_size: u64

}

impl Reader {

    pub fn new(file: Box<dyn SequentialFile>, checksum: bool, initial_offset: u64) -> Self {
        Reader {
            file,
            checksum,
            buffer: vec![0; kBlockSize],
            eof: false,
            last_record_offset: 0,
            end_of_buffer_offset: 0,
            initial_offset,
            resyncing: initial_offset > 0,
            skip_size: 0
        }
    }

    pub fn read_record(&mut self, scratch: &mut Vec<u8>) -> crate::Result<Slice> {
        // todo!() skip to last record offset
        scratch.clear();

        let mut in_fragmented_record = false;
        let mut prospective_record_offset: u64 = 0;
        let mut fragment = Slice::from_empty();
        loop {
            let record_type = self.read_physical_record(&mut fragment);
            let physical_record_offset = self.end_of_buffer_offset - self.skip_size - kHeaderSize as u64 - fragment.size() as u64;

            if self.resyncing {
                if record_type == kMiddleType as u32 {
                    continue;
                } else if record_type == kLastType as u32 {
                    self.resyncing = false;
                } else {
                    self.resyncing = false;
                }
            }
            match record_type {
                K_FULL_TYPE => {
                    self.last_record_offset = physical_record_offset;
                    scratch.clear();
                    return Ok(fragment)
                },
                K_FIRST_TYPE => {
                    in_fragmented_record = true;
                    prospective_record_offset = physical_record_offset;
                    scratch.extend_from_slice(fragment.data());
                },
                K_MIDDLE_TYPE => {
                    if !in_fragmented_record {
                        // todo!()
                    } else {
                        scratch.extend_from_slice(fragment.data());
                    }
                },
                K_LAST_TYPE => {
                    if !in_fragmented_record {
                        // todo!()
                    } else {
                        scratch.extend_from_slice(fragment.data());
                        self.last_record_offset = prospective_record_offset;
                        return Ok(Slice::from_bytes(scratch.as_slice()));
                    }
                },
                kEof => {
                    if in_fragmented_record {
                        // This can be caused by the writer dying immediately after
                        // writing a physical record but before completing the next; don't
                        // treat it as a corruption, just ignore the entire logical record.
                        scratch.clear();
                    }
                    return Ok(Slice::from_empty());
                }
                _ => {
                    in_fragmented_record = false;
                    scratch.clear();

                    break;
                }
            }
        }
        Err(IOError)
    }

    fn read_physical_record(&mut self, fragment: &mut Slice) -> u32 {
        self.buffer.clear();
        self.skip_size = 0;
        if self.eof {
            return kEof;
        }

        match self.file.read(&mut self.buffer) {
            Ok(slice) => {
                self.end_of_buffer_offset = self.end_of_buffer_offset + slice.size() as u64;
                if slice.size() < kBlockSize {
                    self.eof = true;
                }
                // todo!() this slice is read from file
                let slice = Slice::from_empty();
                let header = slice.data();
                let a = (header[4] & 0xff) as u32;
                let b = (header[5] & 0xff) as u32;
                let type_ = header[6] as i32;
                let length = a | (b << 8);
                if kHeaderSize + length as usize > slice.size() {
                    // todo!() error
                    return kEof;
                }

                if type_ == kZeroType as i32 && length == 0 {
                    // todo!() Skip zero length record without reporting any dorps ...
                    self.buffer.clear();
                    return kBadRecord;
                }

                if self.checksum {
                    let expected_crc = crc::unmask(decode_fix32(&header[0..4]));
                    let actual_crc = crc::value(&header[4..]);
                    if actual_crc != expected_crc {
                        // todo!()
                        return kBadRecord;
                    }
                }
                let data = &header[(kHeaderSize + length as usize)..];
                if (self.end_of_buffer_offset - data.len() as u64 - kHeaderSize as u64 - length as u64) < self.initial_offset {
                    self.skip_size = slice.size() as u64;
                    self.buffer.clear();
                    return kBadRecord;
                }

                *fragment = Slice::from_bytes(data);
                return type_ as u32;
            },
            Err(_) => {
                self.buffer.clear();
                self.eof = true;
                return kEof;
            }
        }

    }


}



#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {

    }
}