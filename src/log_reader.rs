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

    buffer: RefCell<Vec<u8>>,

    eof: RefCell<bool>,

    last_record_offset: RefCell<u64>,

    end_of_buffer_offset: RefCell<u64>,

    initial_offset: u64,

    resyncing: bool,

    skip_size: RefCell<u64>

}

impl Reader {

    pub fn new(file: Box<dyn SequentialFile>, checksum: bool, initial_offset: u64) -> Self {
        Reader {
            file,
            checksum,
            buffer: RefCell::new(vec![0; kBlockSize]),
            eof: RefCell::new(false),
            last_record_offset: RefCell::new(0),
            end_of_buffer_offset: RefCell::new(0),
            initial_offset,
            resyncing: initial_offset > 0,
            skip_size: RefCell::new(0)
        }
    }

    pub fn read_record<'a, 'b>(&'a mut self, scratch: &'b mut Vec<u8>) -> crate::Result<Slice<'b>> {
        // todo!() skip to last record offset
        scratch.clear();

        let mut in_fragmented_record = false;
        let mut prospective_record_offset: u64 = 0;
        loop {
            let physical_record_offset = 0; //*self.end_of_buffer_offset.borrow() - *self.skip_size.borrow() - kHeaderSize as u64 - fragment.size() as u64;

            /*if self.resyncing {
                if record_type == kMiddleType as u32 {
                    continue;
                } else if record_type == kLastType as u32 {
                    self.resyncing = false;
                } else {
                    self.resyncing = false;
                }
            }*/
            //let buf = self.buffer.borrow();
            match self.read_physical_record() {
                Ok((record_type, data_pos)) => {
                    let buf = self.buffer.borrow();
                    match record_type {
                        K_FULL_TYPE => {
                            self.last_record_offset.replace(physical_record_offset);
                            scratch.clear();
                            scratch.extend_from_slice(&buf[kHeaderSize..]);
                            return Ok(Slice::from_bytes(&scratch[..]));
                        },
                        K_FIRST_TYPE => {
                            in_fragmented_record = true;
                            prospective_record_offset = physical_record_offset;
                            scratch.extend_from_slice(&buf[data_pos..]);
                        },
                        K_MIDDLE_TYPE => {
                            if !in_fragmented_record {
                                // todo!()
                            } else {
                                scratch.extend_from_slice(&buf[data_pos..]);
                            }
                        },
                        K_LAST_TYPE => {
                            if !in_fragmented_record {
                                // todo!()
                            } else {
                                scratch.extend_from_slice(&buf[data_pos..]);
                                self.last_record_offset.replace(prospective_record_offset);
                                return Ok(Slice::from_bytes(scratch.as_slice()));
                            }
                        },
                        _ => {
                            break;
                        }
                    }
                },
                Err(err_type) => {
                    match err_type {
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
            }
        }
        Err(IOError)
    }

    fn read_physical_record(&self) -> Result<(u32, usize), u32> {
        self.skip_size.replace(0);
        if *self.eof.borrow() {
            return Err(kEof);
        }

        let mut buf_len = 0;
        {
            let mut buf = self.buffer.borrow_mut();
            let res = self.file.read(buf.as_mut_slice());
            match res {
                Ok(slice) => {
                    buf_len = slice.size();
                },
                Err(_) => {
                    self.eof.replace(true);
                    return Err(kEof);
                }
            }
        }

        let end_of_buffer_offset = self.end_of_buffer_offset.take();
        self.end_of_buffer_offset.replace(end_of_buffer_offset + buf_len as u64);
        {
            let buf = self.buffer.borrow();
            let size = buf.len();
            if size < kBlockSize {
                self.eof.replace(true);
            }

            let header = &buf[..buf_len];
            let a = (header[4] & 0xff) as u32;
            let b = (header[5] & 0xff) as u32;
            let type_ = header[6] as i32;
            let length = a | (b << 8);
            if kHeaderSize + length as usize > size {
                // todo!() error
                return Err(kEof);
            }

            if type_ == kZeroType as i32 && length == 0 {
                // todo!() Skip zero length record without reporting any dorps ...
                return Err(kBadRecord);
            }

            if self.checksum {
                let expected_crc = crc::unmask(decode_fix32(&header[0..4]));
                let actual_crc = crc::value(&header[6..]);
                if actual_crc != expected_crc {
                    // todo!()
                    return Err(kBadRecord);
                }
            }
            let prefix_removed = &header[(kHeaderSize + length as usize)..];
            if (end_of_buffer_offset + buf_len as u64 - prefix_removed.len() as u64 - kHeaderSize as u64 - length as u64) < self.initial_offset {
                self.skip_size.replace(size as u64);
                return Err(kBadRecord);
            }

            return Ok((type_ as u32, length as usize));
        }
    }


}



#[cfg(test)]
mod tests {
    use std::rc::Rc;
    use crate::env::MemorySequentialFile;
    use super::*;

    #[test]
    fn test() {
        let memory = Rc::new(vec![129, 221, 1, 7, 11, 0, 1, 104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100]);
        let file = MemorySequentialFile::new(memory);
        let sequential_file = Box::new(file);
        let mut reader = Reader::new(sequential_file, true, 0);
        let mut buf = vec![];
        let slice = reader.read_record(&mut buf).expect("error");
        unsafe {
            let str = String::from_utf8_unchecked(slice.data().to_vec());
            assert_eq!(str, "hello world");
        }
    }
}