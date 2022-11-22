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
use crate::coding::encode_fixed32;
use crate::env::WritableFile;
use crate::log_format::{kBlockSize, kHeaderSize, kMaxRecordType, RecordType};
use crate::slice::Slice;
use crate::Result;
use crate::util::crc;

pub struct Writer {
    dest: Box<dyn WritableFile>,

    block_offset: usize,

    type_crc: [u8; kMaxRecordType as usize + 1]
}

pub fn init_type_crc(type_crc: &mut [u8]) {
    for i in 0..=kMaxRecordType {
        type_crc[i as usize] = i as u8;
    }
}

impl Writer {

    pub fn new(dest: Box<dyn WritableFile>) -> Self {
        Self::new_with_block_offset(dest, 0)
    }

    pub fn new_with_block_offset(dest: Box<dyn WritableFile>, block_offset: usize) -> Self{
        let mut type_crc = [0 as u8; kMaxRecordType as usize + 1];
        init_type_crc(&mut type_crc);
        Writer {
            dest,
            block_offset,
            type_crc
        }
    }

    /// Fragment the record if necessary and emit it.  Note that if slice
    /// is empty, we still want to iterate once to emit a single
    /// zero-length record
    pub fn add_record(&mut self, slice: &Slice) -> Result<()> {
        let data = slice.data();
        let mut left = slice.size();
        let mut offset = 0;

        let mut begin = true;

        loop {
            let leftover = kBlockSize - self.block_offset;
            if leftover < kHeaderSize {
                if leftover > 0 {
                    // Switch to a new block
                    self.dest.append(&Slice::from_bytes(&vec![0 as u8; leftover]))?
                }
                self.block_offset = 0;
            }

            let avail = kBlockSize - self.block_offset - kHeaderSize;
            let fragment_length = if left < avail { left } else { avail };
            let record_type;
            let end = left == fragment_length;
            if begin && end {
                record_type = RecordType::kFullType;
            } else if begin {
                record_type = RecordType::kFirstType;
            } else if end {
                record_type = RecordType::kLastType;
            } else {
                record_type = RecordType::kMiddleType
            }

            self.emit_physical_record(record_type, &data[offset..(offset + fragment_length)])?;
            offset += fragment_length;
            left -= fragment_length;
            begin = false;
            if left <= 0 {
                return Ok(())
            }
        }
    }

    fn emit_physical_record(&mut self, record_type: RecordType, data: &[u8]) -> Result<()> {
        let mut buf = vec![0 as u8; kHeaderSize];
        let length = data.len();
        buf[4] = (length & 0xff) as u8;
        buf[5] = (length >> 8) as u8;
        buf[6] = record_type as u8;

        // Compute the crc of the record type and the payload.
        let mut crc = crc::extend(self.type_crc[record_type as usize], data);
        // Adjust for storage
        crc = crc::mask(crc);

        encode_fixed32(&mut buf, crc, 0);

        // Write the header and the payload
        self.dest.append(&Slice::from_bytes(&buf))?;

        self.dest.append(&Slice::from_bytes(data))?;

        self.dest.flush()?;

        self.block_offset += kHeaderSize + length;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::env::MemoryWritableFile;
    use super::*;

    #[test]
    fn test() {
        let writable_file = Box::new(MemoryWritableFile::new(Vec::new()));
        let mut writer = Writer::new(writable_file);
        writer.add_record(&Slice::from_str("hello world")).expect("write failed");
    }
}