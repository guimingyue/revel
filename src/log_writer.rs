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

use std::borrow::BorrowMut;
use std::cell::{RefCell, RefMut};
use std::fs::File;
use std::io::Write;
use crate::coding::encode_fixed32;
use crate::log_format::{kBlockSize, kHeaderSize, kMaxRecordType, RecordType};
use crate::slice::Slice;
use crate::Result;
use crate::util::crc;

pub struct Writer {
    dest: Box<dyn WritableFile>,

    block_offset: usize,

    type_crc: [u32; kMaxRecordType + 1]
}

pub fn init_type_crc(type_crc: &mut [u32]) {
    for i in 0..=kMaxRecordType {
        type_crc[i] = crc::value(&[i as u8]);
    }
}

impl Writer {

    pub fn new(dest: Box<dyn WritableFile>) -> Self {
        Self::new_with_block_offset(dest, 0)
    }

    pub fn new_with_block_offset(dest: Box<dyn WritableFile>, block_offset: usize) -> Self{
        let mut type_crc = [0 as u32; kMaxRecordType + 1];
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
        let status = self.dest.append(&Slice::from_bytes(&buf))?;

        self.dest.append(&Slice::from_bytes(data))?;

        self.dest.flush()?;

        self.block_offset += kHeaderSize + length;

        Ok(())
    }
}

pub trait WritableFile {

    fn append(&mut self, data: &Slice) -> Result<()>;

    fn flush(&mut self) -> Result<()>;

    fn close(&self) -> Result<()>;

    fn sync(&self) -> Result<()>;

}

const kWritableFileBufferSize: usize = 65536;

pub struct PosixWritableFile {

    // buf_[0, pos_ - 1] contains data to be written to fd_.
    buf: Vec<u8>,
    pos: usize,
    file: RefCell<File>,

    // True if the file's name starts with MANIFEST.
    is_manifest: bool,
    filename: String,
    // The directory of filename_.
    dirname: String
}

fn write_unbuffered(mut file: RefMut<File>, data: &[u8], size: usize) -> Result<()> {
    let result = file.write(&data[0..size]);
    match result {
        Ok(write_result) => Ok(()),
        Err(err) => Err(crate::Error::from(err))
    }
}

impl PosixWritableFile {

    pub fn new(filename: &str, file: File) -> Self {
        PosixWritableFile {
            pos: 0,
            buf: vec![0; kWritableFileBufferSize],
            file: RefCell::new(file),
            // todo!() filename start with MANIFEST
            filename: filename.to_string(),
            is_manifest: false,
            // todo!() parse dirname from filename
            dirname: "".to_string()
        }
    }

    fn flush_buffer(&mut self) -> Result<()>{
        let result = write_unbuffered(self.file.borrow_mut(), self.buf.as_slice(), self.pos);
        self.pos = 0;
        result
    }
}

impl WritableFile for PosixWritableFile {
    fn append(&mut self, data: &Slice) -> Result<()> {
        let write_data = data.data();
        let write_size = data.size();
        let mut write_offset = 0;
        let copy_size = std::cmp::min(write_size, kWritableFileBufferSize - self.pos);
        let size = self.buf.write(&write_data[..copy_size]).expect("");
        self.pos += size;
        write_offset += size;
        if write_size <= write_offset {
            return Ok(());
        }

        // Can't fit in buffer, so need to do at least one write.
        self.flush_buffer()?;

        if write_size - write_offset < kWritableFileBufferSize {
            self.buf.write(&write_data[size..]).expect("");
            return Ok(());
        }
        write_unbuffered(self.file.borrow_mut(), write_data, write_size - write_offset)
    }

    fn flush(&mut self) -> Result<()> {
        self.flush_buffer()
    }

    fn close(&self) -> Result<()> {
        drop(self.file.borrow_mut());
        Ok(())
    }

    fn sync(&self) -> Result<()> {
        self.file.borrow_mut().sync_all()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {

    }
}