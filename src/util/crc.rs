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

use crc::{Crc, CRC_32_ISCSI};

pub const CASTAGNOLI: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

pub fn value(data: &[u8]) -> u32 {
    CASTAGNOLI.checksum(data)
}

// todo!() define as macro instead
pub fn extend(init: u8, data: &[u8]) -> u32 {
    let mut digest = CASTAGNOLI.digest();
    digest.update(&[init]);
    digest.update(data);
    digest.finalize()
}

const kMaskDelta: u32 = 0xa282ead8;

/// Return a masked representation of crc.
///
/// Motivation: it is problematic to compute the CRC of a string that
/// contains embedded CRCs.  Therefore we recommend that CRCs stored
/// somewhere (e.g., in files) should be masked before being stored.
pub const fn mask(crc: u32) -> u32 {
    (((crc >> 15) as u64 | (crc << 17) as u64) + kMaskDelta as u64) as u32
}

/// Return the crc whose masked representation is masked_crc.
pub const fn unmask(masked_crc: u32) -> u32 {
    let rot = (masked_crc as i64 - kMaskDelta as i64) as u32;
    (rot >> 17) | (rot << 15)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crc_standard_results() {
        // From rfc3720 section B.4.
        let buf = vec![0; 32];
        assert_eq!(0x8a9136aa, value(&buf));
        let buf = vec![0xff; 32];
        assert_eq!(0x62a8ab43, value(&buf));

        let mut buf = vec![0xffu8; 32];
        for i in 0..32 {
            buf[i as usize] = i;
        }
        assert_eq!(0x46dd794e, value(&buf));

        for i in 0..32 {
            buf[i as usize] = 31 - i;
        }
        assert_eq!(0x113fdb5c, value(&buf));

        let data:[u8; 48] = [
            0x01, 0xc0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00,
            0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x18, 0x28, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        assert_eq!(0xd9963a56, value(&data));
    }

    #[test]
    fn test_crc_values() {
        assert_ne!(value("a".as_bytes()), value("foo".as_bytes()));
    }

    #[test]
    fn test_crc_extend() {
        assert_eq!(value("hello world".as_bytes()), extend('h' as u8, "ello world".as_bytes()));
    }

    #[test]
    fn test_crc_mask() {
        let crc = value("foo".as_bytes());
        assert_ne!(crc, mask(crc));
        assert_ne!(crc, mask(mask(crc)));
        assert_eq!(crc, unmask(mask(crc)));
        assert_eq!(crc, unmask(unmask(mask(mask(crc)))));
    }

    #[test]
    fn test_hello_world() {
        let bytes = "hello world".as_bytes();
        let crc = extend(1, bytes);
        let mut digest = CASTAGNOLI.digest();
        digest.update(&[1]);
        digest.update(bytes);
        let crc1 = digest.finalize();
        assert_eq!(crc, crc1);
        assert_eq!(mask(crc), mask(crc1));
        assert_eq!(unmask(mask(crc)), unmask(mask(crc1)));
    }
}
