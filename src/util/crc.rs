use crc::{Crc, CRC_32_ISCSI};

pub const CASTAGNOLI: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

pub const fn value(data: &[u8]) -> u32 {
    CASTAGNOLI.checksum(data)
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
}
