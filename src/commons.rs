extern crate twox_hash;
extern crate byteorder;

use self::byteorder::{BigEndian, ByteOrder};
use self::twox_hash::XxHash;
use std::fmt::Write;
use std::hash::Hasher;

pub const HASH_SIZE: usize = 8;

pub struct CHasher
{
    internal: XxHash,
}

impl CHasher
{
    pub fn new() -> CHasher
    {
        CHasher { internal: XxHash::with_seed(0) }
    }

    pub fn update(&mut self, data: &[u8])
    {
        self.internal.write(data);
    }

    pub fn finalize(self, output: &mut [u8])
    {
        if output.len() < HASH_SIZE
        {
            panic!("Hash finalizer buffer too short!");
        }
        BigEndian::write_u64(output, self.internal.finish());
    }
}

#[allow(dead_code)]
pub fn base16(data: &[u8]) -> String
{
    let mut s = String::new();
    for &b in data
    {
        if b < 0x10u8
        {
            write!(&mut s, "0").unwrap();
        }
        write!(&mut s, "{:x}", b).unwrap();
    }
    return s;
}
