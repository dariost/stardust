extern crate byteorder;
extern crate tiny_keccak;

use disk_file::{CHUNK_SIZE, HASH_SIZE};
use disk_file::DiskFile;
use self::byteorder::{BigEndian, ByteOrder};
use self::tiny_keccak::Keccak;
use std::collections::HashMap;
use std::collections::HashSet;
use std::path::PathBuf;

const MAGIC_NUMBER: u32 = 1400136018;
const PROTOCOL_VERSION: u16 = 0;
const ACTION_SEND_DESCRIPTION: u8 = 0;

pub struct Pool
{
    data: Vec<DiskFile>,
    file_index_map: HashMap<u8, usize>,
    update_left: HashSet<(u8, u32)>,
    read_only: bool,
    map_hash_write: HashMap<[u8; HASH_SIZE], Vec<(u8, u32)>>,
    map_hash_read: HashMap<[u8; HASH_SIZE], (u8, u32)>,
    cache_set_sizes: Vec<u64>,
    cache_file_path: Vec<PathBuf>,
}

pub struct FileDescription
{
    pub id: u8,
    pub name: String,
    pub name_hash: [u8; HASH_SIZE],
    pub hash: [u8; HASH_SIZE],
    pub chunk: Vec<[u8; HASH_SIZE]>,
    pub last_chunk_size: u16,
}

impl Pool
{
    pub fn new(files: &Vec<PathBuf>, first_packet: Option<Vec<u8>>) -> Pool
    {
        let have_sizes: bool = first_packet.is_some();
        let mut p = Pool {
            data: Vec::new(),
            read_only: !have_sizes,
            file_index_map: HashMap::new(),
            update_left: HashSet::new(),
            map_hash_write: HashMap::new(),
            map_hash_read: HashMap::new(),
            cache_set_sizes: Vec::new(),
            cache_file_path: files.clone(),
        };
        if have_sizes
        {
            p.update_tables(&first_packet.unwrap());
        }
        {
            let sizes = &p.cache_set_sizes;
            if have_sizes && sizes.len() != files.len()
            {
                panic!("Files and sizes mismatch!");
            }
            if files.len() > 255
            {
                panic!("stardust can only handle up to 255 files");
            }
            for i in 0..files.len()
            {
                let tmp_size = if have_sizes
                {
                    Some(sizes[i])
                }
                else
                {
                    None
                };
                p.data.push(DiskFile::new(&files[i], tmp_size));
            }
        }
        for i in 0..p.data.len()
        {
            for j in 0..p.data[i].get_num_chunks()
            {
                let tmp_chunk_hash = p.data[i].get_chunk_hash(j);
                if p.map_hash_read.contains_key(&tmp_chunk_hash)
                {
                    let &(tmp_file_id, tmp_chunk_id) = p.map_hash_read.get(&tmp_chunk_hash).unwrap();
                    if p.data[i].read_chunk(j) != p.data[tmp_file_id as usize].read_chunk(tmp_chunk_id as usize)
                    {
                        panic!("FATAL: Hash collision!");
                    }
                }
                else
                {
                    p.map_hash_read.insert(tmp_chunk_hash, (i as u8, j as u32));
                }
            }
        }
        return p;
    }

    fn get_description(&self) -> Vec<FileDescription>
    {
        let mut v: Vec<FileDescription> = Vec::new();
        for i in 0..self.data.len()
        {
            v.push(FileDescription::new(&self.data[i], i as u8));
        }
        return v;
    }

    pub fn generate_binary_description(&self) -> Vec<Vec<u8>>
    {
        let description = self.get_description();
        let mut bin_data: Vec<Vec<u8>> = Vec::new();
        let mut file_index: usize = 0;
        let mut chunk_index: usize = 0;
        while file_index < description.len()
        {
            let mut v: Vec<u8> = Vec::new();
            let mut buffer: [u8; 128] = [0; 128];
            // Magic number
            BigEndian::write_u32(&mut buffer[0..4], MAGIC_NUMBER);
            v.extend_from_slice(&buffer[0..4]);
            // Protocol version
            BigEndian::write_u16(&mut buffer[0..2], PROTOCOL_VERSION);
            v.extend_from_slice(&buffer[0..2]);
            // Action
            v.push(ACTION_SEND_DESCRIPTION);
            // Number of files
            v.push(description.len() as u8);
            // Files description
            for desc in &description
            {
                v.push(desc.id);
                v.extend_from_slice(&desc.name_hash);
                v.extend_from_slice(&desc.hash);
                BigEndian::write_u32(&mut buffer[0..4], desc.chunk.len() as u32);
                v.extend_from_slice(&buffer[0..4]);
                BigEndian::write_u16(&mut buffer[0..2], desc.last_chunk_size);
                v.extend_from_slice(&buffer[0..2]);
            }
            // Chunks information
            'outer: while file_index < description.len()
            {
                while chunk_index < description[file_index].chunk.len()
                {
                    if v.len() >= 60000
                    {
                        break 'outer;
                    }
                    v.push(description[file_index].id as u8);
                    v.extend_from_slice(&description[file_index].chunk[chunk_index]);
                    BigEndian::write_u32(&mut buffer[0..4], chunk_index as u32);
                    v.extend_from_slice(&buffer[0..4]);
                    chunk_index += 1;
                }
                chunk_index = 0;
                file_index += 1;
            }
            bin_data.push(v);
        }
        return bin_data;
    }

    fn unwrap_description(&self, data: &Vec<u8>, i: usize) -> (u8, [u8; HASH_SIZE], [u8; HASH_SIZE], u32, u16)
    {
        let struct_size: usize = 7 + 2 * HASH_SIZE;
        let file_id: u8 = data[i * struct_size + 1];
        let mut name_hash: [u8; HASH_SIZE] = [0; HASH_SIZE];
        name_hash.copy_from_slice(&data[(i * struct_size + 2)..(i * struct_size + 2 + HASH_SIZE)]);
        let mut file_hash: [u8; HASH_SIZE] = [0; HASH_SIZE];
        file_hash.copy_from_slice(&data[(i * struct_size + 2 + HASH_SIZE)..(i * struct_size + 2 + 2 * HASH_SIZE)]);
        let num_chunks: u32 = BigEndian::read_u32(&data[(i * struct_size + 2 + 2 * HASH_SIZE)..(i * struct_size + 6 +
                                                                                                2 * HASH_SIZE)]);
        let last_chunk_size: u16 = BigEndian::read_u16(&data[(i * struct_size + 6 + 2 * HASH_SIZE)..(i * struct_size + 8 +
                                                                                                     2 * HASH_SIZE)]);
        return (file_id, name_hash, file_hash, num_chunks, last_chunk_size);
    }

    pub fn update_tables(&mut self, data: &Vec<u8>) -> bool
    {
        if self.read_only
        {
            panic!("Writing on read-only table!");
        }
        if self.cache_set_sizes.len() == 0
        {
            self.cache_set_sizes = vec![0, self.cache_file_path.len() as u64];
            let num_files: usize = data[0] as usize;
            for i in 0..num_files
            {
                let (_, name_hash, _, num_chunks, mut last_chunk_size) = self.unwrap_description(data, i);
                last_chunk_size = last_chunk_size % CHUNK_SIZE as u16;
                let file_size = num_chunks as u64 * CHUNK_SIZE as u64 + last_chunk_size as u64;
                for j in 0..self.cache_file_path.len()
                {
                    let mut name_sha3 = Keccak::new_sha3_256();
                    let req_file_name = String::from(self.cache_file_path[j].file_name().unwrap().to_str().unwrap_or(""));
                    if req_file_name == ""
                    {
                        panic!("Error while decoding filename");
                    }
                    let mut req_file_name_hash: [u8; HASH_SIZE] = [0; HASH_SIZE];
                    name_sha3.update(req_file_name.as_bytes());
                    name_sha3.finalize(&mut req_file_name_hash);
                    if req_file_name_hash == name_hash
                    {
                        self.cache_set_sizes[j] = file_size;
                        break;
                    }
                }
            }
            for i in 0..self.cache_set_sizes.len()
            {
                if self.cache_set_sizes[i] == 0
                {
                    panic!("The server doesn't have all requested files!");
                }
            }
            return false;
        }
        if self.file_index_map.len() == 0
        {
            let num_files: usize = data[0] as usize;
            for i in 0..num_files
            {
                let (file_id, name_hash, _, _, _) = self.unwrap_description(data, i);
                for j in 0..self.data.len()
                {
                    if name_hash == self.data[j].get_name_hash()
                    {
                        self.file_index_map.insert(file_id, j);
                        for k in 0..self.data[j].get_num_chunks()
                        {
                            self.update_left.insert((file_id, k as u32));
                        }
                        break;
                    }
                }
            }
        }
        let struct_size: usize = 7 + 2 * HASH_SIZE;
        let first_index: usize = struct_size * data[0] as usize + 1;
        let desc_size: usize = 5 + HASH_SIZE;
        let num_desc = (data.len() - first_index) / desc_size;
        if (data.len() - first_index) % desc_size != 0
        {
            panic!("Received a corrupted packet");
        }
        for i in 0..num_desc
        {
            let local_slice = &data[(first_index + i * desc_size)..(first_index + (i + 1) * desc_size)];
            let fid: u8 = local_slice[0];
            let mut chunk_hash: [u8; HASH_SIZE] = [0; HASH_SIZE];
            chunk_hash.clone_from_slice(&local_slice[1..(HASH_SIZE + 1)]);
            let chunk_id: u32 = BigEndian::read_u32(&local_slice[(HASH_SIZE + 1)..(HASH_SIZE + 5)]);
            if self.update_left.contains(&(fid, chunk_id))
            {
                if !self.map_hash_write.contains_key(&chunk_hash)
                {
                    let mut chunk_hash2: [u8; HASH_SIZE] = [0; HASH_SIZE];
                    chunk_hash2.clone_from_slice(&chunk_hash);
                    self.map_hash_write.insert(chunk_hash2, Vec::new());
                }
                self.map_hash_write.get_mut(&chunk_hash).unwrap().push((fid, chunk_id));
                self.update_left.remove(&(fid, chunk_id));
            }
        }
        return self.update_left.is_empty();
    }

    pub fn process_chunk(&mut self, chunk_hash: [u8; HASH_SIZE], chunk: &Vec<u8>)
    {
        if self.read_only
        {
            panic!("Writing on read-only files!");
        }
        if !self.map_hash_write.contains_key(&chunk_hash)
        {
            return;
        }
        {
            let vec_mapped = self.map_hash_write.get(&chunk_hash).unwrap();
            for i in vec_mapped
            {
                let &(unmapped_fid, chunk_id) = i;
                if !self.file_index_map.contains_key(&unmapped_fid)
                {
                    continue;
                }
                let mapped_fid = self.file_index_map.get(&unmapped_fid).unwrap().clone();
                self.data[mapped_fid].write_chunk(chunk_id as usize, chunk);
            }
        }
        self.map_hash_write.remove(&chunk_hash);

    }

    pub fn get_chunk(&mut self, chunk_hash: [u8; HASH_SIZE]) -> Option<Vec<u8>>
    {
        if !self.map_hash_read.contains_key(&chunk_hash)
        {
            return None;
        }
        let &(i, j) = self.map_hash_read.get(&chunk_hash).unwrap();
        return Some(self.data[i as usize].read_chunk(j as usize));
    }
}

impl FileDescription
{
    pub fn new(diskfile: &DiskFile, _id: u8) -> FileDescription
    {
        let mut fd = FileDescription {
            id: _id,
            name: diskfile.get_file_name(),
            name_hash: diskfile.get_name_hash(),
            hash: diskfile.get_file_hash(),
            chunk: Vec::new(),
            last_chunk_size: diskfile.get_chunk_size(diskfile.get_num_chunks() - 1) as u16,
        };
        for i in 0..diskfile.get_num_chunks()
        {
            fd.chunk.push(diskfile.get_chunk_hash(i));
        }
        return fd;
    }
}
