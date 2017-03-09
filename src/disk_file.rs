use commons::CHasher;
use commons::HASH_SIZE;
use commons::base16;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::BufReader;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::Path;
use std::time::Instant;

pub const CHUNK_SIZE: usize = 60000;

pub struct DiskFile
{
    file: File,
    file_path: String,
    read_only: bool,
    global_hash: [u8; HASH_SIZE],
    chunk_hash: Vec<[u8; HASH_SIZE]>,
    file_size: u64,
    file_name: String,
    name_hash: [u8; HASH_SIZE],
}

impl DiskFile
{
    pub fn new(path: &Path, set_size: Option<u64>, hash_folder: &str) -> DiskFile
    {
        let ro = set_size.is_none();
        let mut diskfile = DiskFile {
            file: match OpenOptions::new()
                      .write(!ro)
                      .read(true)
                      .create(!ro)
                      .open(path)
            {
                Err(why) => panic!("Cannot open {}: {}", path.to_str().unwrap_or("NULL"), why),
                Ok(result) => result,
            },
            read_only: ro,
            global_hash: [0; HASH_SIZE],
            name_hash: [0; HASH_SIZE],
            chunk_hash: Vec::new(),
            file_path: path.to_str().unwrap_or("NULL").to_string(),
            file_size: 0,
            file_name: String::from(path.file_name()
                                        .unwrap()
                                        .to_str()
                                        .unwrap_or("")),
        };
        if diskfile.file_name == ""
        {
            panic!("Cannot get filename for {}", path.to_str().unwrap_or("NULL"));
        }
        println!("Calculating hash for {}", diskfile.file_name);
        let mut name_sha3 = CHasher::new();
        name_sha3.update(diskfile.file_name.as_bytes());
        name_sha3.finalize(&mut diskfile.name_hash);
        if !diskfile.read_only
        {
            let _ = diskfile.file.set_len(set_size.unwrap());
        }
        let _ = diskfile.file.sync_all();
        let mut buff_file = BufReader::new(match OpenOptions::new().read(true).open(path)
        {
                                               Err(why) => panic!("Cannot open {}: {}", path.to_str().unwrap_or("NULL"), why),
                                               Ok(result) => result,
                                           });
        let mut file_sha3 = CHasher::new();
        let mut byte_count: u64 = 0;
        let mut started = Instant::now();
        loop
        {
            if started.elapsed().as_secs() > 1
            {
                started = Instant::now();
                println!("Hashed {} kibibytes", byte_count / 1024);
            }
            let mut chunk_sha3 = CHasher::new();
            let mut buffer: [u8; CHUNK_SIZE] = [0; CHUNK_SIZE];
            let mut buffer_index: usize = 0;
            loop
            {
                match buff_file.read(&mut buffer[buffer_index..CHUNK_SIZE])
                {
                    Err(why) => panic!("Error while reading {}: {}", diskfile.file_path, why),
                    Ok(0) => break,
                    Ok(n) => buffer_index += n,
                };
            }
            byte_count += buffer_index as u64;
            if set_size.is_some() && byte_count > set_size.unwrap()
            {
                buffer_index -= (byte_count - set_size.unwrap() as u64) as usize;
            }
            if buffer_index != 0
            {
                chunk_sha3.update(&buffer[0..buffer_index]);
                file_sha3.update(&buffer[0..buffer_index]);
            }
            let mut tmp_chunk_sha3: [u8; HASH_SIZE] = [0; HASH_SIZE];
            chunk_sha3.finalize(&mut tmp_chunk_sha3);
            diskfile.chunk_hash.push(tmp_chunk_sha3);
            diskfile.file_size += buffer_index as u64;
            if buffer_index != CHUNK_SIZE
            {
                break;
            }
        }
        file_sha3.finalize(&mut diskfile.global_hash);
        if ro
        {
            let mut file_hash = match OpenOptions::new().write(true).create(true).open(hash_folder.to_owned() + "/" + diskfile.file_name.as_str() +
                                                                   ".xxh64")
            {
                Err(why) => panic!("Cannot open {}: {}", path.to_str().unwrap_or("NULL"), why),
                Ok(result) => result,
            };
            let _ = file_hash.write_all(base16(&diskfile.global_hash).as_bytes());
        }
        diskfile
    }

    pub fn get_name_hash(&self) -> [u8; HASH_SIZE]
    {
        let mut v: [u8; HASH_SIZE] = [0; HASH_SIZE];
        v.clone_from_slice(&self.name_hash);
        v
    }

    pub fn get_chunk_hash(&self, chunk_number: usize) -> [u8; HASH_SIZE]
    {
        let mut v: [u8; HASH_SIZE] = [0; HASH_SIZE];
        v.clone_from_slice(&self.chunk_hash[chunk_number]);
        v
    }

    pub fn get_file_hash(&self) -> [u8; HASH_SIZE]
    {
        let mut v: [u8; HASH_SIZE] = [0; HASH_SIZE];
        v.clone_from_slice(&self.global_hash);
        v
    }

    pub fn get_file_name(&self) -> String
    {
        self.file_name.clone()
    }

    pub fn get_num_chunks(&self) -> usize
    {
        self.chunk_hash.len()
    }

    pub fn get_chunk_size(&self, chunk_number: usize) -> usize
    {
        if chunk_number != self.chunk_hash.len() - 1
        {
            return CHUNK_SIZE;
        }
        if self.file_size % (CHUNK_SIZE as u64) == 0
        {
            return CHUNK_SIZE;
        }
        (self.file_size % (CHUNK_SIZE as u64)) as usize
    }

    pub fn read_chunk(&mut self, chunk_number: usize) -> Vec<u8>
    {
        if chunk_number >= self.chunk_hash.len()
        {
            panic!("{} -> cannot read chunk {}: doesn't exist", self.file_path, chunk_number);
        }
        let chunk_size = self.get_chunk_size(chunk_number);
        let mut v: Vec<u8> = vec![0; chunk_size];
        let _ = match self.file.seek(SeekFrom::Start((CHUNK_SIZE as u64) * (chunk_number as u64)))
        {
            Err(why) => panic!("Seek error on {}: {}", self.file_path, why),
            Ok(n) => n,
        };
        let _ = match self.file.read_exact(v.as_mut_slice())
        {
            Err(why) => panic!("Read error on {}: {}", self.file_path, why),
            _ => 0,
        };
        v
    }

    pub fn write_chunk(&mut self, chunk_number: usize, data: &[u8])
    {
        if self.read_only
        {
            panic!("Trying to write {} which is read-only!", self.file_path);
        }
        let chunk_size = self.get_chunk_size(chunk_number);
        if chunk_size != data.len()
        {
            panic!("Error while writing chunk for {}: chunk size mismatch", self.file_path);
        }
        let _ = match self.file.seek(SeekFrom::Start((CHUNK_SIZE as u64) * (chunk_number as u64)))
        {
            Err(why) => panic!("Seek error on {}: {}", self.file_path, why),
            Ok(n) => n,
        };
        let _ = match self.file.write_all(data)
        {
            Err(why) => panic!("Write error on {}: {}", self.file_path, why),
            _ => 0,
        };
    }
}

impl Drop for DiskFile
{
    fn drop(&mut self)
    {
        if !self.read_only
        {
            match self.file.sync_all()
            {
                Err(why) => panic!("Cannot sync {} to disk: {}", self.file_path, why),
                Ok(_) => println!("{} sync'd to disk!", self.file_path),
            }
        };
    }
}
