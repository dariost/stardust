extern crate byteorder;

use commons::HASH_SIZE;
use pool::{ACTION_REQUEST_DATA, ACTION_REQUEST_DESCRIPTION, ACTION_SEND_DATA, ACTION_SEND_DESCRIPTION, MAGIC_NUMBER,
           PROTOCOL_VERSION};
use pool::Pool;
use self::byteorder::{BigEndian, ByteOrder};
use std::cmp::max;
use std::net::SocketAddr;
use std::net::UdpSocket;
use std::path::PathBuf;
use std::thread::sleep;
use std::time::{Duration, Instant};

pub struct Network
{
    socket: UdpSocket,
    client: bool,
    send_address: Vec<SocketAddr>,
    pool: Pool,
    megabit_per_second: f64,
}

impl Network
{
    fn sanity_check(&self, header: &[u8; 6]) -> bool
    {
        let mut buffer: [u8; 6] = [0; 6];
        BigEndian::write_u32(&mut buffer[0..4], MAGIC_NUMBER);
        BigEndian::write_u16(&mut buffer[4..6], PROTOCOL_VERSION);
        return header.clone() == buffer;
    }

    pub fn run(&mut self)
    {
        println!("Program started!");
        if self.client
        {
            let mut finished = false;
            'outer: while !finished
            {
                let mut started = Instant::now();
                while started.elapsed() < Duration::from_millis(2500)
                {
                    let mut buffer: [u8; 1 << 16] = [0; 1 << 16];
                    let buffer_size = match self.socket.recv_from(&mut buffer)
                    {
                        Err(_) => 0,
                        Ok(n) => n.0,
                    };
                    if buffer_size < 7
                    {
                        continue;
                    }
                    let mut head: [u8; 6] = [0; 6];
                    head.clone_from_slice(&buffer[0..6]);
                    if !self.sanity_check(&head)
                    {
                        continue;
                    }
                    if buffer[6] != ACTION_SEND_DESCRIPTION
                    {
                        continue;
                    }
                    let mut v = Vec::new();
                    v.extend_from_slice(&buffer[7..buffer_size]);
                    finished = self.pool.process_binary_description(&v);
                    if finished
                    {
                        break 'outer;
                    }
                    started = Instant::now();
                    println!("Asking for files description: {} left", self.pool.desc_left());
                }
                let mut send_request: Vec<u8> = Vec::new();
                let mut buffer: [u8; 128] = [0; 128];
                // Magic number
                BigEndian::write_u32(&mut buffer[0..4], MAGIC_NUMBER);
                send_request.extend_from_slice(&buffer[0..4]);
                // Protocol version
                BigEndian::write_u16(&mut buffer[0..2], PROTOCOL_VERSION);
                send_request.extend_from_slice(&buffer[0..2]);
                // Action
                send_request.push(ACTION_REQUEST_DESCRIPTION);
                let _ = match self.socket.send_to(send_request.as_slice(), &self.send_address[0])
                {
                    Err(why) => panic!("Cannot send request for description: {}", why),
                    Ok(n) => n,
                };
            }
        }
        let mut last_print = Instant::now();
        while self.client
        {
            if self.pool.is_complete()
            {
                return;
            }
            let mut buffer: [u8; 1 << 16] = [0; 1 << 16];
            let mut buffer_size;
            let mut count = 0;
            loop
            {
                if last_print.elapsed().as_secs() > 1
                {
                    last_print = Instant::now();
                    println!("~{} kibibytes left", self.pool.chunks_left() * 60000 / 1024);
                }
                if count > 40000 / HASH_SIZE
                {
                    break;
                }
                // TODO: if someone flood us with garbage this cycle never ends
                buffer_size = match self.socket.recv_from(&mut buffer)
                {
                    Err(_) => 0,
                    Ok(n) => n.0,
                };
                if buffer_size == 0
                {
                    break;
                }
                if buffer_size < 7
                {
                    continue;
                }
                let mut head: [u8; 6] = [0; 6];
                head.clone_from_slice(&buffer[0..6]);
                if !self.sanity_check(&head)
                {
                    continue;
                }
                if buffer[6] == ACTION_SEND_DATA
                {
                    count += 1;
                    let mut v: Vec<u8> = Vec::new();
                    v.extend_from_slice(&buffer[7..buffer_size]);
                    self.pool.process_binary_data(&v);
                    if self.pool.is_complete()
                    {
                        return;
                    }
                }
            }
            let send_req = self.pool.get_binary_chunk_request();
            let _ = self.socket.send_to(send_req.as_slice(), &self.send_address[0]);
        }
        let mut send_started = Instant::now();
        while !self.client
        {
            if last_print.elapsed().as_secs() > 1
            {
                last_print = Instant::now();
                println!("Queue length: {}", self.pool.get_queue_size());
            }
            let packet = self.pool.get_binary_send_packet();
            if packet.is_none()
            {
                let _ = self.socket.set_nonblocking(false);
            }
            else
            {
                let unwrapped_packet = packet.unwrap();
                for addr in &self.send_address
                {
                    let _ = self.socket.send_to(unwrapped_packet.as_slice(), addr);
                }
                let converted_size: f64 = (unwrapped_packet.len() as f64) * 8.0 / 1000000.0;
                let milli_stop: u64 = (converted_size * 1000.0 / self.megabit_per_second) as u64;
                let _ = self.socket.set_nonblocking(true);
                sleep(Duration::from_millis(milli_stop));
            }
            let mut buffer: [u8; 1 << 16] = [0; 1 << 16];
            let buffer_size = match self.socket.recv_from(&mut buffer)
            {
                Err(_) => 0,
                Ok(n) => n.0,
            };
            if buffer_size < 7
            {
                continue;
            }
            let mut head: [u8; 6] = [0; 6];
            head.clone_from_slice(&buffer[0..6]);
            if !self.sanity_check(&head)
            {
                continue;
            }
            if buffer[6] == ACTION_REQUEST_DATA
            {
                let mut v: Vec<u8> = Vec::new();
                v.extend_from_slice(&buffer[7..buffer_size]);
                self.pool.process_binary_chunk_request(&v);
            }
            if buffer[6] == ACTION_REQUEST_DESCRIPTION || send_started.elapsed().as_secs() >= 10
            {
                send_started = Instant::now();
                let b = self.pool.generate_binary_description();
                for bv in &b
                {
                    for addr in &self.send_address
                    {
                        let _ = self.socket.send_to(bv.as_slice(), addr);
                    }
                    sleep(max(Duration::from_millis((480.0 / self.megabit_per_second) as u64), Duration::from_millis(1)));
                }
            }
        }
    }

    pub fn new(files: &Vec<PathBuf>, _client: bool, _send_address: Vec<SocketAddr>, _megabit_per_second: f64) -> Network
    {
        println!("Welcome to Stardust");
        if _client && _send_address.len() != 1
        {
            panic!("Cannot have multiple server addresses");
        }
        let sock = match if _client
        {
            UdpSocket::bind("0.0.0.0:31415")
        }
        else
        {
            UdpSocket::bind("0.0.0.0:31416")
        }
        {
            Err(why) => panic!("Cannot create socket: {}", why),
            Ok(s) => s,
        };
        if _client
        {
            println!("Waiting for bootstrap packet");
            let mut bootstrap_packet: [u8; 1 << 16] = [0; 1 << 16];
            let mut bootstrap_packet_size: usize;
            let mut send_request: Vec<u8> = Vec::new();
            let mut buffer: [u8; 128] = [0; 128];
            // Magic number
            BigEndian::write_u32(&mut buffer[0..4], MAGIC_NUMBER);
            send_request.extend_from_slice(&buffer[0..4]);
            // Protocol version
            BigEndian::write_u16(&mut buffer[0..2], PROTOCOL_VERSION);
            send_request.extend_from_slice(&buffer[0..2]);
            // Action
            send_request.push(ACTION_REQUEST_DESCRIPTION);
            let _ = match sock.set_read_timeout(Some(Duration::from_millis(1500)))
            {
                Err(why) => panic!("Cannot set read timeout: {}", why),
                Ok(_) => 0,
            };
            loop
            {
                println!("Requesting bootstrap...");
                let _ = match sock.send_to(send_request.as_slice(), &_send_address[0])
                {
                    Err(why) => panic!("Cannot send request for description: {}", why),
                    Ok(n) => n,
                };
                bootstrap_packet_size = match sock.recv_from(&mut bootstrap_packet)
                {
                    Err(_) => 0,
                    Ok(v) => v.0,
                };
                if bootstrap_packet_size < 7
                {
                    continue;
                }
                if BigEndian::read_u32(&bootstrap_packet[0..4]) != MAGIC_NUMBER
                {
                    continue;
                }
                if BigEndian::read_u16(&bootstrap_packet[4..6]) != PROTOCOL_VERSION
                {
                    println!("Protocol version mismatch");
                    continue;
                }
                if bootstrap_packet[6] != ACTION_SEND_DESCRIPTION
                {
                    continue;
                }
                println!("Bootstrapping started...");
                break;
            }
            let _ = match sock.set_read_timeout(Some(Duration::from_millis(20)))
            {
                Err(why) => panic!("Cannot set read timeout: {}", why),
                Ok(_) => 0,
            };
            let mut vec_boostrap = Vec::new();
            vec_boostrap.extend_from_slice(&bootstrap_packet[7..bootstrap_packet_size]);
            return Network {
                socket: sock,
                client: true,
                send_address: _send_address,
                pool: Pool::new(files, Some(vec_boostrap)),
                megabit_per_second: _megabit_per_second,
            };
        }
        else
        {
            let _ = match sock.set_broadcast(true)
            {
                Err(why) => panic!("Cannot enable broadcast transmission: {}", why),
                Ok(_) => 0,
            };
            return Network {
                socket: sock,
                client: false,
                send_address: _send_address,
                pool: Pool::new(files, None),
                megabit_per_second: _megabit_per_second,
            };
        }
    }
}
