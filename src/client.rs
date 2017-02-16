mod commons;
mod disk_file;
mod pool;
mod network;

use network::Network;
use std::env;
use std::net::{IpAddr, Ipv4Addr};
use std::net::SocketAddr;
use std::path::PathBuf;

fn main()
{
    let args: Vec<String> = env::args().collect();
    if args.len() < 3
    {
        panic!("Usage: {} <server-ip> [file [file [...]]]", args[0]);
    }
    let ip_str: Vec<&str> = args[1].split('.').collect();
    if ip_str.len() != 4
    {
        panic!("Invalid IP address");
    }
    let mut ip: Vec<u8> = Vec::new();
    for s in &ip_str
    {
        ip.push(match s.parse::<u8>()
            {
                Err(why) => panic!("Invalid IP address: {}", why),
                Ok(n) => n,
            });
    }
    let to_send = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(ip[0], ip[1], ip[2], ip[3])), 31416);
    let mut files_path: Vec<PathBuf> = Vec::new();
    for i in args.iter().skip(2)
    {
        files_path.push(PathBuf::from(&i));
    }
    let mut net = Network::new(&files_path, true, vec![to_send; 1], 0.0);
    net.run();
}
