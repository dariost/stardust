extern crate toml;

mod disk_file;
mod pool;
mod network;

use network::Network;
use std::fs;
use std::fs::File;
use std::io::Read;
use std::net::{IpAddr, Ipv4Addr};
use std::net::SocketAddr;
use std::path::PathBuf;
use toml::Parser;

fn main()
{
    let mut conf_file = File::open("stardustd.toml")
        .unwrap_or(match File::open("/etc/stardustd.toml")
        {
            Err(_) => panic!("Cannot open conf file"),
            Ok(n) => n,
        });
    let mut conf_content = String::new();
    let _ = match conf_file.read_to_string(&mut conf_content)
    {
        Err(why) => panic!("Cannot read conf file: {}", why),
        Ok(n) => n,
    };
    let mut parser = Parser::new(conf_content.as_str());
    let table = match parser.parse()
    {
        None => panic!("Cannot parse conf file"),
        Some(n) => n,
    };
    let mut broadcast_address = String::from("255.255.255.255");
    if table.contains_key("broadcast_address")
    {
        broadcast_address = String::from(table.get("broadcast_address").unwrap().clone().as_str().unwrap());
    }
    let mut file_folder = String::from("./");
    if table.contains_key("file_folder")
    {
        file_folder = String::from(table.get("file_folder").unwrap().clone().as_str().unwrap());
    }
    let mut megabit_per_second: f64 = 90.0;
    if table.contains_key("megabit_per_second")
    {
        megabit_per_second = table.get("megabit_per_second").unwrap().clone().as_float().unwrap();
    }
    let mut ip: Vec<u8> = Vec::new();
    for s in broadcast_address.split(".")
    {
        ip.push(match s.parse::<u8>()
            {
                Err(why) => panic!("Invalid IP address: {}", why),
                Ok(n) => n,
            });
    }
    let to_send = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(ip[0], ip[1], ip[2], ip[3])), 31416);
    let mut files_path: Vec<PathBuf> = Vec::new();
    let paths = fs::read_dir(file_folder).unwrap();
    for path in paths
    {
        files_path.push(path.unwrap().path());
    }
    let mut net = Network::new(&files_path, false, &to_send, megabit_per_second);
    net.run();
}
