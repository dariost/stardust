extern crate toml;

mod commons;
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
    let mut conf_file = match File::open("stardustd.toml")
    {
        Err(why) => panic!("Cannot open conf file: {}", why),
        Ok(n) => n,
    };
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
    let mut broadcast_address: Vec<String> = vec![String::from("255.255.255.255"); 1];
    if table.contains_key("broadcast_address")
    {
        broadcast_address.clear();
        let mut bat: Vec<toml::Value> = Vec::new();
        bat.extend_from_slice(table["broadcast_address"].as_slice().unwrap());
        for i in &bat
        {
            broadcast_address.push(String::from(i.as_str().unwrap()));
        }
    }
    let file_folder = if table.contains_key("file_folder")
    {
        String::from(table["file_folder"].clone().as_str().unwrap())
    }
    else
    {
        String::from("./")
    };
    let megabit_per_second: f64 = if table.contains_key("megabit_per_second")
    {
        table["megabit_per_second"].clone().as_float().unwrap()
    }
    else
    {
        90.0
    };
    let mut to_send: Vec<SocketAddr> = Vec::new();
    for i in &broadcast_address
    {
        let mut ip: Vec<u8> = Vec::new();
        for s in i.split('.')
        {
            ip.push(match s.parse::<u8>()
                {
                    Err(why) => panic!("Invalid IP address: {}", why),
                    Ok(n) => n,
                });
        }
        to_send.push(SocketAddr::new(IpAddr::V4(Ipv4Addr::new(ip[0], ip[1], ip[2], ip[3])), 31415));
    }
    let mut files_path: Vec<PathBuf> = Vec::new();
    let paths = fs::read_dir(file_folder).unwrap();
    for path in paths
    {
        files_path.push(path.unwrap().path());
    }
    let mut net = Network::new(&files_path, false, to_send, megabit_per_second);
    net.run();
}
