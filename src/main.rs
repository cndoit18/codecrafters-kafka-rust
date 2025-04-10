use std::io::{Read, Write};
use std::net::TcpListener;

fn main() {
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");
                let mut buf = [0_u8; 4];
                // message_size
                stream.read_exact(buf.as_mut_slice()).unwrap();
                dbg!(&buf);

                let len = u32::from_be_bytes(buf);

                // 0..2 request_api_key
                // 2..4 request_api_version
                // 4..8 correlation_id
                let mut msg = vec![0_u8; len as usize];
                stream.read_exact(msg.as_mut_slice()).unwrap();
                dbg!(&msg);

                let mut response = Vec::<u8>::new();
                let api_version = i16::from_be_bytes([msg[2], msg[3]]);
                dbg!(&api_version);
                match api_version {
                    0..=4 => {
                        response.extend(&[0, 0, 0, 0]);
                        response.extend(&msg[4..8]);
                    }
                    _ => {
                        response.extend(&[0, 0, 0, 0]);
                        response.extend(&msg[4..8]);
                        response.extend(&[0, 0x23]);
                    }
                };
                stream.write_all(response.as_slice()).unwrap();
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
