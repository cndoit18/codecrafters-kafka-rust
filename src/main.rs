use std::io::{Read, Write};
use std::net::TcpListener;
use std::thread;

fn main() {
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                thread::spawn(move || {
                    println!("accepted new connection");
                    let mut buf = [0_u8; 4];
                    while stream.read_exact(&mut buf).is_ok() {
                        // message_size
                        dbg!(&buf);

                        let len = u32::from_be_bytes(buf);
                        // 0..2 request_api_key
                        // 2..4 request_api_version
                        // 4..8 correlation_id
                        let mut msg = vec![0_u8; len as usize];
                        stream.read_exact(msg.as_mut_slice()).unwrap();
                        dbg!(&msg);

                        let mut message = Vec::<u8>::new();
                        let api_key = i16::from_be_bytes([msg[0], msg[1]]);
                        let api_version = i16::from_be_bytes([msg[2], msg[3]]);
                        dbg!(&api_key, &api_version);

                        if !(1..=4).contains(&api_version) {
                            message.extend(&msg[4..8]);
                            message.extend(&[0, 0x23]);
                            let mut response = (message.len() as u32).to_be_bytes().to_vec();
                            response.extend(&message);
                            stream.write_all(response.as_slice()).unwrap();
                            return;
                        }

                        match api_key {
                            // APIVersions
                            18 => {
                                // correlation id
                                message.extend(&msg[4..8]);
                                // error code
                                message.extend(&[0, 0]);
                                // num api key records + 1
                                message.extend(&[3]);
                                // api key
                                message.extend(&[0, 18]);
                                // nim version
                                message.extend(&[0, 0]);
                                // max version
                                message.extend(&[0, 4]);
                                // TAG_BUFFER length
                                message.extend(&[0]);

                                // api key
                                message.extend(&[0, 75]);
                                // nim version
                                message.extend(&[0, 0]);
                                // max version
                                message.extend(&[0, 0]);
                                // TAG_BUFFER length
                                message.extend(&[0]);

                                // throttle time ms
                                message.extend(&[0, 0, 0, 0]);
                                // TAG_BUFFER length
                                message.extend(&[0]);
                            }
                            _ => {
                                unimplemented!();
                            }
                        }
                        let mut response = (message.len() as u32).to_be_bytes().to_vec();
                        response.extend(&message);
                        stream.write_all(response.as_slice()).unwrap();
                    }
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
