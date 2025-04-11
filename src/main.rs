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
                    while let Ok(req) = Request::parse(&stream) {
                        dbg!(&req);
                        let mut message = Vec::<u8>::new();
                        if !(1..=4).contains(&req.header.api_version) {
                            message.extend(req.header.correlation_id.to_be_bytes());
                            message.extend(&[0, 0x23]);
                            let mut response = (message.len() as u32).to_be_bytes().to_vec();
                            response.extend(&message);
                            stream.write_all(response.as_slice()).unwrap();
                            return;
                        }

                        match req.header.api_key {
                            // APIVersions
                            18 => {
                                // correlation id
                                message.extend(req.header.correlation_id.to_be_bytes());
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

#[derive(Default, Debug)]
struct Header {
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    client_id: String,
}

#[derive(Debug)]
struct Request {
    header: Header,
}

impl Request {
    fn parse<R: Read>(mut stream: R) -> Result<Request, String> {
        let mut buf = [0_u8; 4];
        stream.read_exact(&mut buf).map_err(|err| err.to_string())?;
        let mut msg = vec![0_u8; u32::from_be_bytes(buf) as usize];
        stream.read_exact(&mut msg).map_err(|err| err.to_string())?;
        let mut req = Request {
            header: Header::default(),
        };
        req.header.api_key = i16::from_be_bytes([msg[0], msg[1]]);
        req.header.api_version = i16::from_be_bytes([msg[2], msg[3]]);
        req.header.correlation_id = i32::from_be_bytes([msg[4], msg[5], msg[6], msg[7]]);
        req.header.client_id =
            String::from_utf8(msg[10..10 + i16::from_be_bytes([msg[8], msg[9]]) as usize].to_vec())
                .map_err(|err| err.to_string())?;

        Ok(req)
    }
}
enum Message {}
