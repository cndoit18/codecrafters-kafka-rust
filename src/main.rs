use std::io::Write;
use std::net::TcpListener;

fn main() {
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");
                stream.write_all(&[0, 0, 0, 0, 0, 0, 0, 7]).unwrap();
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
