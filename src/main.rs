use bytes::{Buf, BufMut};
use std::fs::File;
use std::io::{Cursor, Read, Write};
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
                        if !(0..=4).contains(&req.header.api_version) {
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
                                stream
                                    .write_all(
                                        Response {
                                            correlation_id: req.header.correlation_id,
                                            tag_buffer: 0,
                                            message: ResponseMessage::APIVersions {
                                                error_code: 0,
                                                api_versions: vec![
                                                    ResponseMessageAPIVersion {
                                                        api_key: 18,
                                                        min_supported_api_version: 0,
                                                        max_supported_api_version: 4,
                                                        tag_buffer: 0,
                                                    },
                                                    ResponseMessageAPIVersion {
                                                        api_key: 75,
                                                        min_supported_api_version: 0,
                                                        max_supported_api_version: 0,
                                                        tag_buffer: 0,
                                                    },
                                                ],
                                                throttle_time: 0,
                                                tag_buffer: 0,
                                            },
                                        }
                                        .to_vec()
                                        .as_slice(),
                                    )
                                    .unwrap();
                            }
                            75 => {
                                let mut resp_topics = vec![];
                                if let RequestMessage::DescribeTopicPartitions {
                                    topics,
                                    response_partition_limit: _,
                                    cursor: _,
                                    tag_buffer: _,
                                } = req.message
                                {
                                    for RequestMessageTopic { name, tag_buffer } in topics {
                                        resp_topics.push(ResponseMessageTopic {
                                            error_code: 3,
                                            topic_name: name,
                                            topic_id: 0,
                                            is_internal: 0,
                                            authorize_operations: 0x0df8,
                                            tag_buffer,
                                        });
                                    }
                                }

                                let resp = Response {
                                    correlation_id: req.header.correlation_id,
                                    tag_buffer: req.header.tag_buffer,
                                    message: ResponseMessage::DescribeTopicPartitions {
                                        throttle_time: 0,
                                        topics: resp_topics,
                                        next_cursor: 0xff,
                                        tag_buffer: 0,
                                    },
                                }
                                .to_vec();
                                dbg!(&resp);
                                stream.write_all(resp.as_slice()).unwrap();
                            }
                            _ => {
                                unimplemented!();
                            }
                        }
                    }
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

#[derive(Debug)]
struct Response {
    correlation_id: i32,
    tag_buffer: u8,
    message: ResponseMessage,
}

impl Response {
    fn to_vec(&self) -> Vec<u8> {
        dbg!(&self);
        let mut msg = vec![];
        msg.put_i32(self.correlation_id);
        msg.put_u8(self.tag_buffer);
        self.message.append(&mut msg);
        let mut response = (msg.len() as u32).to_be_bytes().to_vec();
        response.extend(&msg);
        response
    }
}

impl ResponseMessage {
    fn append(&self, msg: &mut Vec<u8>) {
        match self {
            ResponseMessage::DescribeTopicPartitions {
                throttle_time,
                topics,
                next_cursor,
                tag_buffer,
            } => {
                msg.put_u32(*throttle_time);
                msg.put_u8(topics.len() as u8 + 1);
                for ResponseMessageTopic {
                    error_code,
                    topic_name,
                    topic_id,
                    is_internal,
                    authorize_operations,
                    tag_buffer,
                } in topics
                {
                    msg.put_u16(*error_code);
                    msg.put_u8(topic_name.len() as u8 + 1);
                    msg.put_slice(topic_name.as_bytes());
                    msg.put_u128(*topic_id);
                    msg.put_u8(*is_internal);
                    msg.put_u8(1);
                    msg.put_u32(*authorize_operations);
                    msg.put_u8(*tag_buffer);
                }
                msg.put_u8(*next_cursor);
                msg.put_u8(*tag_buffer);
            }
            ResponseMessage::APIVersions {
                error_code,
                api_versions,
                throttle_time,
                tag_buffer,
            } => {
                msg.put_u8(*error_code);
                msg.put_u8(api_versions.len() as u8 + 1);
                for ResponseMessageAPIVersion {
                    api_key,
                    min_supported_api_version,
                    max_supported_api_version,
                    tag_buffer,
                } in api_versions
                {
                    msg.put_i16(*api_key);
                    msg.put_u16(*min_supported_api_version);
                    msg.put_u16(*max_supported_api_version);
                    msg.put_u8(*tag_buffer);
                }
                msg.put_u32(*throttle_time);
                msg.put_u8(*tag_buffer);
            }
        }
    }
}

#[derive(Debug)]
enum ResponseMessage {
    DescribeTopicPartitions {
        throttle_time: u32,
        topics: Vec<ResponseMessageTopic>,
        next_cursor: u8,
        tag_buffer: u8,
    },
    APIVersions {
        error_code: u8,
        api_versions: Vec<ResponseMessageAPIVersion>,
        throttle_time: u32,
        tag_buffer: u8,
    },
}

#[derive(Debug)]
struct ResponseMessageAPIVersion {
    api_key: i16,
    min_supported_api_version: u16,
    max_supported_api_version: u16,
    tag_buffer: u8,
}

#[derive(Debug)]
struct ResponseMessageTopic {
    error_code: u16,
    topic_name: String,
    topic_id: u128,
    is_internal: u8,
    // ignore partitions
    authorize_operations: u32,
    tag_buffer: u8,
}

#[derive(Default, Debug)]
struct RequestHeader {
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    client_id: String,
    tag_buffer: u8,
}

#[derive(Debug)]
struct Request {
    header: RequestHeader,
    message: RequestMessage,
}

impl Request {
    fn parse<R: Read>(mut stream: R) -> Result<Request, String> {
        let mut buf = [0_u8; 4];
        stream.read_exact(&mut buf).map_err(|err| err.to_string())?;
        let mut msg = vec![0_u8; u32::from_be_bytes(buf) as usize];
        stream.read_exact(&mut msg).map_err(|err| err.to_string())?;
        let mut msg = msg.as_slice();
        let mut header = RequestHeader::default();

        header.api_key = msg.get_i16();
        header.api_version = msg.get_i16();
        header.correlation_id = msg.get_i32();
        let mut client_id = vec![0_u8; msg.get_i16() as usize];
        msg.read_exact(&mut client_id)
            .map_err(|err| err.to_string())?;
        header.client_id = String::from_utf8(client_id).map_err(|err| err.to_string())?;
        header.tag_buffer = msg.get_u8();

        let message = match header.api_key {
            75 => {
                let mut reader = vec![];
                let _ = File::open(
                    "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log",
                )
                .map_err(|err| err.to_string())?
                .read_to_end(&mut reader)
                .map_err(|err| err.to_string())?;
                let mut metadata = vec![];
                let mut reader = Cursor::new(reader.to_vec());
                while reader.remaining() > 0 {
                    dbg!(reader.remaining());
                    metadata.push(ClusterMetadata::parse(&mut reader)?);
                }
                dbg!(&metadata);
                let mut topics =
                    Vec::<RequestMessageTopic>::with_capacity(msg.get_u8() as usize - 1);
                for _i in 0..topics.capacity() {
                    let mut name = vec![0_u8; msg.get_u8() as usize - 1];
                    msg.read_exact(&mut name).map_err(|err| err.to_string())?;
                    topics.push(RequestMessageTopic {
                        name: String::from_utf8(name).map_err(|err| err.to_string())?,
                        tag_buffer: msg.get_u8(),
                    });
                }
                RequestMessage::DescribeTopicPartitions {
                    topics,
                    response_partition_limit: msg.get_u32(),
                    cursor: msg.get_u8(),
                    tag_buffer: msg.get_u8(),
                }
            }
            18 => {
                let mut s = vec![0_u8; msg.get_u8() as usize - 1];
                msg.read_exact(&mut s).map_err(|err| err.to_string())?;
                let client_id = String::from_utf8(s).map_err(|err| err.to_string())?;

                let mut s = vec![0_u8; msg.get_u8() as usize - 1];
                msg.read_exact(&mut s).map_err(|err| err.to_string())?;
                let client_version = String::from_utf8(s).map_err(|err| err.to_string())?;
                RequestMessage::APIVersions {
                    client_id,
                    client_version,
                    tag_buffer: msg.get_u8(),
                }
            }
            _ => unimplemented!(),
        };

        Ok(Request { header, message })
    }
}

#[derive(Debug)]
#[allow(unused)]
enum RequestMessage {
    DescribeTopicPartitions {
        topics: Vec<RequestMessageTopic>,
        response_partition_limit: u32,
        cursor: u8,
        tag_buffer: u8,
    },
    APIVersions {
        client_id: String,
        client_version: String,
        tag_buffer: u8,
    },
}

#[derive(Debug)]
struct RequestMessageTopic {
    name: String,
    tag_buffer: u8,
}

#[derive(Debug)]
struct ClusterMetadata {
    offset: u64,
    partition_leader_epoch: u32,
    magic: u8,
    crc: u32,
    attributes: u16,
    last_offset_delta: u32,
    base_timestamp: u64,
    max_timestamp: u64,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    records: Vec<Record>,
}

#[derive(Debug)]
struct Record {
    attributes: u8,
    timestamp_dalta: i8,
    offset_dalta: i8,
}

impl ClusterMetadata {
    fn parse<R: Read>(mut stream: R) -> Result<Self, String> {
        let mut offset = [0_u8; 8];
        stream
            .read_exact(&mut offset)
            .map_err(|err| err.to_string())?;
        let mut size = [0_u8; 4];
        stream
            .read_exact(&mut size)
            .map_err(|err| err.to_string())?;
        let mut raw = vec![0_u8; u32::from_be_bytes(size) as usize];
        stream.read_exact(&mut raw).map_err(|err| err.to_string())?;
        let mut msg = raw.as_slice();
        let offset = u64::from_be_bytes(offset);
        let partition_leader_epoch = msg.get_u32();
        let magic = msg.get_u8();
        let crc = msg.get_u32();
        let attributes = msg.get_u16();
        let last_offset_delta = msg.get_u32();
        let base_timestamp = msg.get_u64();
        let max_timestamp = msg.get_u64();
        let producer_id = msg.get_i64();
        let producer_epoch = msg.get_i16();
        let base_sequence = msg.get_i32();
        let records_size = msg.get_u32();
        let mut records = vec![];
        for _i in 0..records_size {
            let size = msg.get_u8();
            // https://kafka.apache.org/protocol.html
            // zig-zag encoding
            let size = (size >> 1) ^ (!(size & 1) + 1);
            let mut buf = vec![0_u8; size as usize];
            msg.read_exact(&mut buf).map_err(|err| err.to_string())?;
            let mut buf = buf.as_slice();
            let attributes = buf.get_u8();
            let timestamp_dalta = buf.get_i8();
            let offset_dalta = buf.get_i8();
            let _key_length = buf.get_i8();

            let size = buf.get_u8();
            // zig-zag encoding
            let size = (size >> 1) ^ (!(size & 1) + 1);
            dbg!(&size);
            let mut value = vec![0_u8; size as usize];
            buf.read_exact(&mut value).map_err(|err| err.to_string())?;
            //let mut value = value.as_slice();
            //
            //let mut rv = RecordValue {
            //    frame_version: value.get_u8(),
            //    frame_type: value.get_u8(),
            //    version: value.get_u8(),
            //    name: String::new(),
            //    feature_level: 0,
            //};
            //
            //let name_length = value.get_u8() - 1;
            //let mut name = vec![0_u8; name_length as usize];
            //value.read_exact(&mut name).map_err(|err| err.to_string())?;
            //rv.name = String::from_utf8(name).map_err(|err| err.to_string())?;
            ////rv.feature_level = value.get_u16();
            //let _ = value.get_u8();
            records.push(Record {
                attributes,
                timestamp_dalta,
                offset_dalta,
            });
        }
        Ok(ClusterMetadata {
            offset,
            partition_leader_epoch,
            magic,
            crc,
            attributes,
            last_offset_delta,
            base_timestamp,
            max_timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
            records,
        })
    }
}

#[derive(Debug)]
struct RecordValue {
    frame_version: u8,
    frame_type: u8,
    version: u8,
    name: String,
    feature_level: u16,
}
