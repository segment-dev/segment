use crate::frame;
use crate::keyspace::{Evictor, MAX_MEMORY_SAMPLE_SIZE};
use crate::server::ConnectionHandler;
use anyhow::{anyhow, Result};
use atoi::atoi;
use bytes::Bytes;
use std::iter;
use std::{str, vec};

pub struct Parser {
    iterator: iter::Peekable<vec::IntoIter<frame::Frame>>,
}

#[derive(Debug, PartialEq)]
pub enum Command {
    Get(Get),
    Set(Set),
    Del(Del),
    Create(Create),
}

#[derive(Debug, PartialEq)]
pub struct Get {
    key: String,
    keyspace: String,
}

#[derive(Debug, PartialEq)]
pub struct Set {
    key: String,
    value: Bytes,
    keyspace: String,
}

#[derive(Debug, PartialEq)]
pub struct Del {
    key: String,
    keyspace: String,
}

#[derive(Debug, PartialEq)]
pub struct Create {
    keyspace: String,
    evictor: Evictor,
    max_memory_sample_size: Option<usize>,
}

impl Parser {
    pub fn new(frame: frame::Frame) -> Result<Self> {
        match frame {
            frame::Frame::Array(values) => Ok(Parser {
                iterator: values.into_iter().peekable(),
            }),
            _ => Err(anyhow!("ERRPARSE Failed to parse frame as array")),
        }
    }

    fn next(&mut self) -> Option<frame::Frame> {
        self.iterator.next()
    }

    pub fn next_string(&mut self) -> Result<Option<String>> {
        match self.next() {
            Some(frame) => match frame {
                frame::Frame::String(data) => Ok(Some(data)),
                frame::Frame::Blob(data) => str::from_utf8(&data[..])
                    .map(|v| Some(v.to_string()))
                    .map_err(|e| anyhow!(e)),
                _ => Err(anyhow!("ERRPARSE Failed to parse frame as string")),
            },
            None => Ok(None),
        }
    }

    pub fn next_blob(&mut self) -> Result<Option<Bytes>> {
        match self.next() {
            Some(frame) => match frame {
                frame::Frame::String(data) => Ok(Some(Bytes::from(data))),
                frame::Frame::Blob(data) => Ok(Some(data)),
                _ => Err(anyhow!("ERRPARSE Failed to parse frame as blob")),
            },
            None => Ok(None),
        }
    }

    pub fn _next_integer(&mut self) -> Result<Option<i64>> {
        match self.next() {
            Some(frame) => match frame {
                frame::Frame::String(data) => atoi::<i64>(data.as_bytes())
                    .ok_or_else(|| anyhow!("ERRPARSE Failed to parse frame as integer"))
                    .map(Some),
                frame::Frame::Blob(data) => atoi::<i64>(&data[..])
                    .ok_or_else(|| anyhow!("ERRPARSE Failed to parse frame as integer"))
                    .map(Some),
                frame::Frame::Integer(val) => Ok(Some(val)),
                _ => Err(anyhow!("ERRPARSE Failed to parse frame as integer")),
            },
            None => Ok(None),
        }
    }

    pub fn consumed(&mut self) -> bool {
        self.iterator.peek().is_none()
    }
}

impl Get {
    pub fn parse(parser: &mut Parser) -> Result<Self> {
        if let Some(keyspace) = parser.next_string()? {
            if let Some(key) = parser.next_string()? {
                if !parser.consumed() {
                    return Err(anyhow!(
                        "ERRPARSE Invalid command, wrong number of arguments for 'GET'"
                    ));
                }
                return Ok(Get { keyspace, key });
            }
            return Err(anyhow!("ERRPARSE Invalid command, missing argument 'KEY'"));
        }
        return Err(anyhow!(
            "ERRPARSE Invalid command, missing argument 'KEYSPACE'"
        ));
    }

    pub async fn exec(&self, connection: &mut ConnectionHandler) -> Result<()> {
        match connection
            .keyspace_manager
            .with_keyspace(&self.keyspace, |keyspace| Ok(keyspace.get(&self.key)))
        {
            Ok(response) => {
                if let Some(value) = response {
                    connection.connection.write_blob(&value).await
                } else {
                    connection.connection.write_null().await
                }
            }
            Err(e) => {
                connection
                    .connection
                    .write_error(&format!("ERREXEC {}", e))
                    .await
            }
        }
    }
}

impl Del {
    pub fn parse(parser: &mut Parser) -> Result<Self> {
        if let Some(keyspace) = parser.next_string()? {
            if let Some(key) = parser.next_string()? {
                if !parser.consumed() {
                    return Err(anyhow!(
                        "ERRPARSE Invalid command, wrong number of arguments for 'DEL'"
                    ));
                }
                return Ok(Del { keyspace, key });
            }
            return Err(anyhow!("ERRPARSE Invalid command, missing argument 'KEY'"));
        }
        return Err(anyhow!(
            "ERRPARSE Invalid command, missing argument 'KEYSPACE'"
        ));
    }

    pub async fn exec(&self, connection: &mut ConnectionHandler) -> Result<()> {
        match connection
            .keyspace_manager
            .with_keyspace(&self.keyspace, |keyspace| Ok(keyspace.del(&self.key)))
        {
            Ok(response) => connection.connection.write_integer(response as i64).await,
            Err(e) => {
                connection
                    .connection
                    .write_error(&format!("ERREXEC {}", e))
                    .await
            }
        }
    }
}

impl Set {
    pub fn parse(parser: &mut Parser) -> Result<Self> {
        if let Some(keyspace) = parser.next_string()? {
            if let Some(key) = parser.next_string()? {
                if let Some(value) = parser.next_blob()? {
                    if !parser.consumed() {
                        return Err(anyhow!(
                            "ERRPARSE Invalid command, wrong number of arguments for 'SET'"
                        ));
                    }
                    return Ok(Set {
                        keyspace,
                        key,
                        value,
                    });
                }
                return Err(anyhow!(
                    "ERRPARSE Invalid command, missing argument 'VALUE'"
                ));
            }
            return Err(anyhow!("ERRPARSE Invalid command, missing argument 'KEY'"));
        }
        return Err(anyhow!(
            "ERRPARSE Invalid command, missing argument 'KEYSPACE'"
        ));
    }

    pub async fn exec(self, connection: &mut ConnectionHandler) -> Result<()> {
        match connection
            .keyspace_manager
            .with_keyspace(&self.keyspace, |keyspace| {
                Ok(keyspace.set(self.key, self.value))
            }) {
            Ok(response) => connection.connection.write_integer(response as i64).await,
            Err(e) => {
                connection
                    .connection
                    .write_error(&format!("ERREXEC {}", e))
                    .await
            }
        }
    }
}

impl Create {
    pub fn parse(parser: &mut Parser) -> Result<Self> {
        if let Some(keyspace) = parser.next_string()? {
            let mut cmd = Create {
                keyspace,
                evictor: Evictor::Noop,
                max_memory_sample_size: None,
            };
            let mut tokens = Vec::<String>::with_capacity(6);

            while !parser.consumed() {
                if tokens.len() > 4 {
                    return Err(anyhow!(
                        "ERRPARSE Invalid command, wrong number of arguments for 'CREATE'"
                    ));
                }
                if let Some(token) = parser.next_string()? {
                    tokens.push(token);
                }
            }

            if tokens.is_empty() {
                return Ok(cmd);
            }

            if tokens.len() % 2 != 0 {
                return Err(anyhow!(
                    "ERRPARSE Invalid command, wrong number of arguments for 'CREATE'"
                ));
            }

            let mut i = 0;
            while i < tokens.len() - 1 {
                let arg = &tokens[i].to_uppercase();
                let val = &tokens[i + 1].to_uppercase();

                if arg == "EV" {
                    cmd.evictor = match val.as_str() {
                        "RANDOM" => Evictor::Random,
                        "NOOP" => Evictor::Noop,
                        "LRU" => Evictor::Lru,
                        _ => return Err(anyhow!("ERRPARSE Invalid value '{}' for 'EVICTOR'", val)),
                    };
                } else if arg == "SS" {
                    let sample_size = match val.parse::<usize>() {
                        Ok(v) => v,
                        Err(_) => {
                            return Err(anyhow!(
                                "ERRPARSE Invalid value '{}' for 'SAMPLE SIZE'",
                                val
                            ))
                        }
                    };
                    cmd.max_memory_sample_size = Some(sample_size);
                } else {
                    return Err(anyhow!("ERRPARSE Invalid argument '{}'", arg));
                }
                i += 2;
            }

            if cmd.evictor == Evictor::Noop && cmd.max_memory_sample_size.is_some() {
                return Err(anyhow!(
                    "ERRPARSE Invalid command, 'SAMPLE SIZE' not applicable for 'NOOP' evictor"
                ));
            } else if cmd.evictor != Evictor::Noop && cmd.max_memory_sample_size.is_none() {
                cmd.max_memory_sample_size = Some(MAX_MEMORY_SAMPLE_SIZE);
            }

            return Ok(cmd);
        }
        Err(anyhow!(
            "ERRPARSE Invalid command, missing argument 'KEYSPACE'"
        ))
    }

    pub async fn exec(self, connection: &mut ConnectionHandler) -> Result<()> {
        let mut max_memory_sample_size = 0;
        if let Some(sample_size) = self.max_memory_sample_size {
            max_memory_sample_size = sample_size
        }
        let response =
            connection
                .keyspace_manager
                .create(self.keyspace, self.evictor, max_memory_sample_size);
        connection.connection.write_integer(response as i64).await
    }
}

pub fn new(frame: frame::Frame) -> Result<Command> {
    let mut parser = Parser::new(frame)?;

    if let Some(cmd) = parser.next_string()? {
        let command = cmd.to_uppercase();
        match &command[..] {
            "SET" => return Ok(Command::Set(Set::parse(&mut parser)?)),
            "GET" => return Ok(Command::Get(Get::parse(&mut parser)?)),
            "DEL" => return Ok(Command::Del(Del::parse(&mut parser)?)),
            "CREATE" => return Ok(Command::Create(Create::parse(&mut parser)?)),
            cmd => return Err(anyhow!("ERRPARSE Unknown command '{}'", cmd)),
        }
    }

    return Err(anyhow!("ERRPARSE No command was provided to be executed"));
}

pub async fn exec(cmd: Command, connection: &mut ConnectionHandler) -> Result<()> {
    match cmd {
        Command::Create(cmd) => cmd.exec(connection).await,
        Command::Set(cmd) => cmd.exec(connection).await,
        Command::Del(cmd) => cmd.exec(connection).await,
        Command::Get(cmd) => cmd.exec(connection).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn get_cursor(data: &[u8]) -> Cursor<&[u8]> {
        Cursor::new(data)
    }

    fn get_frame(data: &[u8]) -> frame::Frame {
        let mut cursor = get_cursor(data);
        frame::parse(&mut cursor).unwrap()
    }

    #[test]
    fn new_non_array_frame_error() {
        let frame = get_frame(b"$create\r\n");
        assert!(new(frame).is_err())
    }

    #[test]
    fn new_empty_array_frame_error() {
        let frame = get_frame(b"#0\r\n");
        assert!(new(frame).is_err())
    }

    #[test]
    fn new_unknow_command_error() {
        let frame = get_frame(b"#1\r\n$foo\r\n");
        assert!(new(frame).is_err())
    }

    #[test]
    fn new_create_without_keyspace_error() {
        assert!(new(get_frame(b"#1\r\n$create\r\n")).is_err())
    }

    #[test]
    fn new_create_with_keyspace_no_error() {
        assert_eq!(
            new(get_frame(b"#2\r\n$create\r\n$foo\r\n")).unwrap(),
            Command::Create(Create {
                keyspace: String::from("foo"),
                evictor: Evictor::Noop,
                max_memory_sample_size: None
            })
        )
    }

    #[test]
    fn new_create_noop_evictor_implicit_with_sample_size_error() {
        assert!(new(get_frame(b"#4\r\n$create\r\n$foo\r\n$ss\r\n$100\r\n")).is_err())
    }

    #[test]
    fn new_create_noop_evictor_explicit_with_sample_size_error() {
        assert!(new(get_frame(
            b"#6\r\n$create\r\n$foo\r\n$ss\r\n$100\r\n$ev\r\n$noop\r\n"
        ))
        .is_err())
    }

    #[test]
    fn new_create_lru_evictor_with_sample_size_no_error() {
        assert_eq!(
            new(get_frame(
                b"#6\r\n$create\r\n$foo\r\n$ss\r\n$100\r\n$ev\r\n$lru\r\n"
            ))
            .unwrap(),
            Command::Create(Create {
                keyspace: String::from("foo"),
                evictor: Evictor::Lru,
                max_memory_sample_size: Some(100)
            })
        )
    }

    #[test]
    fn new_create_lru_evictor_without_sample_size_no_error() {
        assert_eq!(
            new(get_frame(b"#4\r\n$create\r\n$foo\r\n$ev\r\n$lru\r\n")).unwrap(),
            Command::Create(Create {
                keyspace: String::from("foo"),
                evictor: Evictor::Lru,
                max_memory_sample_size: Some(MAX_MEMORY_SAMPLE_SIZE)
            })
        )
    }

    #[test]
    fn new_create_random_evictor_with_sample_size_no_error() {
        assert_eq!(
            new(get_frame(
                b"#6\r\n$create\r\n$foo\r\n$ss\r\n$100\r\n$ev\r\n$random\r\n"
            ))
            .unwrap(),
            Command::Create(Create {
                keyspace: String::from("foo"),
                evictor: Evictor::Random,
                max_memory_sample_size: Some(100)
            })
        )
    }

    #[test]
    fn new_create_random_evictor_without_sample_size_no_error() {
        assert_eq!(
            new(get_frame(b"#4\r\n$create\r\n$foo\r\n$ev\r\n$random\r\n")).unwrap(),
            Command::Create(Create {
                keyspace: String::from("foo"),
                evictor: Evictor::Random,
                max_memory_sample_size: Some(MAX_MEMORY_SAMPLE_SIZE)
            })
        )
    }

    #[test]
    fn new_create_invlaid_sample_size_error() {
        assert!(new(get_frame(
            b"#6\r\n$create\r\n$foo\r\n$ss\r\n$abc\r\n$ev\r\n$random\r\n"
        ))
        .is_err())
    }

    #[test]
    fn new_create_negative_sample_size_error() {
        assert!(new(get_frame(
            b"#6\r\n$create\r\n$foo\r\n$ss\r\n$-10000\r\n$ev\r\n$random\r\n"
        ))
        .is_err())
    }

    #[test]
    fn new_create_extra_args_error() {
        assert!(new(get_frame(
            b"#8\r\n$create\r\n$foo\r\n$ss\r\n$100\r\n$ev\r\n$random\r\n$foo\r\n$bar\r\n"
        ))
        .is_err())
    }

    #[test]
    fn new_set_without_keyspace_error() {
        assert!(new(get_frame(b"#1\r\n$set\r\n")).is_err())
    }

    #[test]
    fn new_set_without_key_error() {
        assert!(new(get_frame(b"#2\r\n$set\r\n$keyspace\r\n")).is_err())
    }

    #[test]
    fn new_set_without_value_error() {
        assert!(new(get_frame(b"#3\r\n$set\r\n$keyspace\r\n$foo\r\n")).is_err())
    }

    #[test]
    fn new_set_no_error() {
        assert_eq!(
            new(get_frame(b"#4\r\n$set\r\n$keyspace\r\n$foo\r\n$bar\r\n")).unwrap(),
            Command::Set(Set {
                keyspace: String::from("keyspace"),
                key: String::from("foo"),
                value: Bytes::from("bar")
            })
        )
    }

    #[test]
    fn new_set_extra_args_error() {
        assert!(new(get_frame(
            b"#5\r\n$set\r\n$keyspace\r\n$foo\r\n$bar\r\n$random\r\n"
        ))
        .is_err())
    }

    #[test]
    fn new_get_without_keyspace_error() {
        assert!(new(get_frame(b"#1\r\n$get\r\n")).is_err())
    }

    #[test]
    fn new_get_without_key_error() {
        assert!(new(get_frame(b"#2\r\n$get\r\n$keyspace\r\n")).is_err())
    }

    #[test]
    fn new_get_no_error() {
        assert_eq!(
            new(get_frame(b"#3\r\n$get\r\n$keyspace\r\n$foo\r\n")).unwrap(),
            Command::Get(Get {
                keyspace: String::from("keyspace"),
                key: String::from("foo")
            })
        )
    }

    #[test]
    fn new_get_extra_args_error() {
        assert!(new(get_frame(b"#4\r\n$get\r\n$keyspace\r\n$foo\r\n$bar\r\n")).is_err())
    }

    #[test]
    fn new_del_without_keyspace_error() {
        assert!(new(get_frame(b"#1\r\n$del\r\n")).is_err())
    }

    #[test]
    fn new_del_without_key_error() {
        assert!(new(get_frame(b"#2\r\n$del\r\n$keyspace\r\n")).is_err())
    }

    #[test]
    fn new_del_no_error() {
        assert_eq!(
            new(get_frame(b"#3\r\n$del\r\n$keyspace\r\n$foo\r\n")).unwrap(),
            Command::Del(Del {
                keyspace: String::from("keyspace"),
                key: String::from("foo")
            })
        )
    }

    #[test]
    fn new_del_extra_args_error() {
        assert!(new(get_frame(b"#4\r\n$del\r\n$keyspace\r\n$foo\r\n$bar\r\n")).is_err())
    }
}
