use super::frame;
use super::keyspace::Evictor;
use super::server::ConnectionHandler;
use anyhow::{anyhow, Result};
use atoi::atoi;
use bytes::Bytes;
use std::{str, vec};

pub struct Parser {
    iterator: vec::IntoIter<frame::Frame>,
}

#[derive(Debug)]
pub enum Command {
    Get(Get),
    Set(Set),
    Del(Del),
    Create(Create),
}

#[derive(Debug)]
pub struct Get {
    key: String,
    keyspace: String,
}

#[derive(Debug)]
pub struct Set {
    key: String,
    value: Bytes,
    keyspace: String,
}

#[derive(Debug)]
pub struct Del {
    key: String,
    keyspace: String,
}

#[derive(Debug)]
pub struct Create {
    keyspace: String,
    evictor: Evictor,
}

impl Parser {
    pub fn new(frame: frame::Frame) -> Result<Self> {
        match frame {
            frame::Frame::Array(values) => Ok(Parser {
                iterator: values.into_iter(),
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
        self.next().is_none()
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

    pub async fn exec(&self, connection: &mut ConnectionHandler) {
        match connection
            .keyspace_manager
            .with_keyspace(&self.keyspace, |keyspace| Ok(keyspace.get(&self.key)))
        {
            Ok(response) => {
                if let Some(value) = response {
                    connection
                        .connection
                        .write_frame(frame::Frame::Blob(value))
                        .await
                        .unwrap()
                } else {
                    connection
                        .connection
                        .write_frame(frame::Frame::Null)
                        .await
                        .unwrap()
                }
            }
            Err(e) => connection
                .connection
                .write_frame(frame::Frame::Error(e.to_string()))
                .await
                .unwrap(),
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

    pub async fn exec(&self, connection: &mut ConnectionHandler) {
        match connection
            .keyspace_manager
            .with_keyspace(&self.keyspace, |keyspace| Ok(keyspace.del(&self.key)))
        {
            Ok(response) => connection
                .connection
                .write_frame(frame::Frame::Integer(response as i64))
                .await
                .unwrap(),
            Err(e) => connection
                .connection
                .write_frame(frame::Frame::Error(e.to_string()))
                .await
                .unwrap(),
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

    pub async fn exec(self, connection: &mut ConnectionHandler) {
        match connection
            .keyspace_manager
            .with_keyspace(&self.keyspace, |keyspace| {
                Ok(keyspace.set(self.key, self.value))
            }) {
            Ok(response) => connection
                .connection
                .write_frame(frame::Frame::Integer(response as i64))
                .await
                .unwrap(),
            Err(e) => connection
                .connection
                .write_frame(frame::Frame::Error(e.to_string()))
                .await
                .unwrap(),
        }
    }
}

impl Create {
    pub fn parse(parser: &mut Parser) -> Result<Self> {
        if let Some(keyspace) = parser.next_string()? {
            if let Some(evictor) = parser.next_string()? {
                if !parser.consumed() {
                    return Err(anyhow!(
                        "ERRPARSE Invalid command, wrong number of arguments for 'CREATE'"
                    ));
                }
                let evictor = match evictor.to_uppercase().as_str() {
                    "RANDOM" => Evictor::Random,
                    "NOOP" => Evictor::Noop,
                    "LRU" => Evictor::Lru,
                    _ => return Err(anyhow!("ERRPARSE Invalid value {} for 'EVICTOR'", evictor)),
                };
                return Ok(Create { keyspace, evictor });
            }
            return Err(anyhow!(
                "ERRPARSE Invalid command, missing argument 'EVICTOR'"
            ));
        }
        return Err(anyhow!(
            "ERRPARSE Invalid command, missing argument 'KEYSPACE'"
        ));
    }

    pub async fn exec(self, connection: &mut ConnectionHandler) {
        let response = connection
            .keyspace_manager
            .create(self.keyspace, self.evictor);
        connection
            .connection
            .write_frame(frame::Frame::Integer(response as i64))
            .await
            .unwrap();
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

    return Err(anyhow!(""));
}

pub async fn exec(cmd: Command, connection: &mut ConnectionHandler) {
    match cmd {
        Command::Create(cmd) => cmd.exec(connection).await,
        Command::Set(cmd) => cmd.exec(connection).await,
        Command::Del(cmd) => cmd.exec(connection).await,
        Command::Get(cmd) => cmd.exec(connection).await,
    }
}
