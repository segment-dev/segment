use crate::frame;
use anyhow::{anyhow, Result};
use bytes::{Buf, Bytes, BytesMut};
use std::io::Cursor;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub struct Connection {
    stream: TcpStream,
    buffer: BytesMut,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        Connection {
            stream,
            buffer: BytesMut::with_capacity(4096),
        }
    }

    pub async fn read_frame(&mut self) -> Result<Option<frame::Frame>> {
        loop {
            if let Some(frame) = self.parse_frame().await? {
                return Ok(Some(frame));
            }

            if self.stream.read_buf(&mut self.buffer).await? == 0 {
                // if the buffer is empty at the time of connection termination
                // it was a clean termination, but if there was data present in the
                // buffer at the time of termination it was unclean
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err(anyhow!("connection reset by peer"));
                }
            }
        }
    }

    async fn parse_frame(&mut self) -> Result<Option<frame::Frame>, frame::ParseError> {
        let mut cursor = Cursor::new(&self.buffer[..]);
        match frame::parse(&mut cursor) {
            Ok(frame) => {
                let advance_by = cursor.position() as usize;
                self.buffer.advance(advance_by);
                Ok(Some(frame))
            }
            Err(frame::ParseError::IncompleteFrame) => Ok(None),
            Err(e) => {
                // If there is an error in parsing the frame
                // we want to move the buffer ahead otherwise
                // there will be an infinite loop where we will
                // keep on readin the malformed frame again and again
                let advance_by = cursor.position() as usize;
                self.buffer.advance(advance_by);
                Err(e)
            }
        }
    }

    pub async fn write_frame(&mut self, frame: frame::Frame) -> Result<()> {
        match frame {
            frame::Frame::Array(values) => {
                self.stream.write_u8(b'#').await?;
                self.stream
                    .write_all(values.len().to_string().as_bytes())
                    .await?;
                self.stream.write_all(b"\r\n").await?;

                for value in values {
                    self.write(value).await?;
                }
            }
            _ => self.write(frame).await?,
        }

        Ok(())
    }

    async fn write(&mut self, frame: frame::Frame) -> Result<()> {
        match frame {
            frame::Frame::String(data) => {
                self.stream.write_u8(b'$').await?;
                self.stream.write_all(data.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }

            frame::Frame::Integer(data) => {
                self.stream.write_u8(b'%').await?;
                self.stream.write_all(data.to_string().as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }

            frame::Frame::Error(data) => {
                self.stream.write_u8(b'!').await?;
                self.stream.write_all(data.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }

            frame::Frame::Null => {
                self.stream.write_all(b"*-1\r\n\r\n").await?;
            }

            frame::Frame::Blob(data) => {
                let len = data.len();
                self.stream.write_u8(b'*').await?;
                self.stream.write_all(len.to_string().as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
                self.stream.write_all(&data).await?;
                self.stream.write_all(b"\r\n").await?;
            }

            _ => unreachable!(),
        }

        Ok(())
    }

    pub async fn write_string(&mut self, data: &str) -> Result<()> {
        self.stream.write_u8(b'$').await?;
        self.stream.write_all(data.as_bytes()).await?;
        self.stream.write_all(b"\r\n").await?;

        Ok(())
    }

    pub async fn write_integer(&mut self, data: i64) -> Result<()> {
        self.stream.write_u8(b'%').await?;
        self.stream.write_all(data.to_string().as_bytes()).await?;
        self.stream.write_all(b"\r\n").await?;

        Ok(())
    }

    pub async fn write_error(&mut self, data: &str) -> Result<()> {
        self.stream.write_u8(b'!').await?;
        self.stream.write_all(data.as_bytes()).await?;
        self.stream.write_all(b"\r\n").await?;

        Ok(())
    }

    pub async fn write_null(&mut self) -> Result<()> {
        self.stream.write_all(b"*-1\r\n\r\n").await?;
        Ok(())
    }

    pub async fn write_blob(&mut self, data: &Bytes) -> Result<()> {
        self.stream.write_u8(b'*').await?;
        self.stream
            .write_all(data.len().to_string().as_bytes())
            .await?;
        self.stream.write_all(b"\r\n").await?;
        self.stream.write_all(data).await?;
        self.stream.write_all(b"\r\n").await?;

        Ok(())
    }
}
