use atoi::atoi;
use bytes::{Buf, Bytes};
use std::io::Cursor;
use std::str;
use std::string::FromUtf8Error;
use thiserror::Error;

#[derive(Debug, PartialEq)]
pub enum Frame {
    String(String),
    Blob(Bytes),
    Integer(i64),
    Null,
    Array(Vec<Frame>),
    Error(String),
}

#[derive(Debug, Error, PartialEq)]
pub enum ParseError {
    #[error("incomplete frame")]
    IncompleteFrame,

    #[error("invalid frame")]
    InvalidFrame,
}

pub fn parse(cursor: &mut Cursor<&[u8]>) -> Result<Frame, ParseError> {
    let line = get_line(cursor)?;
    if line.is_empty() {
        return Err(ParseError::InvalidFrame);
    }
    // First byte of the frame is always the type identifier
    let type_identifier = line[0];
    // All the data after the first byte is frame data
    let frame_data = &line[1..line.len()];

    match type_identifier {
        b'$' => Ok(Frame::String(String::from_utf8(frame_data.to_vec())?)),
        b'%' => Ok(Frame::Integer(
            atoi::<i64>(frame_data).ok_or(ParseError::InvalidFrame)?,
        )),
        b'!' => Ok(Frame::Error(String::from_utf8(frame_data.to_vec())?)),
        b'*' => {
            // If the length of the blob is -1, it might be a null frame
            if frame_data == b"-1" {
                // We skip 2 bytes to skip the leading CRLF
                skip(2, cursor)?;
                return Ok(Frame::Null);
            }
            let length = atoi::<usize>(frame_data).ok_or(ParseError::InvalidFrame)?;

            // We check if we have enough data to parse the frame
            // length+2 makes sure that we are accounting for leading CRLF
            if cursor.remaining() < length + 2 {
                return Err(ParseError::IncompleteFrame);
            }

            let frame = Bytes::copy_from_slice(&cursor.chunk()[..length]);
            skip(length + 2, cursor)?;
            Ok(Frame::Blob(frame))
        }
        b'#' => {
            let length = atoi::<usize>(frame_data).ok_or(ParseError::InvalidFrame)?;
            let mut values = Vec::with_capacity(length);

            for _ in 0..length {
                match parse(cursor) {
                    Ok(frame) => match frame {
                        // Nested arrays are not supported
                        Frame::Array(_) => return Err(ParseError::InvalidFrame),
                        _ => values.push(frame),
                    },
                    Err(e) => return Err(e),
                }
            }
            Ok(Frame::Array(values))
        }
        _ => Err(ParseError::InvalidFrame),
    }
}

/// Tries parse a line from the data. A line is a CRLF terminated sequence.
fn get_line<'a>(cursor: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], ParseError> {
    if !cursor.has_remaining() {
        return Err(ParseError::IncompleteFrame);
    }

    // Start reading from the current position of the cursor
    // and read till the second last position
    let start = cursor.position() as usize;
    let end = cursor.get_ref().len() - 1;

    for i in start..end {
        // If the sequence is CRLF terminated we return the slice
        // and move the cursor to the begining of the next line
        if cursor.get_ref()[i] == b'\r' && cursor.get_ref()[i + 1] == b'\n' {
            cursor.set_position((i + 2) as u64);
            return Ok(&cursor.get_ref()[start..i]);
        }
    }

    // If after iterating over the entire cursor we could not find an ending
    // more data needs to be buffered
    Err(ParseError::IncompleteFrame)
}

/// Skips ahead n positions in the cursor
fn skip(n: usize, cursor: &mut Cursor<&[u8]>) -> Result<(), ParseError> {
    if cursor.remaining() < n {
        return Err(ParseError::IncompleteFrame);
    }
    cursor.advance(n);
    Ok(())
}

impl From<FromUtf8Error> for ParseError {
    fn from(_: FromUtf8Error) -> Self {
        ParseError::InvalidFrame
    }
}

impl std::fmt::Display for Frame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Frame::Array(v) => {
                for (i, value) in v.iter().enumerate() {
                    writeln!(f, "{}) {}", i, value)?
                }
            }
            Frame::Blob(v) => match str::from_utf8(&v[..]) {
                Ok(v) => write!(f, "(blob) {}", v)?,
                Err(e) => write!(f, "(error) {}", e)?,
            },
            Frame::Error(v) => write!(f, "(error) {}", v)?,
            Frame::Integer(v) => write!(f, "(integer) {}", v)?,
            Frame::Null => write!(f, "(null)")?,
            Frame::String(v) => write!(f, "(string) {}", v)?,
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn get_cursor(data: &[u8]) -> Cursor<&[u8]> {
        Cursor::new(data)
    }

    #[test]
    fn get_line_no_data_incomplete_frame() {
        let mut cursor = get_cursor(b"");
        assert_eq!(get_line(&mut cursor), Err(ParseError::IncompleteFrame));
    }

    #[test]
    fn get_line_no_cr_incomplete_frame() {
        let mut cursor = get_cursor(b"$test\n");
        assert_eq!(get_line(&mut cursor), Err(ParseError::IncompleteFrame));
    }

    #[test]
    fn get_line_no_lf_incomplete_frame() {
        let mut cursor = get_cursor(b"$test\r");
        assert_eq!(get_line(&mut cursor), Err(ParseError::IncompleteFrame));
    }

    #[test]
    fn get_line_no_crlf_incomplete_frame() {
        let mut cursor = get_cursor(b"$test");
        assert_eq!(get_line(&mut cursor), Err(ParseError::IncompleteFrame));
    }

    #[test]
    fn get_line_normal_line_no_error() {
        let mut cursor = get_cursor(b"$test\r\n");
        assert_eq!(get_line(&mut cursor).unwrap(), b"$test");
    }

    #[test]
    fn skip_more_than_length_incomplete_frame() {
        let mut cursor = get_cursor(b"$test");
        assert_eq!(skip(100, &mut cursor), Err(ParseError::IncompleteFrame));
    }

    #[test]
    fn skip_zero_length_no_error() {
        let mut cursor = get_cursor(b"$test");
        skip(0, &mut cursor).unwrap();
        assert_eq!(cursor.remaining(), 5)
    }

    #[test]
    fn parse_unknown_type() {
        let mut cursor = get_cursor(b"(this is a frame with unknown type\r\n");
        assert_eq!(parse(&mut cursor), Err(ParseError::InvalidFrame))
    }

    #[test]
    fn parse_empty_invalid_frame() {
        let mut cursor = get_cursor(b"");
        assert_eq!(parse(&mut cursor), Err(ParseError::IncompleteFrame))
    }

    #[test]
    fn parse_string_no_error() {
        let mut cursor = get_cursor(b"$this is a random string\r\n");
        assert_eq!(
            parse(&mut cursor),
            Ok(Frame::String("this is a random string".to_string()))
        )
    }

    #[test]
    fn parse_empty_string_no_error() {
        let mut cursor = get_cursor(b"$\r\n");
        assert_eq!(parse(&mut cursor), Ok(Frame::String("".to_string())))
    }

    #[test]
    fn parse_string_incomplete_frame() {
        let mut cursor = get_cursor(b"$this is a random string\r");
        assert_eq!(parse(&mut cursor), Err(ParseError::IncompleteFrame))
    }

    #[test]
    fn parse_positive_integer_no_error() {
        let mut cursor = get_cursor(b"%1000\r\n");
        assert_eq!(parse(&mut cursor), Ok(Frame::Integer(1000)))
    }

    #[test]
    fn parse_negative_integer_no_error() {
        let mut cursor = get_cursor(b"%-1000\r\n");
        assert_eq!(parse(&mut cursor), Ok(Frame::Integer(-1000)))
    }

    #[test]
    fn parse_zero_no_error() {
        let mut cursor = get_cursor(b"%0\r\n");
        assert_eq!(parse(&mut cursor), Ok(Frame::Integer(0)))
    }

    #[test]
    fn parse_multiple_zeroes_no_error() {
        let mut cursor = get_cursor(b"%00000000\r\n");
        assert_eq!(parse(&mut cursor), Ok(Frame::Integer(0)))
    }

    #[test]
    fn parse_float_no_error() {
        let mut cursor = get_cursor(b"%10000.12000\r\n");
        assert_eq!(parse(&mut cursor), Ok(Frame::Integer(10000)))
    }

    #[test]
    fn parse_empty_frame_invalid_frame() {
        let mut cursor = get_cursor(b"%\r\n");
        assert_eq!(parse(&mut cursor), Err(ParseError::InvalidFrame))
    }

    #[test]
    fn parse_invalid_integer_invalid_frame() {
        let mut cursor = get_cursor(b"%abc\r\n");
        assert_eq!(parse(&mut cursor), Err(ParseError::InvalidFrame))
    }

    #[test]
    fn parse_integer_incomplete_frame() {
        let mut cursor = get_cursor(b"%100\n");
        assert_eq!(parse(&mut cursor), Err(ParseError::IncompleteFrame))
    }

    #[test]
    fn parse_error_no_error() {
        let mut cursor = get_cursor(b"!this is an error frame\r\n");
        assert_eq!(
            parse(&mut cursor),
            Ok(Frame::Error("this is an error frame".to_string()))
        )
    }

    #[test]
    fn parse_empty_error_no_error() {
        let mut cursor = get_cursor(b"!\r\n");
        assert_eq!(parse(&mut cursor), Ok(Frame::Error("".to_string())))
    }

    #[test]
    fn parse_error_incomplete_frame() {
        let mut cursor = get_cursor(b"!this is a random error\r");
        assert_eq!(parse(&mut cursor), Err(ParseError::IncompleteFrame))
    }

    #[test]
    fn parse_null_no_error() {
        let mut cursor = get_cursor(b"*-1\r\n\r\n");
        assert_eq!(parse(&mut cursor), Ok(Frame::Null))
    }

    #[test]
    fn parse_null_with_data_no_error() {
        let mut cursor = get_cursor(b"*-1\r\nhello world\r\n");
        assert_eq!(parse(&mut cursor), Ok(Frame::Null))
    }

    #[test]
    fn parse_null_invalid_frame() {
        let mut cursor = get_cursor(b"*-1\n\r\n");
        assert_eq!(parse(&mut cursor), Err(ParseError::InvalidFrame))
    }

    #[test]
    fn parse_null_incomplete_frame() {
        let mut cursor = get_cursor(b"*-1\r\n");
        assert_eq!(parse(&mut cursor), Err(ParseError::IncompleteFrame))
    }

    #[test]
    fn parse_blob_no_error() {
        let mut cursor = get_cursor(b"*7\r\nsegment\r\n");
        assert_eq!(parse(&mut cursor), Ok(Frame::Blob(Bytes::from("segment"))))
    }

    #[test]
    fn parse_blob_with_delimiters_no_error() {
        let mut cursor = get_cursor(b"*9\r\nseg\r\nment\r\n");
        assert_eq!(
            parse(&mut cursor),
            Ok(Frame::Blob(Bytes::from("seg\r\nment")))
        )
    }

    #[test]
    fn parse_empty_blob_no_error() {
        let mut cursor = get_cursor(b"*0\r\n\r\n");
        assert_eq!(parse(&mut cursor), Ok(Frame::Blob(Bytes::from(""))))
    }

    #[test]
    fn parse_blob_length_less_than_data_no_error() {
        let mut cursor = get_cursor(b"*7\r\nseg\r\nment\r\n");
        assert_eq!(
            parse(&mut cursor),
            Ok(Frame::Blob(Bytes::from("seg\r\nme")))
        )
    }

    #[test]
    fn parse_blob_length_greater_than_data_incomplete_frame() {
        let mut cursor = get_cursor(b"*10\r\nseg\r\nment\r\n");
        assert_eq!(parse(&mut cursor), Err(ParseError::IncompleteFrame))
    }

    #[test]
    fn parse_blob_invalid_length_invalid_frame() {
        let mut cursor = get_cursor(b"*abc\r\nseg\r\nment\r\n");
        assert_eq!(parse(&mut cursor), Err(ParseError::InvalidFrame))
    }

    #[test]
    fn parse_blob_negative_length_invalid_frame() {
        let mut cursor = get_cursor(b"*-1000\r\nseg\r\nment\r\n");
        assert_eq!(parse(&mut cursor), Err(ParseError::InvalidFrame))
    }

    #[test]
    fn parse_array_no_error() {
        let mut cursor = get_cursor(b"#1\r\n$foo\r\n");
        assert_eq!(
            parse(&mut cursor),
            Ok(Frame::Array(vec![Frame::String("foo".to_string())]))
        )
    }
    #[test]
    fn parse_empty_array_no_error() {
        let mut cursor = get_cursor(b"#0\r\n");
        assert_eq!(parse(&mut cursor), Ok(Frame::Array(vec![])))
    }

    #[test]
    fn parse_array_incomplete_frame() {
        let mut cursor = get_cursor(b"#0\r");
        assert_eq!(parse(&mut cursor), Err(ParseError::IncompleteFrame))
    }

    #[test]
    fn parse_array_child_incomplete_incomplete_frame() {
        let mut cursor = get_cursor(b"#1\r\n$sachin\r");
        assert_eq!(parse(&mut cursor), Err(ParseError::IncompleteFrame))
    }

    #[test]
    fn parse_array_nested_array_invalid_frame() {
        let mut cursor = get_cursor(b"#1\r\n#0\r\n");
        assert_eq!(parse(&mut cursor), Err(ParseError::InvalidFrame))
    }

    #[test]
    fn parse_array_invalid_length_invalid_frame() {
        let mut cursor = get_cursor(b"#abc\r\n$foo\r\n");
        assert_eq!(parse(&mut cursor), Err(ParseError::InvalidFrame))
    }

    #[test]
    fn parse_array_negative_length_invalid_frame() {
        let mut cursor = get_cursor(b"#-1\r\n$foo\r\n");
        assert_eq!(parse(&mut cursor), Err(ParseError::InvalidFrame))
    }
}
