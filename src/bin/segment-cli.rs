use anyhow::Result;
use clap::Parser;
use rustyline::error::ReadlineError;
use rustyline::Editor;
use segment::connection;
use segment::frame;
use tokio::net::TcpStream;

#[derive(Debug, Parser)]
struct Args {
    /// Specify the server port
    #[clap(long, default_value_t = 9890)]
    port: u16,

    /// Specify the server host
    #[clap(long, default_value = "127.0.0.1")]
    host: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let stream = TcpStream::connect(format!("{}:{}", args.host, args.port)).await?;
    let mut connection = connection::Connection::new(stream);
    let mut rl = Editor::<()>::new();
    loop {
        let readline = rl.readline(&format!("{}:{}> ", args.host, args.port));
        match readline {
            Ok(line) => {
                rl.add_history_entry(&line);
                let cmd = tokenize_command(&line);
                match connection.write_frame(frame::Frame::Array(cmd)).await {
                    Ok(_) => match connection.read_frame().await {
                        Ok(response) => {
                            if let Some(frame) = response {
                                println!("{}", frame)
                            } else {
                                println!("(null)")
                            }
                        }
                        Err(e) => {
                            eprintln!("{}", e)
                        }
                    },
                    Err(e) => {
                        eprintln!("{}", e);
                        break;
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                break;
            }
            Err(ReadlineError::Eof) => {
                break;
            }
            Err(e) => {
                eprintln!("{}", e);
                break;
            }
        }
    }

    Ok(())
}

fn tokenize_command(cmd: &str) -> Vec<frame::Frame> {
    let mut tokens = Vec::new();
    let mut token = String::new();

    let mut is_open_quote = false;

    for c in cmd.trim().chars() {
        if c == '"' && is_open_quote {
            is_open_quote = false;
            tokens.push(frame::Frame::String(token.clone()));
            token.clear()
        } else if c == '"' && !is_open_quote {
            is_open_quote = true
        } else if c == ' ' && is_open_quote {
            token.push(c);
        } else if c == ' ' && !is_open_quote {
            if !token.is_empty() {
                tokens.push(frame::Frame::String(token.clone()));
                token.clear();
            }
        } else {
            token.push(c)
        }
    }

    if !token.is_empty() {
        tokens.push(frame::Frame::String(token.clone()))
    }

    tokens
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenize_command_without_quotes_should_match() {
        let tokens = tokenize_command("set keyspace key value");
        assert_eq!(
            vec![
                frame::Frame::String("set".to_string()),
                frame::Frame::String("keyspace".to_string()),
                frame::Frame::String("key".to_string()),
                frame::Frame::String("value".to_string())
            ],
            tokens
        );
    }

    #[test]
    fn tokenize_command_all_quotes_should_match() {
        let tokens = tokenize_command("\"set\" \"keyspace\" \"key\" \"value\"");
        assert_eq!(
            vec![
                frame::Frame::String("set".to_string()),
                frame::Frame::String("keyspace".to_string()),
                frame::Frame::String("key".to_string()),
                frame::Frame::String("value".to_string())
            ],
            tokens
        );
    }

    #[test]
    fn tokenize_command_irregular_spaces_should_match() {
        let tokens = tokenize_command("\"set\"         \"keyspace\"     \"key\"       \"value\"");
        assert_eq!(
            vec![
                frame::Frame::String("set".to_string()),
                frame::Frame::String("keyspace".to_string()),
                frame::Frame::String("key".to_string()),
                frame::Frame::String("value".to_string())
            ],
            tokens
        );
    }

    #[test]
    fn tokenize_command_quote_in_command_should_mismatch() {
        let tokens = tokenize_command("\"set\"\" \"keyspace\" \"key\" \"value\"");
        assert_ne!(
            vec![
                frame::Frame::String("set".to_string()),
                frame::Frame::String("keyspace".to_string()),
                frame::Frame::String("key".to_string()),
                frame::Frame::String("value".to_string())
            ],
            tokens
        );
    }

    #[test]
    fn tokenize_command_space_in_command_should_match() {
        let tokens = tokenize_command("\"set\" \"keyspace\" \"this is a key\" \"value\"");
        assert_eq!(
            vec![
                frame::Frame::String("set".to_string()),
                frame::Frame::String("keyspace".to_string()),
                frame::Frame::String("this is a key".to_string()),
                frame::Frame::String("value".to_string())
            ],
            tokens
        );
    }

    #[test]
    fn tokenize_command_space_in_all_tokens_should_match() {
        let tokens = tokenize_command(
            "\"set command\" \"random keyspace\" \"this is a key\" \"this is a value\"",
        );
        assert_eq!(
            vec![
                frame::Frame::String("set command".to_string()),
                frame::Frame::String("random keyspace".to_string()),
                frame::Frame::String("this is a key".to_string()),
                frame::Frame::String("this is a value".to_string())
            ],
            tokens
        );
    }
}
