use super::command;
use super::connection::Connection;
use super::frame;
use super::keyspace;
use crate::shutdown::ShutdownListener;
use anyhow::Result;
use log::{error, info};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::signal::ctrl_c;
use tokio::sync::{broadcast, mpsc};

/// Holds the server state. We use a broadcast::Sender to notify all connections of a shutdown event.
/// We are using mpsc::Sender and mpsc::Receiver to wait for all the connections to be closed
pub struct Server {
    listener: TcpListener,
    _max_memory: u64,
    keyspace_manager: Arc<keyspace::KeyspaceManager>,
    shutdown_notifier: broadcast::Sender<()>,
    shutdown_complete_tx: mpsc::Sender<()>,
    shutdown_complete_rx: mpsc::Receiver<()>,
}

pub struct ConnectionHandler {
    pub connection: Connection,
    pub keyspace_manager: Arc<keyspace::KeyspaceManager>,
    shutdown_listener: ShutdownListener,
    _shutdown_complete_tx: mpsc::Sender<()>,
}

pub async fn start(listener: TcpListener, max_memory: u64) -> Result<()> {
    let server = Server::new(listener, max_memory)?;
    tokio::select! {
        result = server.start() => {
            match result {
                Ok(_) => {}
                Err(e) => {
                    error!("{}", e)
                }
            }
        }

        _ = ctrl_c() => {
            info!("Shutdown signal received, shutting down")
        }
    }

    let Server {
        shutdown_complete_tx,
        mut shutdown_complete_rx,
        shutdown_notifier,
        ..
    } = server;

    // Drop the shutdown notifier, signalling the begining of a shutdown event.
    // ShutdownListener will listen for such events and will shutdown the read loop
    // Upon shutdown of a read loop the ConnectionHandler is dropped which will in turn drop the
    // _shutdown_complete_tx signalling the completion of a cycle for one connection. This cycle
    // will be repeated for all the active connections.
    drop(shutdown_notifier);

    // Drop own shutdown_complete_tx otherwise the shutdown_complete_rx.recv() will wait forever
    drop(shutdown_complete_tx);

    // Wait for all other conections to be closed before shutting down the server
    shutdown_complete_rx.recv().await;

    Ok(())
}

impl Server {
    pub fn new(listener: TcpListener, max_memory: u64) -> Result<Self> {
        let (shutdown_notifier, _) = broadcast::channel(1);
        let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);
        let keyspace_manager = Arc::new(keyspace::KeyspaceManager::new(max_memory));
        info!("Server initialized");
        Ok(Server {
            shutdown_notifier,
            listener,
            shutdown_complete_rx,
            shutdown_complete_tx,
            keyspace_manager,
            _max_memory: max_memory,
        })
    }

    pub async fn start(&self) -> Result<()> {
        info!("Ready to accept connections");
        loop {
            let (stream, _) = self.listener.accept().await?;
            let mut connection_handler = ConnectionHandler::new(
                Connection::new(stream),
                self.keyspace_manager.clone(),
                ShutdownListener::new(self.shutdown_notifier.subscribe()),
                self.shutdown_complete_tx.clone(),
            );

            tokio::spawn(async move {
                if let Err(e) = connection_handler.handle().await {
                    error!("{}", e)
                }
            });
        }
    }
}

impl ConnectionHandler {
    pub fn new(
        connection: Connection,
        keyspace_manager: Arc<keyspace::KeyspaceManager>,
        shutdown_listener: ShutdownListener,
        shutdown_complete_tx: mpsc::Sender<()>,
    ) -> Self {
        ConnectionHandler {
            connection,
            keyspace_manager,
            shutdown_listener,
            _shutdown_complete_tx: shutdown_complete_tx,
        }
    }

    pub async fn handle(&mut self) -> Result<()> {
        while !self.shutdown_listener.shutdown() {
            let result = tokio::select! {
                _ = self.shutdown_listener.listen() => {
                    return Ok(())
                }

                frame = self.connection.read_frame() => frame
            };

            let frame = match result {
                Ok(frame) => frame,
                Err(e) => {
                    self.connection
                        .write_frame(frame::Frame::Error(e.to_string()))
                        .await?;
                    continue;
                }
            };

            let frame = match frame {
                Some(frame) => frame,
                None => return Ok(()),
            };

            let command = match command::new(frame) {
                Ok(cmd) => cmd,
                Err(e) => {
                    self.connection
                        .write_frame(frame::Frame::Error(e.to_string()))
                        .await?;
                    continue;
                }
            };
            command::exec(command, self).await?;
        }

        Ok(())
    }
}
