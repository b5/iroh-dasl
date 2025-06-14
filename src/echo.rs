use anyhow::Result;
use iroh::{Endpoint, NodeAddr, endpoint::Connection, protocol::ProtocolHandler};
use n0_future::boxed::BoxFuture;

pub const ALPN: &[u8] = b"iroh/echo/0";

#[derive(Debug, Clone)]
pub struct Echo;

impl Echo {
    ///
    pub async fn echo(endpoint: Endpoint, addr: NodeAddr, phrase: &str) -> Result<()> {
        // Open a connection to the accepting node
        let conn = endpoint.connect(addr, ALPN).await?;

        // Open a bidirectional QUIC stream
        let (mut send, mut recv) = conn.open_bi().await?;

        // Send some data to be echoed
        send.write_all(phrase.as_bytes()).await?;

        // Signal the end of data for this particular stream
        send.finish()?;

        // Receive the echo, but limit reading up to maximum 1000 bytes
        let response = recv.read_to_end(1000).await?;
        assert_eq!(&response, b"Hello, world!");

        // Explicitly close the whole connection.
        conn.close(0u32.into(), b"bye!");

        // The above call only queues a close message to be sent (see how it's not async!).
        // We need to actually call this to make sure this message is sent out.
        endpoint.close().await;
        // If we don't call this, but continue using the endpoint, we then the queued
        // close call will eventually be picked up and sent.
        // But always try to wait for endpoint.close().await to go through before dropping
        // the endpoint to ensure any queued messages are sent through and connections are
        // closed gracefully.
        Ok(())
    }
}

impl ProtocolHandler for Echo {
    /// The `accept` method is called for each incoming connection for our ALPN.
    ///
    /// The returned future runs on a newly spawned tokio task, so it can run as long as
    /// the connection lasts.
    fn accept(&self, connection: Connection) -> BoxFuture<Result<()>> {
        // We have to return a boxed future from the handler.
        Box::pin(async move {
            // We can get the remote's node id from the connection.
            let node_id = connection.remote_node_id()?;
            println!("accepted connection from {node_id}");

            // Our protocol is a simple request-response protocol, so we expect the
            // connecting peer to open a single bi-directional stream.
            let (mut send, mut recv) = connection.accept_bi().await?;

            // Echo any bytes received back directly.
            // This will keep copying until the sender signals the end of data on the stream.
            let bytes_sent = tokio::io::copy(&mut recv, &mut send).await?;
            println!("Copied over {bytes_sent} byte(s)");

            // By calling `finish` on the send stream we signal that we will not send anything
            // further, which makes the receive stream on the other end terminate.
            send.finish()?;

            // Wait until the remote closes the connection, which it does once it
            // received the response.
            connection.closed().await;

            Ok(())
        })
    }
}
