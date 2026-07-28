#[cfg(test)]
mod tests {
    use std::{io::ErrorKind, net::UdpSocket, sync::mpsc, thread, time::Duration};

    use lib::SoftwareVersion;
    use lib::client::ClientMessage::{self, ClientAck, ClientP2PAck};

    #[test]
    fn client_ping() {
        println!("Starting UDP Client Ping test...");
        let (tx, rx) = mpsc::channel();

        let server_handle = thread::spawn(move || {
            let server_socket =
                UdpSocket::bind("127.0.0.1:0").expect("couldn't bind server socket");
            let server_addr = server_socket
                .local_addr()
                .expect("couldn't get local address");

            tx.send(server_addr).expect("failed to send server address");

            server_socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .expect("set_read_timeout failed");

            let mut buf = [0; 512];
            match server_socket.recv_from(&mut buf) {
                Ok((number_of_bytes, src_addr)) => {
                    let received_data = &buf[..number_of_bytes];

                    let received_ping = ClientMessage::deserialize(received_data)
                        .expect("Server failed to deserialize Ping");

                    let ping_data = match received_ping {
                        ClientAck => panic!("Received a Client ACK."),
                        ClientP2PAck(_ack) => panic!("Received a Peer-to-Peer ACK. What????"),
                        ClientMessage::Ping(ping) => ping,
                    };

                    let project_version = SoftwareVersion::project_version();

                    assert_eq!(ping_data.peer, "peer");
                    assert_eq!(ping_data.version.major, project_version.major);
                    assert_eq!(ping_data.version.minor, project_version.minor);
                    assert_eq!(ping_data.version.patch, project_version.patch);

                    // answer
                    let pong_message = ClientMessage::new_ping();
                    let buf = ClientMessage::serialize(&pong_message)
                        .expect("Serialization of the ping response failed on parallel thread.");
                    server_socket
                        .send_to(&buf, src_addr)
                        .expect("Server failed to send pong");
                }
                Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                    panic!("Server read timed out!");
                }
                Err(e) => {
                    panic!("Server recv_from failed: {:?}", e);
                }
            }
        });

        let server_addr = rx.recv().expect("failed to receive server address");

        let client_socket = UdpSocket::bind("127.0.0.1:0").expect("couldn't bind client socket");
        client_socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("set_read_timeout failed");

        let client_ping = ClientMessage::new_ping();
        let serialized_ping = ClientMessage::serialize(&client_ping)
            .expect("Failed to serialize Ping object on main thread.");

        client_socket
            .send_to(&serialized_ping, server_addr)
            .expect("client failed to send data");

        let mut response_buf = [0; 512];
        match client_socket.recv_from(&mut response_buf) {
            Ok((number_of_bytes, _src_addr)) => {
                let response_data = &response_buf[..number_of_bytes];
                if let ClientMessage::Ping(response) = ClientMessage::deserialize(response_data)
                    .expect("Could not deserialize pong response on main thread.")
                {
                    println!(
                        "Received a response from a '{}' version ^{}.{}",
                        response.peer, response.version.major, response.version.minor
                    );
                } else {
                    panic!("Received a reponse other than a Ping Client object.")
                }
            }
            Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                panic!("Client read timed out waiting for pong!");
            }
            Err(e) => {
                panic!("Client recv_from failed: {:?}", e);
            }
        }

        server_handle.join().expect("server thread panicked");
    }
}

#[cfg(test)]
mod p2p_tests {
    use std::{io::ErrorKind, net::TcpListener as StdTcpListener, time::Duration};

    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        time::sleep,
    };

    use lib::{
        SoftwareVersion,
        client::{
            ClientMessage, Fingerprint,
            p2p::{p2p_initiate_handshake, p2p_waitfor_handshake},
        },
    };

    fn get_free_port() -> u16 {
        StdTcpListener::bind("127.0.0.1:0")
            .expect("Failed to bind to an ephemeral port")
            .local_addr()
            .expect("Failed to get local address")
            .port()
    }

    #[tokio::test]
    async fn test_p2p_handshake_success() {
        let port = get_free_port();
        let local_addr = format!("127.0.0.1:{}", port);
        let client_fingerprint = Fingerprint {
            key: "client_key".to_string(),
        };
        let server_fingerprint = Fingerprint {
            key: "server_key".to_string(),
        };

        let addr = local_addr.clone();
        let sv_fingerprint = server_fingerprint.clone();
        let sv_fingerprint2 = server_fingerprint.clone();
        let cl_fingerprint = client_fingerprint.clone();

        let server_handle = tokio::spawn(async move {
            let (_listener, received_ack) = p2p_waitfor_handshake(addr, sv_fingerprint)
                .await
                .expect("Server failed to wait for handshake");

            assert_eq!(received_ack.fingerprint.key, cl_fingerprint.key);
            assert_eq!(received_ack.version, SoftwareVersion::project_version());
        });

        sleep(Duration::from_millis(1000)).await;

        let (mut client_stream, received_ack) =
            p2p_initiate_handshake(local_addr.clone(), client_fingerprint.clone())
                .await
                .expect("Client failed to initiate handshake");

        let _ = client_stream.shutdown().await;
        server_handle.abort();

        assert_eq!(received_ack.fingerprint.key, sv_fingerprint2.key);
        assert_eq!(received_ack.version, SoftwareVersion::project_version());

        server_handle.await.expect("Server task failed");
    }

    #[tokio::test]
    async fn test_p2p_handshake_wrong_message_type() {
        let port = get_free_port();
        let local_addr = format!("127.0.0.1:{}", port);
        let client_fingerprint = Fingerprint {
            key: "client_key".to_string(),
        };

        let local_addr2 = local_addr.clone();
        let server_handle = tokio::spawn(async move {
            let listener = TcpListener::bind(local_addr.clone())
                .await
                .expect("Server failed to bind");
            let (mut stream, _) = listener
                .accept()
                .await
                .expect("Server failed to accept connection");

            let mut buf = vec![0; 512];
            let n = stream
                .read(&mut buf)
                .await
                .expect("Server failed to read from client");
            let _received_message =
                ClientMessage::deserialize(&buf[..n]).expect("Server failed to deserialize");

            // Server sends a Ping message instead of ClientP2PAck
            let ping_message = ClientMessage::new_ping();
            let ping_bytes = ClientMessage::serialize(&ping_message).unwrap();
            stream
                .write_all(&ping_bytes)
                .await
                .expect("Server failed to send Ping response");
        });

        sleep(Duration::from_millis(100)).await;

        let result = p2p_initiate_handshake(local_addr2.clone(), client_fingerprint).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Other);
        assert!(err.to_string().contains("Received Ping"));

        server_handle.await.expect("Server task failed");
    }
}
