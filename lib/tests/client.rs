fn get_free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("Failed to bind to an ephemeral port")
        .local_addr()
        .expect("Failed to get local address")
        .port()
}

#[cfg(test)]
mod general_client_tests {
    use std::time::Duration;

    use lib::{
        BUFFER_DEFAULT_SIZE, SoftwareVersion,
        client::ClientMessage::{self, ClientAck, ClientP2PAck},
    };
    use tokio::{
        net::UdpSocket,
        time::{sleep, timeout},
    };

    async fn get_free_udp_port_async() -> u16 {
        UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("Failed to bind to an ephemeral port")
            .local_addr()
            .expect("Failed to get local address")
            .port()
    }

    #[test]
    fn serialization_sanity_check() {
        let ping = ClientMessage::build_ping();
        let bytes = ClientMessage::serialize(&ping)
            .expect("Could not serialize")
            .to_vec();

        let pong = ClientMessage::deserialize(&bytes).expect("Could not deserialize");
        match pong {
            ClientAck => {}
            ClientMessage::Ping(_) => return,
            ClientP2PAck(_) => {}
            ClientMessage::ClientP2PExchangePayload(_) => {}
        }

        panic!("Incorrect type match after deserialization");
    }

    #[tokio::test]
    async fn ping_test() {
        let server_port = get_free_udp_port_async().await;
        let server_addr = format!("127.0.0.1:{}", server_port);
        let client_bind_port = get_free_udp_port_async().await;
        let client_bind_addr = format!("127.0.0.1:{}", client_bind_port);

        let server_addr2 = server_addr.clone();

        let server_handle = tokio::spawn(async move {
            let server_socket = UdpSocket::bind(server_addr.clone())
                .await
                .expect("couldn't bind server socket");

            let mut buf = [0; BUFFER_DEFAULT_SIZE];
            let (number_of_bytes, src_addr) =
                timeout(Duration::from_secs(5), server_socket.recv_from(&mut buf))
                    .await
                    .expect("Server timed out waiting for ping")
                    .expect("Server recv_from failed");

            let received_data = &buf[..number_of_bytes];

            let received_ping = ClientMessage::deserialize(received_data)
                .expect("Server failed to deserialize Ping");

            let ping_data = match received_ping {
                ClientMessage::Ping(ping) => ping,
                ClientMessage::ClientAck => panic!("Incorrect deserialized type"),
                ClientMessage::ClientP2PAck(_ack) => panic!("Incorrect deserialized type"),
                ClientMessage::ClientP2PExchangePayload(_) => panic!("Incorrect deserialized type"),
            };

            let project_version = SoftwareVersion::project_version();

            assert_eq!(ping_data.peer, "peer");
            assert_eq!(ping_data.version.major, project_version.major);
            assert_eq!(ping_data.version.minor, project_version.minor);
            assert_eq!(ping_data.version.patch, project_version.patch);

            // sending a pong back
            let pong_message = ClientMessage::build_ping();
            let serialized_pong = ClientMessage::serialize(&pong_message)
                .expect("Serialization of the ping response failed on parallel task.");
            server_socket
                .send_to(&serialized_pong, src_addr)
                .await
                .expect("Server failed to send pong");
        });

        sleep(Duration::from_millis(100)).await;

        // sender
        let client_socket_for_sending = UdpSocket::bind(client_bind_addr.clone())
            .await
            .expect("couldn't bind client socket for sending");

        let client_ping_message = ClientMessage::build_ping();
        let serialized_ping = ClientMessage::serialize(&client_ping_message)
            .expect("Failed to serialize Ping object on main task.");

        client_socket_for_sending
            .send_to(&serialized_ping, server_addr2)
            .await
            .expect("client failed to send data");

        // the client needs to receive the pong.
        let mut response_buf = [0; BUFFER_DEFAULT_SIZE];
        let (number_of_bytes, _src_addr) = timeout(
            Duration::from_secs(5),
            client_socket_for_sending.recv_from(&mut response_buf),
        )
        .await
        .expect("Client timed out waiting for pong")
        .expect("Client recv_from failed");

        let response_data = &response_buf[..number_of_bytes];
        if let ClientMessage::Ping(response) = ClientMessage::deserialize(response_data)
            .expect("Could not deserialize pong response on main task.")
        {
            println!(
                "Received a response from a '{}' version ^{}.{}",
                response.peer, response.version.major, response.version.minor
            );
            let project_version = SoftwareVersion::project_version();
            assert_eq!(response.peer, "peer");
            assert_eq!(response.version.major, project_version.major);
            assert_eq!(response.version.minor, project_version.minor);
            assert_eq!(response.version.patch, project_version.patch);
        } else {
            panic!("Received a response other than a Ping Client object.")
        }

        server_handle.await.expect("server task panicked");
    }
}

#[cfg(test)]
mod p2p_tests {
    use std::{time::Duration};

    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        time::sleep,
    };

    use lib::{
        BUFFER_DEFAULT_SIZE, MAC_SIZE, SoftwareVersion, client::{
            ClientMessage, EncryptedMessage, Fingerprint, MAC, User,
            p2p::{p2p_initiate_handshake, p2p_send_encrypted_message, p2p_waitfor_handshake},
        }, fix_byte_buffer,
    };

    use crate::get_free_port;

    #[tokio::test]
    async fn p2p_handshake_success() {
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

        sleep(Duration::from_secs(2)).await;

        let (_client_stream, received_ack) =
            p2p_initiate_handshake(local_addr, client_fingerprint)
                .await
                .expect("Client failed to initiate handshake");

        assert_eq!(received_ack.fingerprint.key, sv_fingerprint2.key);
        assert_eq!(received_ack.version, SoftwareVersion::project_version());

        server_handle.await.expect("Server task failed");
    }

    #[tokio::test]
    async fn p2p_handshake_message_type_detection() {
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

            let mut buf = vec![0; BUFFER_DEFAULT_SIZE];
            let n = stream
                .read(&mut buf)
                .await
                .expect("Server failed to read from client");
            let _received_message =
                ClientMessage::deserialize(&buf[..n]).expect("Server failed to deserialize");

            // server sends a Ping message instead of ClientP2PAck
            let ping_message = ClientMessage::build_ping();
            let ping_bytes = ClientMessage::serialize(&ping_message).unwrap();
            stream
                .write_all(&ping_bytes)
                .await
                .expect("Server failed to send Ping response");
        });

        sleep(Duration::from_millis(100)).await;

        let result = p2p_initiate_handshake(local_addr2.clone(), client_fingerprint).await;

        assert!(result.is_err()); // has to throw because of incorrect type

        server_handle.await.expect("Server task failed");
    }

    #[tokio::test]
    async fn p2p_messaging() {
        let port = get_free_port();
        let local_addr = format!("127.0.0.1:{}", port);
        let client_fingerprint = Fingerprint {
            key: "client_key".to_string(),
        };

        let server_fingerprint = Fingerprint {
            key: "server_key".to_string(),
        };

        let local_addr2 = local_addr.clone();

        let waiter = p2p_waitfor_handshake(local_addr2, server_fingerprint);
        let waiter_thread = tokio::spawn(async move { waiter.await });

        sleep(Duration::from_millis(500)).await;

        let mut buf: [u8; BUFFER_DEFAULT_SIZE] = [0; BUFFER_DEFAULT_SIZE];

        const MESSAGE: &str = "a random unencrypted message.";

        let sender_thread = tokio::spawn(async move {
            let (stream, _) = p2p_initiate_handshake(local_addr, client_fingerprint)
                .await
                .expect("Could not start initiator client on sender thread");

            let message = MESSAGE.to_string();

            let mac = fix_byte_buffer(&[7u8; MAC_SIZE], size_of::<MAC>());
            let msg_bytes = fix_byte_buffer(message.as_bytes(), size_of::<EncryptedMessage>());

            let user = User::new();
            let mut stream = p2p_send_encrypted_message(stream, user, msg_bytes, mac)
                .await
                .expect("Could not send message on sender thread");

            let _ = stream.shutdown().await;
        });

        let (mut stream, _) = waiter_thread
            .await
            .expect("Could not finish waiter thread")
            .expect("Could not extract TcpStream from waiter thread");

        let bytes_read = stream
            .read(&mut buf)
            .await
            .expect("Could not receive data on main thread");
        let data = &buf[..bytes_read];
        let data_message = ClientMessage::deserialize(data).expect("Could not deserialize object");

        let _ = sender_thread.await;

        match data_message {
            ClientMessage::ClientAck => {}
            ClientMessage::ClientP2PAck(_) => {}
            ClientMessage::Ping(_) => {}
            ClientMessage::ClientP2PExchangePayload(payload) => {
                let message_received = payload
                    .read_message()
                    .expect("Could not read string, invalid");

                // as we know it is unencrypted, we can just filter null bytes and read it as a string
                let message_bytes = message_received.into_bytes();
                let message = str::from_utf8(&message_bytes).unwrap();
                let fixed_message: String = message.chars().filter(|c| *c != '\0').collect();
                assert_eq!(fixed_message, MESSAGE.to_string());
            }
        }
    }
}
