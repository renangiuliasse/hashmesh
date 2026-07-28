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
