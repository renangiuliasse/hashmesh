#[cfg(test)]
mod node_tests {
    use std::time::Duration;

    use lib::{
        ClientPossibleArchitecture, Message, NodePossibleArchitecture, SoftwareVersion,
        client::{User, send_default_hanshake},
        get_free_port,
        node::{NodeAckResponse, NodeMessage, NodeSettings, handle_user_handshake},
    };
    use tokio::{
        net::{TcpListener, TcpStream},
        time::sleep,
    };

    #[tokio::test]
    async fn user_node_handshake() {
        let target_type = NodePossibleArchitecture::Neighbour;
        let server_info = NodeAckResponse::new(NodeSettings::new(target_type));

        let port = get_free_port();

        let sv_thread = tokio::spawn(async move {
            let listener = TcpListener::bind(format!("127.0.0.1:{}", port))
                .await
                .unwrap();

            match listener.accept().await {
                Err(_e) => panic!("error on accepting incoming connection"),
                Ok((mut stream, _socket)) => {
                    let (_stream, clienthandshake) =
                        handle_user_handshake(&mut stream, server_info)
                            .await
                            .unwrap();
                    assert_eq!(
                        clienthandshake.get_version(),
                        SoftwareVersion::project_version()
                    );
                    assert_eq!(
                        clienthandshake.get_target_type(),
                        ClientPossibleArchitecture::Neighbour
                    );
                }
            }
        });

        sleep(Duration::from_secs(1)).await;

        let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
            .await
            .unwrap();

        let user = User::default();
        let (_stream, message) =
            send_default_hanshake(&mut stream, &user, ClientPossibleArchitecture::Neighbour)
                .await
                .unwrap();

        if let Message::NodeMessage(nodemessage) = message {
            if let NodeMessage::NodeAckResponse(handshake) = nodemessage {
                assert_eq!(
                    handshake.get_config().architecture,
                    NodePossibleArchitecture::Neighbour
                );
            } else {
                panic!("Not a client handshake")
            }
        } else {
            panic!("Not a client message")
        }

        sv_thread.await.unwrap();
    }
}
