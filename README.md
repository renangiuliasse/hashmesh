<div align='center'>

  [![build](https://github.com/renangiuliasse/hashmesh/actions/workflows/rust%20build.yml/badge.svg)](https://github.com/renangiuliasse/hashmesh/actions/workflows/rust%20build.yml)
  [![Cargo doc](https://github.com/renangiuliasse/hashmesh/actions/workflows/rust%20doc%20with%20gh%20pages.yml/badge.svg)](https://github.com/renangiuliasse/hashmesh/actions/workflows/rust%20doc%20with%20gh%20pages.yml)

  [library documentation](https://renangiuliasse.github.io/hashmesh/lib/)
  
</div>

# HashMesh
HashMesh is decentralized, and peer-to-peer messaging application designed for robust and private communication. Built with Rust for performance and security, 
HashMesh aims to provide a resilient communication network where users maintain control over their data, and share only what's needed.

## Project Overview
HashMesh operates on a custom binary protocol over TCP, avoiding traditional HTTP/JSON overhead for efficiency and direct control over the communication stack.

### Key Features
*   **Custom Binary Protocol:** Efficient, low-overhead communication using `rkyv` for zero-copy serialization/deserialization.
*   **Peer-to-Peer (P2P) Architecture:** Direct client-to-client connections for enhanced privacy and resilience.
*   **Out-of-band Security:** End-to-end encryption and authentication is all trusted on the user, we don't take care of what you want to share and we don't track you. Out-of-band encryption mechanisms are central to the protocol design.
*   **Cross-Platform (Planned):** Rust backend for core logic, Flutter frontend for desktop and mobile applications.
*   **Decentralized Discovery (Planned):** Mechanisms for clients to discover each other without relying on a single central server.
*   **Message Routing:** Intelligent routing of messages through connected peers to reach the intended recipient without sharing who.

### Network architecture
The network consists of user devices and server nodes. Messages from a sender are routed through a server node to
a destination. The server's role is to forward the message to either confirmed nodes that can reach the destination
or to nodes that can help map the destination.

Servers establish connections through a handshake process. They exchange version and type information. A successful
handshake results in an "ok" response. If there's a denial or failure, a reason or nothing is returned.

### Server Configuration
Server behavior can be configured using parameters such as:
- `rate_limit`: The maximum operation rate in seconds.
- `timeout`: The duration after which an operation times out, in seconds.
- `user_count`: An optional parameter related to user counts.
- `userid_format_rule`: A regular expression for user ID format, defaulting to UUID V7.
- `max_message_length`: The maximum allowed length for messages.

### Message Structure
All communication is encapsulated within a top-level `Message` enum, which can contain various sub-messages like `ClientMessage`, `NodeMessage`, `HandshakeMessage`, etc. Each message includes:

*   **`version`**: Protocol version for compatibility.
*   **`own_uid`**: Sender's unique identifier (UUID).
*   **`type`**: Indicates the message's purpose (e.g., "web", "p2p", "broadcast").
*   **`encrypted_msg`**: The actual payload, encrypted for confidentiality.
*   **`dest_uid`**: Recipient's unique identifier.
*   **`ignore_uid_list`**: A list of UUIDs to avoid when routing, preventing loops.
*   **`MAC`**: Message Authentication Code for integrity and authenticity.

### Custom TCP Handshake
Upon establishing a raw TCP connection, clients engage in a custom handshake process:

1.  **Initial Exchange:** Clients exchange their protocol `version`, `own_uid`, and optionally a connection `type`.
2.  **Key Exchange:** Securely establish a shared symmetric encryption key using a Diffie-Hellman-like key exchange.
3.  **Authentication:** Verify peer identity using digital signatures and public/private key pairs.
4.  **Session Establishment:** Confirm the secure channel is ready for application data.

This handshake ensures that all subsequent application data is encrypted and authenticated, providing end-to-end security.
Please note that this project assumes that encrypted data **can be decrypted by the recepient!** Encryption keys are assumed
to have been shared **out of band, through a trusted channel!**

### Message Routing
Messages are routed through connected peers. If a message's `dest_uid` is not the current recipient, 
the message is re-encrypted (if necessary) and forwarded to other connected peers, using the `ignore_uid_list` to prevent redundant forwarding.

## Getting Started

### Prerequisites

*   Rust (latest stable)
*   Cargo (latest stable)

### Building the Rust Components
Navigate to the `lib/` folder of the HashMesh repository and use Cargo:

```bash
cargo build --relase
```

### Integrating with Flutter (FFI)
The `lib` Rust crate will be compiled as a native library (e.g., `.so`, `.dylib`, `.dll`) and linked into the Flutter application. Tools 
like `flutter_rust_bridge` can simplify this process significantly.

### Running the Application
Detailed instructions for running the Flutter application and setting up a local P2P network will be provided as the project develops.

## Contributing
We welcome contributions to HashMesh! Please refer to our `CONTRIBUTING.md` (to be created) for guidelines on how to get involved.

## License
This project is licensed under the [GPLv3] - see the `LICENSE` file for details.
