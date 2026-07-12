use std::net::{IpAddr, Ipv6Addr, SocketAddr};

use tokio::net::TcpStream;

#[tokio::test]
async fn ipv6_loopback_accepts_ipv6_connections() {
    let listener =
        match tunl::listener::bind(SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 0)).await {
            Ok(listener) => listener,
            Err(error) if error.kind() == std::io::ErrorKind::AddrNotAvailable => return,
            Err(error) => panic!("failed to bind IPv6 loopback: {error}"),
        };
    let address = listener.local_addr().unwrap();

    let (client, accepted) = tokio::join!(TcpStream::connect(address), listener.accept());
    client.unwrap();
    accepted.unwrap();
}

#[tokio::test]
async fn unspecified_ipv6_address_accepts_both_ip_families() {
    let listener =
        match tunl::listener::bind(SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0)).await {
            Ok(listener) => listener,
            Err(error) if error.kind() == std::io::ErrorKind::AddrNotAvailable => return,
            Err(error) => panic!("failed to bind dual-stack listener: {error}"),
        };
    let port = listener.local_addr().unwrap().port();

    let ipv4 = TcpStream::connect(("127.0.0.1", port));
    let accept_ipv4 = listener.accept();
    let (client, accepted) = tokio::join!(ipv4, accept_ipv4);
    client.unwrap();
    accepted.unwrap();

    let ipv6 = TcpStream::connect(("::1", port));
    let accept_ipv6 = listener.accept();
    let (client, accepted) = tokio::join!(ipv6, accept_ipv6);
    client.unwrap();
    accepted.unwrap();
}
