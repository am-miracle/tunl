use std::io;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};

use socket2::{Domain, Socket, Type};
use tokio::net::TcpListener;

pub async fn bind(address: SocketAddr) -> io::Result<TcpListener> {
    if address.ip() != IpAddr::V6(Ipv6Addr::UNSPECIFIED) {
        return TcpListener::bind(address).await;
    }

    // Set IPV6_V6ONLY explicitly so `::` has the same dual-stack behavior
    // on every supported operating system.
    let socket = Socket::new(Domain::IPV6, Type::STREAM, None)?;
    socket.set_only_v6(false)?;
    socket.set_nonblocking(true)?;
    socket.bind(&address.into())?;
    socket.listen(128)?;

    let listener: std::net::TcpListener = socket.into();
    TcpListener::from_std(listener)
}
