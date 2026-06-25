use std::io;

use crate::io::AsyncReadWrite;

// Copies bytes between local and target in both directions concurrently.
//
// copy_bidirectional runs both copy loops in a single future. When one side
// sends EOF it shuts down the write half of the other side, then drains the
// remaining direction before returning. This is the correct half-close
// behaviour for TCP: one side can stop sending while still receiving.
pub async fn run(
    mut local: impl AsyncReadWrite,
    mut target: impl AsyncReadWrite,
) -> io::Result<()> {
    tokio::io::copy_bidirectional(&mut local, &mut target).await?;
    Ok(())
}
