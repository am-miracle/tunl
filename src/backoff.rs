use std::time::Duration;

/// Exponential backoff with a cap. Starts at `base`, doubles each call, never
/// exceeds `cap`. Call `reset` after a successful connect to start over.
pub struct Backoff {
    base: Duration,
    current: Duration,
    cap: Duration,
}

impl Backoff {
    pub fn with_base(base: Duration, cap: Duration) -> Self {
        Self {
            base,
            current: base,
            cap,
        }
    }

    pub fn delay(&mut self) -> Duration {
        let d = self.current;
        self.current = self
            .current
            .checked_mul(2)
            .unwrap_or(Duration::MAX)
            .min(self.cap);
        d
    }

    pub fn reset(&mut self) {
        self.current = self.base;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sequence_is_exponential_with_cap() {
        let mut b = Backoff::with_base(Duration::from_secs(1), Duration::from_secs(15));
        assert_eq!(b.delay(), Duration::from_secs(1));
        assert_eq!(b.delay(), Duration::from_secs(2));
        assert_eq!(b.delay(), Duration::from_secs(4));
        assert_eq!(b.delay(), Duration::from_secs(8));
        assert_eq!(b.delay(), Duration::from_secs(15));
        assert_eq!(b.delay(), Duration::from_secs(15)); // stays capped
    }

    #[test]
    fn reset_restores_base() {
        let mut b = Backoff::with_base(Duration::from_secs(1), Duration::from_secs(15));
        b.delay();
        b.delay();
        b.delay(); // now at 8s
        b.reset();
        assert_eq!(b.delay(), Duration::from_secs(1));
    }

    #[test]
    fn with_base_uses_custom_values() {
        let mut b = Backoff::with_base(Duration::from_millis(100), Duration::from_millis(400));
        assert_eq!(b.delay(), Duration::from_millis(100));
        assert_eq!(b.delay(), Duration::from_millis(200));
        assert_eq!(b.delay(), Duration::from_millis(400));
        assert_eq!(b.delay(), Duration::from_millis(400)); // capped
    }

    #[test]
    fn delay_growth_saturates_before_cap() {
        let mut b = Backoff::with_base(Duration::MAX, Duration::MAX);
        assert_eq!(b.delay(), Duration::MAX);
        assert_eq!(b.delay(), Duration::MAX);
    }
}
