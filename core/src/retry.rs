/*!
# Retry Module

This module defines retry policies for failed tasks in ChopFlow.

Retry policies determine:
- Whether a failed task should be retried
- How long to wait before the next retry attempt
- The maximum number of retry attempts
- Backoff strategies to avoid overwhelming the system

The module includes:
- The `RetryPolicy` enum with different retry strategies
- Support for no retries, fixed interval, and exponential backoff
- Configurable jitter to prevent retry storms
- Methods to calculate next retry times and durations

Retry policies are crucial for building resilient distributed systems
that can recover from transient failures without manual intervention.
*/

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::time::Duration as StdDuration;

/// Retry policy for failed tasks
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RetryPolicy {
    /// No retries
    NoRetry,

    /// Fixed time between retries
    Fixed {
        /// Delay between retries in seconds
        delay_seconds: u64,
        /// Maximum number of retries
        max_retries: u32,
    },

    /// Exponential backoff (delay doubles after each retry)
    ExponentialBackoff {
        /// Initial delay in seconds
        initial_delay_seconds: u64,
        /// Maximum delay in seconds
        max_delay_seconds: u64,
        /// Maximum number of retries
        max_retries: u32,
        /// Random jitter factor (0.0 to 1.0)
        jitter: f64,
    },
}

impl RetryPolicy {
    /// Create a new exponential backoff policy with defaults
    pub fn exponential_backoff(max_retries: u32) -> Self {
        Self::ExponentialBackoff {
            initial_delay_seconds: 5,
            max_delay_seconds: 300, // 5 minutes
            max_retries,
            jitter: 0.1,
        }
    }

    /// Calculate the next retry time based on the current retry count
    pub fn next_retry_time(&self, retry_count: u32) -> Option<DateTime<Utc>> {
        if retry_count == 0 {
            return Some(Utc::now());
        }

        let delay_seconds = match self {
            Self::NoRetry => return None,

            Self::Fixed {
                delay_seconds,
                max_retries,
            } => {
                if retry_count > *max_retries {
                    return None;
                }
                *delay_seconds
            }

            Self::ExponentialBackoff {
                initial_delay_seconds,
                max_delay_seconds,
                max_retries,
                jitter,
            } => {
                if retry_count > *max_retries {
                    return None;
                }

                // Calculate exponential delay
                let factor = 2u64.pow(retry_count - 1);
                let delay = initial_delay_seconds * factor;
                let delay = std::cmp::min(delay, *max_delay_seconds);

                // Add jitter
                if *jitter > 0.0 {
                    use rand::Rng;
                    let mut rng = rand::thread_rng();
                    let jitter_factor = 1.0 - jitter + (rng.gen::<f64>() * jitter * 2.0);
                    (delay as f64 * jitter_factor) as u64
                } else {
                    delay
                }
            }
        };

        Some(Utc::now() + Duration::seconds(delay_seconds as i64))
    }

    /// Convert to a standard Duration
    pub fn to_duration(&self, retry_count: u32) -> Option<StdDuration> {
        let delay_seconds = match self {
            Self::NoRetry => return None,

            Self::Fixed {
                delay_seconds,
                max_retries,
            } => {
                if retry_count > *max_retries {
                    return None;
                }
                *delay_seconds
            }

            Self::ExponentialBackoff {
                initial_delay_seconds,
                max_delay_seconds,
                max_retries,
                jitter,
            } => {
                if retry_count > *max_retries {
                    return None;
                }

                // Calculate exponential delay
                let factor = 2u64.pow(retry_count - 1);
                let delay = initial_delay_seconds * factor;
                let delay = std::cmp::min(delay, *max_delay_seconds);

                // Add jitter
                if *jitter > 0.0 {
                    use rand::Rng;
                    let mut rng = rand::thread_rng();
                    let jitter_factor = 1.0 - jitter + (rng.gen::<f64>() * jitter * 2.0);
                    (delay as f64 * jitter_factor) as u64
                } else {
                    delay
                }
            }
        };

        Some(StdDuration::from_secs(delay_seconds))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_retry_returns_none() {
        assert!(RetryPolicy::NoRetry.next_retry_time(1).is_none());
        assert!(RetryPolicy::NoRetry.to_duration(1).is_none());
    }

    #[test]
    fn exponential_backoff_grows_and_caps() {
        let policy = RetryPolicy::ExponentialBackoff {
            initial_delay_seconds: 1,
            max_delay_seconds: 10,
            max_retries: 3,
            jitter: 0.0,
        };
        // retry_count 1 -> 1s, 2 -> 2s, 3 -> 4s (capped by max_retries=3);
        // 4 exceeds max_retries -> None.
        assert_eq!(policy.to_duration(1).unwrap(), StdDuration::from_secs(1));
        assert_eq!(policy.to_duration(2).unwrap(), StdDuration::from_secs(2));
        assert_eq!(policy.to_duration(3).unwrap(), StdDuration::from_secs(4));
        assert!(policy.to_duration(4).is_none());
    }

    #[test]
    fn exponential_capped_at_max_delay() {
        let policy = RetryPolicy::ExponentialBackoff {
            initial_delay_seconds: 8,
            max_delay_seconds: 10,
            max_retries: 5,
            jitter: 0.0,
        };
        // 8 * 2^0 = 8 (count 1), 8*2 = 16 -> capped at 10 (count 2).
        assert_eq!(policy.to_duration(1).unwrap(), StdDuration::from_secs(8));
        assert_eq!(policy.to_duration(2).unwrap(), StdDuration::from_secs(10));
    }

    #[test]
    fn next_retry_time_is_in_the_future() {
        let policy = RetryPolicy::exponential_backoff(3);
        let now = Utc::now();
        let next = policy.next_retry_time(1).unwrap();
        assert!(next > now);
    }

    #[test]
    fn fixed_policy_uses_constant_delay() {
        let policy = RetryPolicy::Fixed {
            delay_seconds: 7,
            max_retries: 2,
        };
        assert_eq!(policy.to_duration(1).unwrap(), StdDuration::from_secs(7));
        assert_eq!(policy.to_duration(2).unwrap(), StdDuration::from_secs(7));
        assert!(policy.to_duration(3).is_none());
    }
}
