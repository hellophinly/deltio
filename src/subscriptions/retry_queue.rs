use crate::subscriptions::AckDeadline;
use crate::topics::TopicMessage;
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use tokio::sync::Notify;
use tokio::time::Instant;

/// A queue that holds messages waiting for their retry backoff delay to elapse.
pub(crate) struct RetryQueue {
    /// Scheduled deliveries ordered by delivery time, with a monotonic sequence
    /// number for tie-breaking.
    schedule: BTreeSet<(AckDeadline, u64)>,

    /// Maps sequence numbers to their messages.
    messages: HashMap<u64, Arc<TopicMessage>>,

    /// Monotonically increasing sequence number for insertion ordering.
    next_seq: u64,

    /// Notify when a new, earlier delivery time has been inserted.
    notify: Notify,
}

impl RetryQueue {
    /// Creates a new empty `RetryQueue`.
    pub fn new() -> Self {
        Self {
            schedule: BTreeSet::new(),
            messages: HashMap::new(),
            next_seq: 0,
            notify: Notify::new(),
        }
    }

    /// Adds a message to the retry queue, scheduled for delivery at `deliver_at`.
    pub fn add(&mut self, message: Arc<TopicMessage>, deliver_at: AckDeadline) {
        let seq = self.next_seq;
        self.next_seq += 1;

        let key = (deliver_at, seq);

        // Check if this new entry is earlier than the current first.
        let should_notify = self
            .schedule
            .first()
            .map(|first| &key < first)
            .unwrap_or(true);

        self.schedule.insert(key);
        self.messages.insert(seq, message);

        if should_notify {
            self.notify.notify_waiters();
        }
    }

    /// Takes all messages whose delivery time has been reached.
    pub fn take_ready(&mut self, now: &Instant) -> Vec<Arc<TopicMessage>> {
        let mut result = Vec::new();
        while let Some(&(deadline, seq)) = self.schedule.first() {
            if now < &deadline.time() {
                break;
            }

            self.schedule.pop_first();
            if let Some(message) = self.messages.remove(&seq) {
                result.push(message);
            }
        }
        result
    }

    /// Polls for the next batch of ready messages, sleeping until the earliest
    /// delivery time or until notified of a new earlier entry.
    pub async fn poll_next_ready(&mut self) -> Option<Vec<Arc<TopicMessage>>> {
        loop {
            let now = Instant::now();
            let ready = self.take_ready(&now);
            if !ready.is_empty() {
                return Some(ready);
            }

            let notified = self.notify.notified();
            if let Some(&(deadline, _)) = self.schedule.first() {
                let when = deadline.time();
                tokio::select! {
                    _ = notified => {},
                    _ = tokio::time::sleep_until(when) => {}
                }
            } else {
                notified.await;
            }
        }
    }

    /// Returns the number of messages in the retry queue.
    pub fn len(&self) -> usize {
        self.messages.len()
    }

    /// Clears all messages from the retry queue.
    pub fn clear(&mut self) {
        self.schedule.clear();
        self.messages.clear();
        self.notify.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use lazy_static::lazy_static;
    use std::time::Duration;

    lazy_static! {
        static ref EPOCH: Instant = Instant::now();
    }

    fn deadline_at(secs: u64) -> AckDeadline {
        AckDeadline::new(&EPOCH.checked_add(Duration::from_secs(secs)).unwrap())
    }

    fn new_message(data: u8) -> Arc<TopicMessage> {
        Arc::new(TopicMessage::new(Bytes::from(vec![data]), None))
    }

    #[test]
    fn take_ready_returns_messages_in_order() {
        let mut queue = RetryQueue::new();
        queue.add(new_message(3), deadline_at(3));
        queue.add(new_message(1), deadline_at(1));
        queue.add(new_message(2), deadline_at(2));

        assert_eq!(queue.len(), 3);

        // Take messages ready at time 2.
        let ready = queue.take_ready(&deadline_at(2).time());
        assert_eq!(ready.len(), 2);
        assert_eq!(ready[0].data[0], 1);
        assert_eq!(ready[1].data[0], 2);
        assert_eq!(queue.len(), 1);

        // Take remaining at time 3.
        let ready = queue.take_ready(&deadline_at(3).time());
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].data[0], 3);
        assert_eq!(queue.len(), 0);
    }

    #[test]
    fn take_ready_returns_empty_when_none_ready() {
        let mut queue = RetryQueue::new();
        queue.add(new_message(1), deadline_at(5));

        let ready = queue.take_ready(&deadline_at(1).time());
        assert!(ready.is_empty());
        assert_eq!(queue.len(), 1);
    }

    #[test]
    fn clear_removes_all() {
        let mut queue = RetryQueue::new();
        queue.add(new_message(1), deadline_at(1));
        queue.add(new_message(2), deadline_at(2));
        assert_eq!(queue.len(), 2);

        queue.clear();
        assert_eq!(queue.len(), 0);

        let ready = queue.take_ready(&deadline_at(10).time());
        assert!(ready.is_empty());
    }
}
