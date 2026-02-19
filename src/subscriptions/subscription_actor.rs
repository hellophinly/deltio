use crate::collections::Messages;
use crate::push::PushSubscriptionsRegistry;
use crate::subscriptions::errors::*;
use crate::subscriptions::futures::{Deleted, MessagesAvailable};
use crate::subscriptions::outstanding::OutstandingMessageTracker;
use crate::subscriptions::retry_queue::RetryQueue;
use crate::subscriptions::subscription_manager::SubscriptionManagerDelegate;
use crate::subscriptions::{
    AckDeadline, AckId, AcknowledgeMessagesError, DeadlineModification, PulledMessage,
    SubscriptionInfo, SubscriptionStats,
};
use crate::topics::topic_manager::TopicManager;
use crate::topics::{MessageId, RemoveSubscriptionError, Topic, TopicMessage, TopicName};
use futures::future::Shared;
use futures::FutureExt;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::{Arc, Weak};
use tokio::sync::{mpsc, oneshot, Notify};
use tokio::time::Instant;

/// The max amount of messages that can be pulled.
const MAX_PULL_COUNT: u16 = 1_000;

/// Requests for the `SubscriptionActor`.
pub enum SubscriptionRequest {
    PostMessages {
        messages: Vec<Arc<TopicMessage>>,
    },
    GetInfo {
        responder: oneshot::Sender<Result<SubscriptionInfo, GetInfoError>>,
    },
    PullMessages {
        max_count: u16,
        responder: oneshot::Sender<Result<Vec<PulledMessage>, PullMessagesError>>,
    },
    AcknowledgeMessages {
        ack_ids: Vec<AckId>,
        responder: oneshot::Sender<Result<(), AcknowledgeMessagesError>>,
    },
    ModifyDeadline {
        deadline_modifications: Vec<DeadlineModification>,
        responder: oneshot::Sender<Result<(), ModifyDeadlineError>>,
    },
    Delete {
        responder: oneshot::Sender<Result<(), DeleteError>>,
    },
    GetStats {
        responder: oneshot::Sender<Result<SubscriptionStats, GetStatsError>>,
    },
}

/// Actor for the subscription.
pub(crate) struct SubscriptionActor {
    /// The subscription's internal ID.
    #[allow(dead_code)]
    internal_id: u32,

    /// The topic that the subscription is attached to.
    /// We use a weak reference because the topic may be deleted.
    topic: Weak<Topic>,

    /// Info about the subscription.
    info: SubscriptionInfo,

    /// A list of messages that are to be pulled.
    backlog: Messages,

    /// A map of messages have been pulled but not acked/nacked yet.
    outstanding: OutstandingMessageTracker,

    /// An observer to notify of various things such as new messages being available.
    observer: Arc<SubscriptionObserver>,

    /// When the subscription is configured for push, it reports it
    /// to the registry.
    push_registry: PushSubscriptionsRegistry,

    /// Used for communicating to the manager of changes to the subscription.
    delegate: SubscriptionManagerDelegate,

    /// The next ID to use as the ACK ID for a pulled message.
    next_ack_id: AckId,

    /// Whether the subscription has been marked as deleted.
    deleted: bool,

    /// Tracks delivery attempt count per message (keyed by message ID).
    delivery_attempts: HashMap<MessageId, u16>,

    /// Holds messages waiting for their retry backoff delay to elapse.
    retry_queue: RetryQueue,

    /// The topic manager, used for publishing to the dead letter topic.
    /// Only `Some` when a dead letter policy is configured.
    topic_manager: Option<Arc<TopicManager>>,
}

impl SubscriptionActor {
    /// Starts the actor.
    pub fn start(
        internal_id: u32,
        info: SubscriptionInfo,
        topic: Arc<Topic>,
        observer: Arc<SubscriptionObserver>,
        push_registry: PushSubscriptionsRegistry,
        delegate: SubscriptionManagerDelegate,
        topic_manager: Option<Arc<TopicManager>>,
    ) -> mpsc::Sender<SubscriptionRequest> {
        let (sender, mut receiver) = mpsc::channel(16);

        // If push is configured, register it with the push registry.
        if info.push_config.is_some() {
            push_registry.set(info.name.clone(), info.push_config.clone())
        }

        let mut actor = Self {
            internal_id,
            info,
            observer,
            delegate,
            push_registry,
            topic: Arc::downgrade(&topic),
            backlog: Messages::new(),
            outstanding: OutstandingMessageTracker::new(),
            next_ack_id: AckId::new(1),
            deleted: false,
            delivery_attempts: HashMap::new(),
            retry_queue: RetryQueue::new(),
            topic_manager,
        };

        tokio::spawn(async move {
            let deleted = actor.observer.deleted();
            let poll = async {
                loop {
                    tokio::select! {
                        Some(request) = receiver.recv() => {
                            actor.receive(request).await
                        },
                        Some(expired) = actor.outstanding.poll_next_expired() => {
                            actor.handle_expired_messages(expired).await;
                        },
                        Some(ready) = actor.retry_queue.poll_next_ready() => {
                            actor.handle_retry_ready(ready);
                        }
                    }
                }
            };

            tokio::select! {
                _ = deleted => (),
                _ = poll => (),
            }
        });

        sender
    }

    /// Receives a request.
    async fn receive(&mut self, request: SubscriptionRequest) {
        match request {
            SubscriptionRequest::PostMessages { messages } => {
                self.post_messages(messages);
            }
            SubscriptionRequest::GetInfo { responder } => {
                let result = self.get_info();
                let _ = responder.send(result);
            }
            SubscriptionRequest::PullMessages {
                max_count,
                responder,
            } => {
                let result = self.pull_messages(max_count);
                let _ = responder.send(result);
            }
            SubscriptionRequest::AcknowledgeMessages { ack_ids, responder } => {
                let result = self.acknowledge_messages(ack_ids);
                let _ = responder.send(result);
            }
            SubscriptionRequest::ModifyDeadline {
                deadline_modifications,
                responder,
            } => {
                let result = self.modify_deadline(deadline_modifications).await;
                let _ = responder.send(result);
            }
            SubscriptionRequest::Delete { responder } => {
                let result = self.delete().await;
                let _ = responder.send(result);
            }
            SubscriptionRequest::GetStats { responder } => {
                let result = self.get_stats();
                let _ = responder.send(result);
            }
        }
    }

    /// Gets info about the subscription.
    fn get_info(&mut self) -> Result<SubscriptionInfo, GetInfoError> {
        Ok(self.info.clone())
    }

    /// Posts new messages to the subscription.
    fn post_messages(&mut self, new_messages: Vec<Arc<TopicMessage>>) {
        if self.deleted {
            return;
        }

        self.backlog.append(new_messages);
        self.observer.notify_new_messages_available();
    }

    /// Pulls messages from the subscription, marking them as outstanding so they won't be
    /// delivered to anyone else.
    fn pull_messages(&mut self, max_count: u16) -> Result<Vec<PulledMessage>, PullMessagesError> {
        if self.deleted {
            return Ok(Default::default());
        }

        let outgoing_len = self.backlog.len() as u16;
        let capacity = max_count.clamp(0, outgoing_len.max(MAX_PULL_COUNT)) as usize;
        let mut result = Vec::with_capacity(capacity);

        let now = Instant::now();
        let deadline = now + self.info.ack_deadline;
        while let Some(message) = self.backlog.pop_front() {
            let ack_id = self.next_ack_id;
            self.next_ack_id = ack_id.next();

            let delivery_attempt = self
                .delivery_attempts
                .get(&message.id)
                .copied()
                .unwrap_or(1);
            let deadline = AckDeadline::new(&deadline);
            let pulled_message =
                PulledMessage::new(Arc::clone(&message), ack_id, deadline, delivery_attempt);
            result.push(pulled_message.clone());

            // Track the outstanding message so we can ACK it later (and also expire it).
            self.outstanding.add(pulled_message);

            if result.len() >= capacity {
                break;
            }
        }

        // If there are still messages left in the backlog, trigger another signal.
        if !self.backlog.is_empty() {
            self.observer.notify_new_messages_available();
        }

        Ok(result)
    }

    /// Acknowledges messages that have been pulled.
    fn acknowledge_messages(
        &mut self,
        ack_ids: Vec<AckId>,
    ) -> Result<(), AcknowledgeMessagesError> {
        if self.deleted {
            return Ok(());
        }

        let acked = self.outstanding.remove(ack_ids.into_iter());
        for message in &acked {
            self.delivery_attempts.remove(&message.message().id);
        }

        Ok(())
    }

    /// Modifies the deadline for messages that have been pulled.
    async fn modify_deadline(
        &mut self,
        deadline_modifications: Vec<DeadlineModification>,
    ) -> Result<(), ModifyDeadlineError> {
        if self.deleted {
            return Ok(());
        }

        let nacks = self.outstanding.modify(deadline_modifications);
        self.requeue_messages(nacks).await;

        Ok(())
    }

    /// Marks the subscription as deleted. Further requests will be no-ops.
    async fn delete(&mut self) -> Result<(), DeleteError> {
        if self.deleted {
            return Ok(());
        }

        self.deleted = true;

        // If the topic is still around, remove ourselves from it's list of subscriptions.
        if let Some(topic) = self.topic.upgrade() {
            topic
                .remove_subscription(self.info.name.clone())
                .await
                .map_err(|e| match e {
                    RemoveSubscriptionError::Closed => DeleteError::Closed,
                })?;
        }

        self.delegate.delete(&self.info.name);
        self.observer.notify_deleted();
        self.outstanding.clear();
        self.backlog.clear();
        self.retry_queue.clear();
        self.delivery_attempts.clear();

        // Unregister the subscription from push.
        self.push_registry.set(self.info.name.clone(), None);

        Ok(())
    }

    /// Gets the stats for the subscription.
    fn get_stats(&mut self) -> Result<SubscriptionStats, GetStatsError> {
        let stats = SubscriptionStats::new(
            self.info.name.clone(),
            self.topic
                .upgrade()
                .map(|t| t.name.clone())
                .unwrap_or_else(TopicName::deleted),
            self.outstanding.len(),
            self.backlog.len() + self.retry_queue.len(),
        );
        Ok(stats)
    }

    /// Handles expired messages by requeueing them (with retry backoff if configured).
    async fn handle_expired_messages(&mut self, expired: Vec<PulledMessage>) {
        log::debug!("{}: {} messages expired", &self.info.name, expired.len());
        self.requeue_messages(expired).await;
    }

    /// Requeues messages after a nack or deadline expiry, applying retry backoff if configured.
    /// If a dead letter policy is configured and the max delivery attempts have been exceeded,
    /// the message is forwarded to the dead letter topic instead.
    async fn requeue_messages(&mut self, messages: Vec<PulledMessage>) {
        let now = Instant::now();
        let mut dead_letter_messages: Vec<TopicMessage> = Vec::new();
        let mut dead_letter_message_ids: Vec<MessageId> = Vec::new();

        for pulled in messages {
            let message = pulled.into_message();
            let message_id = message.id;

            // Increment delivery attempt.
            let attempt = self
                .delivery_attempts
                .entry(message_id)
                .and_modify(|a| *a = a.saturating_add(1))
                .or_insert(2);

            // Check if we should dead-letter this message.
            if let Some(ref dlp) = self.info.dead_letter_policy {
                if *attempt as i32 >= dlp.max_delivery_attempts {
                    // Create a new TopicMessage for the DLQ topic from the original.
                    let dlq_message =
                        TopicMessage::new(message.data.clone(), message.attributes.clone());
                    dead_letter_messages.push(dlq_message);
                    dead_letter_message_ids.push(message_id);
                    continue;
                }
            }

            if let Some(ref retry_policy) = self.info.retry_policy {
                let backoff = retry_policy.calculate_backoff(*attempt);
                let deliver_at = AckDeadline::new(&(now + backoff));
                self.retry_queue.add(message, deliver_at);
            } else {
                self.backlog.append(std::iter::once(message));
            }
        }

        // Publish dead-lettered messages to the DLQ topic.
        if !dead_letter_messages.is_empty() {
            if let Some(ref topic_manager) = self.topic_manager {
                let dlp = self.info.dead_letter_policy.as_ref().unwrap();
                match topic_manager.get_topic(&dlp.dead_letter_topic) {
                    Ok(dlq_topic) => {
                        let count = dead_letter_messages.len();
                        if let Err(e) = dlq_topic.publish_messages(dead_letter_messages).await {
                            log::warn!(
                                "{}: failed to publish {} messages to dead letter topic {}: {}",
                                &self.info.name,
                                count,
                                &dlp.dead_letter_topic,
                                e
                            );
                        } else {
                            log::debug!(
                                "{}: dead-lettered {} messages to {}",
                                &self.info.name,
                                count,
                                &dlp.dead_letter_topic
                            );
                        }
                    }
                    Err(_) => {
                        log::warn!(
                            "{}: dead letter topic {} no longer exists, dropping {} messages",
                            &self.info.name,
                            &dlp.dead_letter_topic,
                            dead_letter_messages.len()
                        );
                    }
                }
            }

            // Clean up delivery attempts for dead-lettered messages.
            for id in &dead_letter_message_ids {
                self.delivery_attempts.remove(id);
            }
        }

        if !self.backlog.is_empty() {
            self.observer.notify_new_messages_available();
        }
    }

    /// Handles messages whose retry backoff has elapsed by moving them to the backlog.
    fn handle_retry_ready(&mut self, ready: Vec<Arc<TopicMessage>>) {
        log::debug!(
            "{}: {} retry messages ready",
            &self.info.name,
            ready.len()
        );
        self.backlog.append(ready);
        if !self.backlog.is_empty() {
            self.observer.notify_new_messages_available();
        }
    }
}

/// Observer for propagating signals to the `Subscription`.
pub(crate) struct SubscriptionObserver {
    /// Notifies when there are new messages to pull.
    notify_messages_available: Notify,

    /// Notifies when the subscription gets deleted.
    /// Used by consumers to cancel any in-progress long-running operations.
    deleted_recv: Shared<oneshot::Receiver<()>>,

    // See above.
    // This shouldn't impact performance since it's only used for deletion,
    // which happens at most once per subscription.
    deleted_send: Mutex<Option<oneshot::Sender<()>>>,
}

impl SubscriptionObserver {
    /// Creates a new `SubscriptionObserver`.
    pub fn new() -> Self {
        let (deleted_send, deleted_recv) = oneshot::channel();
        Self {
            deleted_send: Mutex::new(Some(deleted_send)),
            deleted_recv: deleted_recv.shared(),
            notify_messages_available: Notify::new(),
        }
    }

    /// Notifies of new messages being available.
    pub fn notify_new_messages_available(&self) {
        self.notify_messages_available.notify_one();
    }

    /// Notifies that the subscription was deleted.
    pub fn notify_deleted(&self) {
        // The oneshot sender is consumed when sending, so we need
        // to put it in an `Option` backed by a mutex.
        // First, acquire the lock and attempt to take out the value.
        // This will leave `None` in it's place, so if this method were to run
        // again, it would no-op.
        let taken = {
            let mut unlocked = self.deleted_send.lock();
            unlocked.take()
        };

        // If we were able to take out the sender, send the notification.
        if let Some(sender) = taken {
            let _ = sender.send(());
            // Also notify everyone waiting for messages.
            self.notify_messages_available.notify_waiters();
        }
    }

    /// Returns a signal for new messages.
    /// When new messages arrive, any waiters of the signal will be
    /// notified. The signal will be subscribed to immediately, so the time at which
    /// this method is called is important.
    pub fn new_messages_available(&self) -> MessagesAvailable {
        MessagesAvailable::new(self.notify_messages_available.notified())
    }

    /// Returns a signal for when the subscription is deleted.
    pub fn deleted(&self) -> Deleted {
        Deleted::new(Shared::clone(&self.deleted_recv))
    }
}
