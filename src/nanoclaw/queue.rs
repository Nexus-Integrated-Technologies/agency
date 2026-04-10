use std::collections::{HashMap, VecDeque};

use crate::foundation::{QueueOutcome, QueueSnapshot, WorkQueue};

#[derive(Debug, Default)]
struct GroupState {
    active: bool,
    pending_messages: bool,
    running_task_id: Option<String>,
    pending_tasks: VecDeque<String>,
}

pub struct GroupQueue {
    max_active_groups: usize,
    active_groups: usize,
    groups: HashMap<String, GroupState>,
    waiting_groups: VecDeque<String>,
}

impl GroupQueue {
    pub fn new(max_active_groups: usize) -> Self {
        Self {
            max_active_groups: max_active_groups.max(1),
            active_groups: 0,
            groups: HashMap::new(),
            waiting_groups: VecDeque::new(),
        }
    }

    pub fn enqueue_message_check(&mut self, group_jid: &str) -> QueueOutcome {
        {
            let state = self.group_mut(group_jid);
            if state.active {
                state.pending_messages = true;
                return QueueOutcome::Queued;
            }
        }

        if self.active_groups >= self.max_active_groups {
            {
                let state = self.group_mut(group_jid);
                state.pending_messages = true;
            }
            self.push_waiting(group_jid);
            return QueueOutcome::Queued;
        }

        {
            let state = self.group_mut(group_jid);
            state.active = true;
            state.pending_messages = false;
        }
        self.active_groups += 1;
        QueueOutcome::Started
    }

    pub fn enqueue_task(&mut self, group_jid: &str, task_id: &str) -> QueueOutcome {
        {
            let state = self.group_mut(group_jid);
            if state.running_task_id.as_deref() == Some(task_id)
                || state.pending_tasks.iter().any(|queued| queued == task_id)
            {
                return QueueOutcome::SkippedDuplicate;
            }

            if state.active {
                state.pending_tasks.push_back(task_id.to_string());
                return QueueOutcome::Queued;
            }
        }

        if self.active_groups >= self.max_active_groups {
            {
                let state = self.group_mut(group_jid);
                state.pending_tasks.push_back(task_id.to_string());
            }
            self.push_waiting(group_jid);
            return QueueOutcome::Queued;
        }

        {
            let state = self.group_mut(group_jid);
            state.active = true;
            state.running_task_id = Some(task_id.to_string());
        }
        self.active_groups += 1;
        QueueOutcome::Started
    }

    pub fn finish_group(&mut self, group_jid: &str) {
        if let Some(state) = self.groups.get_mut(group_jid) {
            if state.active {
                state.active = false;
                state.running_task_id = None;
                self.active_groups = self.active_groups.saturating_sub(1);
            }
        }
        self.drain_waiting();
    }

    pub fn snapshots(&self) -> Vec<QueueSnapshot> {
        let mut groups = self
            .groups
            .iter()
            .map(|(jid, state)| QueueSnapshot {
                jid: jid.clone(),
                active: state.active,
                pending_messages: state.pending_messages,
                running_task_id: state.running_task_id.clone(),
                pending_tasks: state.pending_tasks.iter().cloned().collect(),
            })
            .collect::<Vec<_>>();
        groups.sort_by(|left, right| left.jid.cmp(&right.jid));
        groups
    }

    fn group_mut(&mut self, group_jid: &str) -> &mut GroupState {
        self.groups.entry(group_jid.to_string()).or_default()
    }

    fn push_waiting(&mut self, group_jid: &str) {
        if !self.waiting_groups.iter().any(|queued| queued == group_jid) {
            self.waiting_groups.push_back(group_jid.to_string());
        }
    }

    fn drain_waiting(&mut self) {
        while self.active_groups < self.max_active_groups {
            let Some(next_group) = self.waiting_groups.pop_front() else {
                break;
            };
            let Some(state) = self.groups.get_mut(&next_group) else {
                continue;
            };

            if state.active {
                continue;
            }

            state.active = true;
            if let Some(task_id) = state.pending_tasks.pop_front() {
                state.running_task_id = Some(task_id);
            } else {
                state.pending_messages = false;
            }
            self.active_groups += 1;
        }
    }
}

impl WorkQueue for GroupQueue {
    fn enqueue_group_messages(&mut self, group_id: &str) -> QueueOutcome {
        self.enqueue_message_check(group_id)
    }

    fn enqueue_group_task(&mut self, group_id: &str, task_id: &str) -> QueueOutcome {
        self.enqueue_task(group_id, task_id)
    }

    fn finish_group(&mut self, group_id: &str) {
        GroupQueue::finish_group(self, group_id);
    }

    fn snapshots(&self) -> Vec<QueueSnapshot> {
        GroupQueue::snapshots(self)
    }
}

#[cfg(test)]
mod tests {
    use super::GroupQueue;
    use crate::foundation::QueueOutcome;

    #[test]
    fn queues_messages_when_at_capacity() {
        let mut queue = GroupQueue::new(1);

        assert_eq!(queue.enqueue_message_check("alpha"), QueueOutcome::Started);
        assert_eq!(queue.enqueue_message_check("beta"), QueueOutcome::Queued);

        let snapshots = queue.snapshots();
        assert_eq!(snapshots[1].jid, "beta");
        assert!(snapshots[1].pending_messages);
    }

    #[test]
    fn deduplicates_task_ids() {
        let mut queue = GroupQueue::new(1);

        assert_eq!(queue.enqueue_task("alpha", "task-1"), QueueOutcome::Started);
        assert_eq!(
            queue.enqueue_task("alpha", "task-1"),
            QueueOutcome::SkippedDuplicate
        );
    }
}
