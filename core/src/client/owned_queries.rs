// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Queries submitted by this connection that may still be running on the server.
//!
//! `kill_query` uses this to choose between the session-bound HTTP `/kill`
//! endpoint (queries started here) and SQL `KILL QUERY` (any other query).

use std::collections::HashMap;

/// Queries whose results were dropped or failed before reading to the end are
/// never confirmed finished; bound them so a long-lived connection cannot grow.
/// Queries still being submitted or read are never evicted: each of them is
/// backed by a live request or result handle.
pub(crate) const MAX_OWNED_QUERIES: usize = 256;

#[derive(Debug)]
struct OwnedQuery {
    seq: u64,
    node_id: Option<String>,
    /// The submission future is alive and pins this entry against eviction.
    submitting: bool,
    /// No usable first response yet, even if the submission future is gone.
    awaiting_first_response: bool,
    /// A result handle (`Pages`) for the query is alive.
    reading: bool,
    /// The client-generated ID, when the server or a gateway assigned another.
    submitted_id: Option<String>,
}

/// Where `kill_query` should send `/kill` for an owned query, as of now.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct KillTarget {
    /// The ID the server knows the query by.
    pub query_id: String,
    pub node_id: Option<String>,
    /// The server may not have registered the query yet.
    pub retry_not_found: bool,
}

/// What a decoded first response proved about the query. Without one (I/O,
/// HTTP or decode error) `first_response` is not called and the query stays
/// owned under the submitted ID, since it may still be running.
#[derive(Debug)]
pub(crate) enum FirstOutcome<'a> {
    /// The server reported the query with more pages to fetch. A gateway may
    /// have replaced the ID, so this carries the server-side ID and node.
    Running {
        query_id: &'a str,
        node_id: Option<String>,
    },
    /// The server reported the query as finished or failed.
    Finished,
}

#[derive(Default, Debug)]
pub(crate) struct OwnedQueries {
    queries: HashMap<String, OwnedQuery>,
    next_seq: u64,
}

impl OwnedQueries {
    fn insert(&mut self, query_id: &str, query: OwnedQuery) {
        self.queries.insert(query_id.to_string(), query);
        self.trim();
    }

    fn new_entry(&mut self, node_id: Option<String>, submitting: bool) -> OwnedQuery {
        self.next_seq += 1;
        OwnedQuery {
            seq: self.next_seq,
            node_id,
            submitting,
            awaiting_first_response: submitting,
            reading: false,
            submitted_id: None,
        }
    }

    /// Evict the oldest abandoned entries (neither being submitted nor read)
    /// until the set is within its bound. The set may exceed the bound only
    /// while more than `MAX_OWNED_QUERIES` queries are submitted or read at
    /// once, each holding its own request or result handle.
    fn trim(&mut self) {
        while self.queries.len() > MAX_OWNED_QUERIES {
            let Some(oldest) = self
                .queries
                .iter()
                .filter(|(_, q)| !q.submitting && !q.reading)
                .min_by_key(|(_, q)| q.seq)
                .map(|(id, _)| id.clone())
            else {
                return;
            };
            self.queries.remove(&oldest);
        }
    }

    /// A start request is about to be sent with this client-generated ID.
    /// Called before the ID becomes visible through `last_query_id`.
    pub(crate) fn submission_started(&mut self, query_id: &str) {
        let entry = self.new_entry(None, true);
        self.insert(query_id, entry);
    }

    /// The submission guard is gone. Keep the entry only if a request may have
    /// reached the server: the query can then be running without a response.
    pub(crate) fn submission_ended(&mut self, query_id: &str, may_have_reached_server: bool) {
        if !may_have_reached_server {
            // Never sent: there is no server query to kill.
            self.queries.remove(query_id);
        } else if let Some(query) = self.queries.get_mut(query_id) {
            // Allow eviction, but keep retrying an early /kill 404 if we never
            // received a usable first response.
            query.submitting = false;
            // A burst of live submissions may have grown the set past its bound.
            self.trim();
        }
    }

    /// Record the first response of `submitted_id`. Call this before the
    /// server-side ID is published through `last_query_id`.
    pub(crate) fn first_response(&mut self, submitted_id: &str, outcome: FirstOutcome<'_>) {
        match outcome {
            FirstOutcome::Running { query_id, node_id } => {
                self.queries.remove(submitted_id);
                let mut entry = self.new_entry(node_id, false);
                // Every start path wraps a running first response in `Pages`,
                // which calls `result_released` when dropped.
                entry.reading = true;
                // A kill that started with the client ID must still find it.
                entry.submitted_id = (query_id != submitted_id).then(|| submitted_id.into());
                self.insert(query_id, entry);
            }
            FirstOutcome::Finished => {
                self.queries.remove(submitted_id);
            }
        }
    }

    pub(crate) fn finished(&mut self, query_id: &str) {
        self.queries.remove(query_id);
    }

    /// The result handle is gone before the query was confirmed finished. The
    /// query stays owned, since it may still run, but becomes evictable.
    pub(crate) fn result_released(&mut self, query_id: &str) {
        if let Some(query) = self.queries.get_mut(query_id) {
            query.reading = false;
            self.trim();
        }
    }

    /// The current `/kill` target for `query_id`, which may be the server ID
    /// of a query submitted under this client ID. `None` if not owned.
    pub(crate) fn kill_target(&self, query_id: &str) -> Option<KillTarget> {
        let target = |id: &str, query: &OwnedQuery| KillTarget {
            query_id: id.to_string(),
            node_id: query.node_id.clone(),
            retry_not_found: query.awaiting_first_response,
        };
        if let Some(query) = self.queries.get(query_id) {
            return Some(target(query_id, query));
        }
        self.queries
            .iter()
            .find(|(_, q)| q.submitted_id.as_deref() == Some(query_id))
            .map(|(id, q)| target(id, q))
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.queries.len()
    }

    #[cfg(test)]
    pub(crate) fn is_reading(&self, query_id: &str) -> Option<bool> {
        self.queries.get(query_id).map(|q| q.reading)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn running<'a>(query_id: &'a str, node_id: Option<&str>) -> FirstOutcome<'a> {
        FirstOutcome::Running {
            query_id,
            node_id: node_id.map(Into::into),
        }
    }

    fn target(query_id: &str, node_id: Option<&str>, retry_not_found: bool) -> Option<KillTarget> {
        Some(KillTarget {
            query_id: query_id.into(),
            node_id: node_id.map(Into::into),
            retry_not_found,
        })
    }

    #[test]
    fn unsent_submission_is_forgotten_and_sent_one_is_kept() {
        let mut owned = OwnedQueries::default();
        owned.submission_started("unsent");
        owned.submission_started("sent");
        assert_eq!(owned.kill_target("sent"), target("sent", None, true));
        owned.submission_ended("unsent", false);
        owned.submission_ended("sent", true);
        assert_eq!(owned.kill_target("unsent"), None);
        assert_eq!(owned.kill_target("sent"), target("sent", None, true));
        assert!(!owned.queries["sent"].submitting);
    }

    #[test]
    fn rewritten_id_is_reachable_through_the_client_id() {
        let mut owned = OwnedQueries::default();
        owned.submission_started("client-id");
        owned.first_response("client-id", running("server-id", Some("n1")));
        owned.submission_ended("client-id", true);
        // Both the client ID and the server ID reach the server-side query.
        assert_eq!(
            owned.kill_target("client-id"),
            target("server-id", Some("n1"), false)
        );
        assert_eq!(
            owned.kill_target("server-id"),
            target("server-id", Some("n1"), false)
        );
        owned.finished("server-id");
        assert_eq!(owned.kill_target("client-id"), None);
        assert_eq!(owned.len(), 0);
    }

    #[test]
    fn finished_first_response_is_not_tracked() {
        let mut owned = OwnedQueries::default();
        owned.submission_started("q");
        owned.first_response("q", FirstOutcome::Finished);
        owned.submission_ended("q", true);
        assert_eq!(owned.len(), 0);

        owned.submission_started("r");
        owned.first_response("r", running("r", None));
        owned.finished("r");
        assert_eq!(owned.len(), 0);
    }

    #[test]
    fn unusable_first_response_keeps_the_query_owned() {
        let mut owned = OwnedQueries::default();
        // No decoded first response, e.g. a body or decode error.
        owned.submission_started("q");
        owned.submission_ended("q", true);
        assert_eq!(owned.kill_target("q"), target("q", None, true));
    }

    #[test]
    fn eviction_keeps_live_submissions() {
        let mut owned = OwnedQueries::default();
        owned.submission_started("live");
        for i in 0..MAX_OWNED_QUERIES + 10 {
            let id = format!("dropped-{i}");
            owned.submission_started(&id);
            owned.submission_ended(&id, true);
        }
        assert_eq!(owned.len(), MAX_OWNED_QUERIES);
        assert_eq!(owned.kill_target("live"), target("live", None, true));
        assert_eq!(owned.kill_target("dropped-0"), None);
        assert!(owned
            .kill_target(&format!("dropped-{}", MAX_OWNED_QUERIES + 9))
            .is_some());
    }

    #[test]
    fn queries_being_read_are_never_evicted() {
        let mut owned = OwnedQueries::default();
        let ids: Vec<_> = (0..MAX_OWNED_QUERIES + 10)
            .map(|i| format!("q{i}"))
            .collect();
        // Every query got a running first response and its result is open.
        for id in &ids {
            owned.submission_started(id);
            owned.first_response(id, running(id, Some("n1")));
            owned.submission_ended(id, true);
        }
        assert_eq!(owned.len(), MAX_OWNED_QUERIES + 10);
        assert_eq!(
            owned.kill_target(&ids[0]),
            target(&ids[0], Some("n1"), false)
        );

        // Abandoned queries are evictable even while others are being read.
        for i in 0..5 {
            let id = format!("abandoned-{i}");
            owned.submission_started(&id);
            owned.submission_ended(&id, true);
        }
        assert_eq!(owned.len(), MAX_OWNED_QUERIES + 10);
        assert!(owned.kill_target(&ids[0]).is_some());

        // Releasing the results makes them evictable, oldest first.
        for id in &ids {
            owned.result_released(id);
        }
        assert_eq!(owned.len(), MAX_OWNED_QUERIES);
        assert_eq!(owned.kill_target(&ids[0]), None);
        assert!(owned.kill_target(&ids[ids.len() - 1]).is_some());
    }

    #[test]
    fn bound_is_restored_after_a_burst_of_live_submissions_ends() {
        let mut owned = OwnedQueries::default();
        let ids: Vec<_> = (0..MAX_OWNED_QUERIES + 50)
            .map(|i| format!("q{i}"))
            .collect();
        for id in &ids {
            owned.submission_started(id);
        }
        // Nothing can be evicted while every submission is live.
        assert_eq!(owned.len(), MAX_OWNED_QUERIES + 50);
        for id in &ids {
            owned.submission_ended(id, true);
        }
        assert_eq!(owned.len(), MAX_OWNED_QUERIES);
        // The newest entries survive.
        assert!(owned.kill_target(&ids[ids.len() - 1]).is_some());
        assert_eq!(owned.kill_target(&ids[0]), None);
    }
}
