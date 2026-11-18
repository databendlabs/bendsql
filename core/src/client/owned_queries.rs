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
const MAX_OWNED_QUERIES: usize = 256;

#[derive(Debug)]
struct OwnedQuery {
    seq: u64,
    node_id: Option<String>,
    /// The start request is still awaiting its first response.
    submitting: bool,
    /// `kill_query` arrived while submitting; kill once the response lands.
    cancel_requested: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct KillTarget {
    pub node_id: Option<String>,
    pub submitting: bool,
}

/// What the first response of a submission proved about the query.
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
    /// No usable response (I/O, HTTP or decode error): the query may still be
    /// running under the submitted ID.
    Unknown,
}

/// A kill requested before the first response, to be sent now.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct KillReplay {
    pub query_id: String,
    pub node_id: Option<String>,
}

#[derive(Default, Debug)]
pub(crate) struct OwnedQueries {
    queries: HashMap<String, OwnedQuery>,
    next_seq: u64,
}

impl OwnedQueries {
    fn insert(&mut self, query_id: &str, node_id: Option<String>, submitting: bool) {
        self.next_seq += 1;
        self.queries.insert(
            query_id.to_string(),
            OwnedQuery {
                seq: self.next_seq,
                node_id,
                submitting,
                cancel_requested: false,
            },
        );
        self.trim();
    }

    /// Evict the oldest entries whose submission has ended until the set is
    /// within its bound. Live submissions are never evicted, so a deferred
    /// cancel is not lost; the set may exceed the bound only while more than
    /// `MAX_OWNED_QUERIES` submissions are live at once.
    fn trim(&mut self) {
        while self.queries.len() > MAX_OWNED_QUERIES {
            let Some(oldest) = self
                .queries
                .iter()
                .filter(|(_, q)| !q.submitting)
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
        self.insert(query_id, None, true);
    }

    /// The submission guard is gone. Keep the entry only if a request may have
    /// reached the server: the query can then be running without a response.
    ///
    /// Returns a kill that `kill_query` queued but no first-response handler
    /// consumed, e.g. because the submitting future was dropped; the caller
    /// must send it, since `kill_query` already reported success.
    pub(crate) fn submission_ended(
        &mut self,
        query_id: &str,
        may_have_reached_server: bool,
    ) -> Option<KillReplay> {
        if !may_have_reached_server {
            // Never sent: there is no server query to kill.
            self.queries.remove(query_id);
            return None;
        }
        let query = self.queries.get_mut(query_id)?;
        query.submitting = false;
        let replay = std::mem::take(&mut query.cancel_requested).then(|| KillReplay {
            query_id: query_id.to_string(),
            node_id: query.node_id.clone(),
        });
        // A burst of live submissions may have grown the set past its bound.
        self.trim();
        replay
    }

    /// Record the first response of `submitted_id`. Call this before the
    /// server-side ID is published through `last_query_id`. Returns the kill to
    /// send if `kill_query` ran before this response and the query may still
    /// be running.
    pub(crate) fn first_response(
        &mut self,
        submitted_id: &str,
        outcome: FirstOutcome<'_>,
    ) -> Option<KillReplay> {
        match outcome {
            FirstOutcome::Running { query_id, node_id } => {
                let cancel_requested = self
                    .queries
                    .remove(submitted_id)
                    .is_some_and(|q| q.cancel_requested);
                self.insert(query_id, node_id.clone(), false);
                cancel_requested.then(|| KillReplay {
                    query_id: query_id.to_string(),
                    node_id,
                })
            }
            FirstOutcome::Finished => {
                self.queries.remove(submitted_id);
                None
            }
            FirstOutcome::Unknown => {
                // Only a confirmed terminal response may forget the query.
                let query = self.queries.get_mut(submitted_id)?;
                std::mem::take(&mut query.cancel_requested).then(|| KillReplay {
                    query_id: submitted_id.to_string(),
                    node_id: query.node_id.clone(),
                })
            }
        }
    }

    pub(crate) fn finished(&mut self, query_id: &str) {
        self.queries.remove(query_id);
    }

    /// Look up a query for `kill_query`. A kill that races the first response
    /// is also remembered, so the response handler can repeat it once the
    /// server is guaranteed to know the query.
    pub(crate) fn request_kill(&mut self, query_id: &str) -> Option<KillTarget> {
        let query = self.queries.get_mut(query_id)?;
        if query.submitting {
            query.cancel_requested = true;
        }
        Some(KillTarget {
            node_id: query.node_id.clone(),
            submitting: query.submitting,
        })
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.queries.len()
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

    #[test]
    fn unsent_submission_is_forgotten_and_sent_one_is_kept() {
        let mut owned = OwnedQueries::default();
        owned.submission_started("unsent");
        owned.submission_started("sent");
        owned.submission_ended("unsent", false);
        owned.submission_ended("sent", true);
        assert_eq!(owned.request_kill("unsent"), None);
        assert_eq!(
            owned.request_kill("sent"),
            Some(KillTarget {
                node_id: None,
                submitting: false
            })
        );
    }

    #[test]
    fn kill_before_first_response_is_replayed_once_running() {
        let mut owned = OwnedQueries::default();
        owned.submission_started("client-id");
        assert_eq!(
            owned.request_kill("client-id"),
            Some(KillTarget {
                node_id: None,
                submitting: true
            })
        );
        // The gateway rewrote the ID; the replay targets the server ID and node.
        assert_eq!(
            owned.first_response("client-id", running("server-id", Some("n1"))),
            Some(KillReplay {
                query_id: "server-id".into(),
                node_id: Some("n1".into())
            })
        );
        owned.submission_ended("client-id", true);
        assert_eq!(owned.request_kill("client-id"), None);
        assert_eq!(
            owned.request_kill("server-id"),
            Some(KillTarget {
                node_id: Some("n1".into()),
                submitting: false
            })
        );
        // A later kill does not request another replay.
        assert_eq!(
            owned.first_response("server-id", running("server-id", None)),
            None
        );
    }

    #[test]
    fn finished_first_response_is_not_tracked() {
        let mut owned = OwnedQueries::default();
        owned.submission_started("q");
        owned.request_kill("q");
        assert_eq!(owned.first_response("q", FirstOutcome::Finished), None);
        owned.submission_ended("q", true);
        assert_eq!(owned.len(), 0);

        owned.submission_started("r");
        assert_eq!(owned.first_response("r", running("r", None)), None);
        owned.finished("r");
        assert_eq!(owned.len(), 0);
    }

    #[test]
    fn unusable_first_response_keeps_the_query_owned() {
        let mut owned = OwnedQueries::default();
        owned.submission_started("q");
        assert_eq!(owned.first_response("q", FirstOutcome::Unknown), None);
        owned.submission_ended("q", true);
        assert!(owned.request_kill("q").is_some_and(|t| !t.submitting));

        // A kill requested before the unusable response is replayed once.
        owned.submission_started("r");
        owned.request_kill("r");
        assert_eq!(
            owned.first_response("r", FirstOutcome::Unknown),
            Some(KillReplay {
                query_id: "r".into(),
                node_id: None
            })
        );
        assert_eq!(owned.first_response("r", FirstOutcome::Unknown), None);
    }

    #[test]
    fn queued_kill_is_returned_when_the_submission_ends_unconsumed() {
        let mut owned = OwnedQueries::default();
        owned.submission_started("q");
        owned.request_kill("q");
        assert_eq!(
            owned.submission_ended("q", true),
            Some(KillReplay {
                query_id: "q".into(),
                node_id: None
            })
        );
        // Consumed: a later kill on the ended submission is not queued again.
        owned.request_kill("q");
        assert_eq!(owned.submission_ended("q", true), None);

        // An unsent submission has nothing to kill.
        owned.submission_started("unsent");
        owned.request_kill("unsent");
        assert_eq!(owned.submission_ended("unsent", false), None);

        // A replay consumed by the first response is not returned again.
        owned.submission_started("r");
        owned.request_kill("r");
        assert!(owned.first_response("r", FirstOutcome::Unknown).is_some());
        assert_eq!(owned.submission_ended("r", true), None);
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
        assert!(owned.request_kill("live").is_some_and(|t| t.submitting));
        assert_eq!(owned.request_kill("dropped-0"), None);
        assert!(owned
            .request_kill(&format!("dropped-{}", MAX_OWNED_QUERIES + 9))
            .is_some());
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
        assert!(owned.request_kill(&ids[ids.len() - 1]).is_some());
        assert_eq!(owned.request_kill(&ids[0]), None);
    }
}
