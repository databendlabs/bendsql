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

use super::{APIClient, QuerySubmission};

#[test]
fn unsent_submission_preserves_response_id_on_either_side_of_publication() {
    for response_first in [true, false] {
        let client = APIClient::default();
        client.set_last_query_id(Some("older-query".into()));
        if response_first {
            client.set_last_query_id(Some("server-response-id".into()));
        }

        let submission = QuerySubmission::new(&client, "new-submission-id".into());
        assert_eq!(client.last_query_id().as_deref(), Some("new-submission-id"));
        assert_eq!(
            client.last_query_id.lock().pending["new-submission-id"].as_deref(),
            Some(if response_first {
                "server-response-id"
            } else {
                "older-query"
            }),
        );
        if !response_first {
            client.set_last_query_id(Some("server-response-id".into()));
        }

        // A response before publication must be restored; a response after
        // publication must not be overwritten by an unsent query's rollback.
        drop(submission);
        assert_eq!(
            client.last_query_id().as_deref(),
            Some("server-response-id")
        );
    }
}

#[test]
fn concurrent_submission_rollback_never_resurrects_an_unsent_id() {
    let completion_orders: &[&[usize]] = &[
        &[0, 1],
        &[1, 0],
        &[0, 1, 2],
        &[0, 2, 1],
        &[1, 0, 2],
        &[1, 2, 0],
        &[2, 0, 1],
        &[2, 1, 0],
    ];
    for previous_id in [None, Some("previous-server-id".to_string())] {
        for order in completion_orders {
            // Exercise every mix of definitely-unsent and possibly-sent queries.
            for sent_mask in 0..(1 << order.len()) {
                let client = APIClient::default();
                client.set_last_query_id(previous_id.clone());
                let mut submissions: Vec<_> = (0..order.len())
                    .map(|index| {
                        let mut submission =
                            QuerySubmission::new(&client, format!("query-{index}"));
                        submission.may_have_reached_server = sent_mask & (1 << index) != 0;
                        Some(submission)
                    })
                    .collect();
                let mut valid = vec![true; order.len()];
                for &index in *order {
                    drop(submissions[index].take());
                    if sent_mask & (1 << index) == 0 {
                        valid[index] = false;
                    }
                    let expected = valid
                        .iter()
                        .rposition(|&valid| valid)
                        .map(|index| format!("query-{index}"))
                        .or_else(|| previous_id.clone());
                    assert_eq!(
                        client.last_query_id(),
                        expected,
                        "order={order:?}, sent_mask={sent_mask}, finished={index}"
                    );
                    assert_eq!(
                        client.last_query_id.lock().pending.len(),
                        submissions
                            .iter()
                            .filter(|submission| submission.is_some())
                            .count()
                    );
                }
            }
        }
    }
}

#[test]
fn rollback_invalidation_propagates_to_new_submissions() {
    let client = APIClient::default();
    client.set_last_query_id(Some("server-response-id".into()));
    let first = QuerySubmission::new(&client, "first-unsent".into());
    let second = QuerySubmission::new(&client, "second-unsent".into());
    drop(first);
    let third = QuerySubmission::new(&client, "third-unsent".into());
    drop(second);
    drop(third);
    assert_eq!(
        client.last_query_id().as_deref(),
        Some("server-response-id")
    );
    assert!(client.last_query_id.lock().pending.is_empty());
}

#[test]
fn concurrent_unsent_rollbacks_preserve_response_overrides() {
    for response_at in 0..=3 {
        let client = APIClient::default();
        let submissions: Vec<_> = (0..3)
            .map(|index| Some(QuerySubmission::new(&client, format!("unsent-{index}"))))
            .collect();
        for (step, submission) in submissions
            .into_iter()
            .chain(std::iter::once(None))
            .enumerate()
        {
            if step == response_at {
                client.set_last_query_id(Some("server-response-id".into()));
            }
            drop(submission);
            if step >= response_at {
                assert_eq!(
                    client.last_query_id().as_deref(),
                    Some("server-response-id")
                );
            }
        }
        assert!(client.last_query_id.lock().pending.is_empty());
    }
}
