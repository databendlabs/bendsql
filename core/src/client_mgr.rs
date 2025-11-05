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

use crate::APIClient;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::{Arc, Weak};
use tokio::runtime::Runtime;
use tokio::sync::Notify;
use tokio::time::{Duration, Instant};

pub static GLOBAL_CLIENT_MANAGER: Lazy<ClientManager> = Lazy::new(ClientManager::new);
pub static GLOBAL_RUNTIME: Lazy<Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create global Tokio runtime")
});

pub(crate) struct ClientManager {
    clients: Arc<Mutex<HashMap<String, Weak<APIClient>>>>,
    notify: Arc<Notify>,
}

impl ClientManager {
    fn new() -> Self {
        let idle_interval = 3600;
        let mut busy_interval = 15;
        if let Ok(val) = std::env::var("DATABEND_DRIVER_HEARTBEAT_INTERVAL_SECONDS") {
            // only used for test
            busy_interval = val
                .parse()
                .expect("Failed to parse DATABEND_DRIVER_HEARTBEAT_INTERVAL_SECONDS");
        }
        let clients = Arc::new(Mutex::new(HashMap::<String, Weak<APIClient>>::new()));
        let clients_clone = clients.clone();
        let notify = Arc::new(Notify::new());
        let notify_clone = Arc::clone(&notify);
        GLOBAL_RUNTIME.spawn(async move {
            let mut interval = idle_interval;
            loop {
                match tokio::time::timeout_at(
                    Instant::now() + Duration::from_secs(interval),
                    notify_clone.notified(),
                )
                .await
                {
                    Ok(()) => {
                        interval = busy_interval;
                    }
                    Err(_) => {
                        let clients: Vec<_> = clients_clone.lock().values().cloned().collect();
                        if clients.is_empty() {
                            let guard = clients_clone.lock();
                            if guard.is_empty() {
                                interval = idle_interval;
                            }
                        } else {
                            for client in clients {
                                if let Some(client) = client.upgrade() {
                                    if let Err(err) = client.try_heartbeat().await {
                                        let session_id = client.session_id.as_str();
                                        log::error!(
                                            "[session {session_id}] heartbeat failed: {err}"
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });
        Self { clients, notify }
    }

    pub(crate) async fn register_client(&self, client: Arc<APIClient>) {
        let mut guard = self.clients.lock();
        guard.insert(client.session_id.clone(), Arc::downgrade(&client));
        if guard.len() == 1 {
            self.notify.notify_one();
        }
    }

    pub(crate) fn unregister_client(&self, id: &str) {
        self.clients.lock().remove(id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_client_tracks_active_session() {
        let mgr = Arc::new(ClientManager::new());
        let mut client = APIClient::default();
        client.session_id = "session-1".to_string();
        let client = Arc::new(client);

        let mgr_clone = Arc::clone(&mgr);
        let client_clone = Arc::clone(&client);
        GLOBAL_RUNTIME.block_on(async move {
            mgr_clone.register_client(client_clone).await;
        });

        {
            let guard = mgr.clients.lock();
            let stored = guard.get("session-1").expect("client not stored");
            assert!(
                stored.upgrade().is_some(),
                "stored weak reference is dangling"
            );
            assert_eq!(guard.len(), 1);
        }

        drop(client);
        let guard = mgr.clients.lock();
        let stored = guard.get("session-1").expect("client missing after drop");
        assert!(
            stored.upgrade().is_none(),
            "weak reference should be cleared after client drop"
        );
    }

    #[test]
    fn unregister_client_removes_session() {
        let mgr = Arc::new(ClientManager::new());
        let mut client = APIClient::default();
        client.session_id = "session-2".to_string();
        let client = Arc::new(client);

        let mgr_clone = Arc::clone(&mgr);
        let client_clone = Arc::clone(&client);
        GLOBAL_RUNTIME.block_on(async move {
            mgr_clone.register_client(client_clone).await;
        });

        mgr.unregister_client("session-2");
        assert!(
            !mgr.clients.lock().contains_key("session-2"),
            "client entry should be removed after unregister"
        );
    }
}
