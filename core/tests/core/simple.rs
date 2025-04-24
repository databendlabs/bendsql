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

use databend_client::APIClient;
use tokio_stream::StreamExt;

use crate::common::DEFAULT_DSN;

#[tokio::test]
async fn select_simple() {
    let dsn = option_env!("TEST_DATABEND_DSN").unwrap_or(DEFAULT_DSN);
    let client = APIClient::new(dsn, None).await.unwrap();
    let mut pages = client.start_query("select 15532", true).await.unwrap();
    let page = pages.next().await.unwrap().unwrap();
    assert_eq!(page.data, [[Some("15532".to_string())]]);
}
