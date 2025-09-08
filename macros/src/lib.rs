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

use proc_macro::TokenStream;

mod from_row;
mod parser;
mod serde_bend_macro;

#[proc_macro_derive(TryFromRow)]
pub fn try_from_row_derive(tokens_input: TokenStream) -> TokenStream {
    from_row::try_from_row_derive(tokens_input)
}

#[proc_macro_derive(serde_bend, attributes(serde_bend))]
pub fn serde_bend_derive(tokens_input: TokenStream) -> TokenStream {
    serde_bend_macro::serde_bend_derive(tokens_input)
}
