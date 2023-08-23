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

use syn::{Data, DeriveInput, ExprLit, Fields, FieldsNamed, Lit};
use syn::{Expr, Meta};

/// Parses the tokens_input to a DeriveInput and returns the struct name from which it derives and
/// the named fields
pub(crate) fn parse_named_fields<'a>(
    input: &'a DeriveInput,
    current_derive: &str,
) -> &'a FieldsNamed {
    match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(named_fields) => named_fields,
            _ => panic!(
                "derive({}) works only for structs with named fields. Tuples don't need derive.",
                current_derive
            ),
        },
        _ => panic!("derive({}) works only on structs!", current_derive),
    }
}

pub(crate) fn get_path(input: &DeriveInput) -> Result<proc_macro2::TokenStream, syn::Error> {
    let mut this_path: Option<proc_macro2::TokenStream> = None;
    for attr in input.attrs.iter() {
        if !attr.path().is_ident("databend_driver") {
            continue;
        }
        match &attr.meta {
            Meta::NameValue(name_value) => {
                if let Expr::Lit(ExprLit {
                    lit: Lit::Str(lit), ..
                }) = &name_value.value
                {
                    let path = syn::Ident::new(&lit.value(), lit.span());
                    if this_path.is_none() {
                        this_path = Some(quote::quote!(#path::_macro_internal));
                    } else {
                        return Err(syn::Error::new_spanned(
                            &name_value.path,
                            "the `databend_driver` attribute was set multiple times",
                        ));
                    }
                } else {
                    return Err(syn::Error::new_spanned(
                        &name_value.value,
                        "the `databend_driver` attribute should be a string literal",
                    ));
                }
            }
            other => {
                return Err(syn::Error::new_spanned(
                    other,
                    "the `databend_driver` attribute have a single value",
                ));
            }
        }
    }
    Ok(this_path.unwrap_or_else(|| quote::quote!(scylla::_macro_internal)))
}
