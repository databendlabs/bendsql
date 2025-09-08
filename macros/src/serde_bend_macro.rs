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
use quote::{quote, quote_spanned};
use syn::{spanned::Spanned, Attribute, DeriveInput, Meta};

pub fn serde_bend_derive(tokens_input: TokenStream) -> TokenStream {
    let item = syn::parse::<DeriveInput>(tokens_input).expect("No DeriveInput");
    let struct_fields = crate::parser::parse_named_fields(&item, "serde_bend");

    let struct_name = &item.ident;
    let (impl_generics, ty_generics, where_clause) = item.generics.split_for_impl();

    let path = quote!(databend_driver::_macro_internal);

    // Generate field deserializations from Row
    let from_row_fields = struct_fields.named.iter().enumerate().map(|(i, field)| {
        let field_name = &field.ident;
        let field_type = &field.ty;
        // Check for skip_deserializing attribute
        let skip_deserializing = has_serde_bend_attr(field, "skip_deserializing");
        if skip_deserializing {
            quote_spanned! {field.span() =>
                #field_name: Default::default(),
            }
        } else {
            // Check for rename attribute
            let field_index = get_field_index(&field.attrs, i);
            quote_spanned! {field.span() =>
                #field_name: {
                    let col_value = row.values().get(#field_index)
                        .ok_or_else(|| format!("missing column at index {}", #field_index))?;
                    <#field_type>::try_from(col_value.clone())
                        .map_err(|_| format!("failed converting column {} to type {}", #field_index, std::any::type_name::<#field_type>()))?
                },
            }
        }
    });

    // Generate field serializations to Values for insert
    let to_values_fields = struct_fields.named.iter().map(|field| {
        let field_name = &field.ident;

        // Check for skip_serializing attribute
        let skip_serializing = has_serde_bend_attr(field, "skip_serializing");

        if skip_serializing {
            quote! {}
        } else {
            quote_spanned! {field.span() =>
                values.push((&self.#field_name).into());
            }
        }
    });

    // Generate the field names for SQL generation (excluding skip_serializing for INSERT)
    let insert_field_names = struct_fields
        .named
        .iter()
        .filter_map(|field| {
            let field_name = &field.ident;

            // For INSERT: exclude skip_serializing
            let skip_serializing = has_serde_bend_attr(field, "skip_serializing");

            if skip_serializing {
                None
            } else {
                let name = get_renamed_field_name(&field.attrs)
                    .unwrap_or_else(|| field_name.as_ref().unwrap().to_string());
                Some(quote! { #name })
            }
        })
        .collect::<Vec<_>>();

    // Generate the field names for SQL queries (excluding skip_deserializing for SELECT)
    let query_field_names = struct_fields
        .named
        .iter()
        .filter_map(|field| {
            let field_name = &field.ident;

            // For SELECT: exclude skip_deserializing
            let skip_deserializing = has_serde_bend_attr(field, "skip_deserializing");
            let skip_both = has_serde_bend_attr(field, "skip_serializing")
                && has_serde_bend_attr(field, "skip_deserializing");

            if skip_deserializing || skip_both {
                None
            } else {
                let name = get_renamed_field_name(&field.attrs)
                    .unwrap_or_else(|| field_name.as_ref().unwrap().to_string());
                Some(quote! { #name })
            }
        })
        .collect::<Vec<_>>();

    // For backward compatibility, use insert_field_names as default
    let field_names = &insert_field_names;

    let generated = quote! {
        impl #impl_generics TryFrom<#path::Row> for #struct_name #ty_generics #where_clause {
            type Error = String;

            fn try_from(row: #path::Row) -> Result<Self, String> {
                Ok(#struct_name {
                    #(#from_row_fields)*
                })
            }
        }

        impl #impl_generics #struct_name #ty_generics #where_clause {
            pub fn field_names() -> Vec<&'static str> {
                vec![#(#field_names),*]
            }

            pub fn query_field_names() -> Vec<&'static str> {
                vec![#(#query_field_names),*]
            }

            pub fn insert_field_names() -> Vec<&'static str> {
                vec![#(#insert_field_names),*]
            }

            pub fn to_values(&self) -> Vec<#path::Value> {
                let mut values = Vec::new();
                #(#to_values_fields)*
                values
            }
        }

        impl #impl_generics databend_driver::RowORM for #struct_name #ty_generics #where_clause {
            fn field_names() -> Vec<&'static str> {
                Self::field_names()
            }

            fn query_field_names() -> Vec<&'static str> {
                Self::query_field_names()
            }

            fn insert_field_names() -> Vec<&'static str> {
                Self::insert_field_names()
            }

            fn to_values(&self) -> Vec<databend_driver::_macro_internal::Value> {
                self.to_values()
            }
        }
    };

    TokenStream::from(generated)
}

// check if field has serde_bend attribute
fn has_serde_bend_attr(field: &syn::Field, attr_name: &str) -> bool {
    field.attrs.iter().any(|attr| {
        if attr.path().is_ident("serde_bend") {
            if let Meta::List(list) = &attr.meta {
                return list.tokens.to_string().contains(attr_name);
            }
        }
        false
    })
}

// get the renamed field name
fn get_renamed_field_name(attrs: &[Attribute]) -> Option<String> {
    for attr in attrs {
        if attr.path().is_ident("serde_bend") {
            if let Meta::List(list) = &attr.meta {
                let tokens_str = list.tokens.to_string();

                if let Some(start) = tokens_str.find("rename = \"") {
                    let start = start + "rename = \"".len();
                    if let Some(end) = tokens_str[start..].find('"') {
                        return Some(tokens_str[start..start + end].to_string());
                    }
                }

                if let Some(start) = tokens_str.find("rename = ") {
                    let start = start + "rename = ".len();
                    let end = tokens_str[start..]
                        .find(',')
                        .unwrap_or(tokens_str[start..].len());
                    let name = tokens_str[start..start + end].trim();
                    if !name.is_empty() && !name.starts_with('"') {
                        return Some(name.to_string());
                    }
                }
            }
        }
    }
    None
}

fn get_field_index(_attrs: &[Attribute], default_index: usize) -> usize {
    default_index
}
