extern crate proc_macro;
use syn::{parse_macro_input, FnArg, ItemTrait, ReturnType, TraitItem, Type};

#[proc_macro_attribute]
pub fn service(
    _attr: proc_macro::TokenStream,
    mut item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let item1 = item.clone();
    let input = parse_macro_input!(item1 as ItemTrait);

    let service_name = input.ident;

    let service_mod = {
        use convert_case::{Case, Casing};
        quote::format_ident!("{}", service_name.to_string().to_case(Case::Snake))
    };

    let ops = input.items.iter().filter_map(|i| {
        if let TraitItem::Fn(f) = i {
            let name = f.sig.ident.clone();
            let inputs = f.sig.inputs.pairs().filter_map(|t| {
                if let FnArg::Typed(a) = t.value() {
                    Some(a.ty.clone())
                } else {
                    None
                }
            });
            let inputs = inputs.collect::<Vec<_>>();
            if inputs.is_empty() {
                Some(quote::quote! {
                #name(()),
                })
            } else {
                Some(quote::quote! {
                #name(#(#inputs,)*),
                })
            }
        } else {
            None
        }
    });

    let resps = input.items.iter().filter_map(|i| {
        if let TraitItem::Fn(f) = i {
            let name = f.sig.ident.clone();
            Some(if let ReturnType::Type(_, ty) = f.sig.output.clone() {
                quote::quote! { #name(#ty), }
            } else {
                quote::quote! { #name(()), }
            })
        } else {
            None
        }
    });

    let client_impls = input.items.iter().filter_map(|i| {
        if let TraitItem::Fn(f) = i {
            let mut sig = f.sig.clone();
            sig.output = ReturnType::Type(
                Default::default(),
                Box::new(Type::Verbatim(match sig.output {
                    ReturnType::Type(_, typ) => {
                        quote::quote! { crlf::bincode::Result< #typ > }
                    }
                    ReturnType::Default => {
                        quote::quote! { crlf::bincode::Result<()> }
                    }
                })),
            );
            let name = sig.ident.clone();
            let args = sig
                .inputs
                .pairs()
                .filter_map(|t| {
                    if let FnArg::Typed(a) = t.value() {
                        Some(a.pat.clone())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();

            let op = if args.is_empty() {
                quote::quote! { let op = super::rpc::Request::#name(()); }
            } else {
                quote::quote! { let op = super::rpc::Request::#name(#(#args,)*); }
            };

            Some(quote::quote! {
            pub #sig {
            #op
            crlf::bincode::serialize_into(&mut self.sender, &op).expect("Oops");
            let response = crlf::bincode::deserialize_from(&mut self.receiver)?;
            match response {
                super::rpc::Response::#name(output) => Ok(output),
                r => panic!("Unexpected response: {:?}", r),
            }
            }
            })
        } else {
            None
        }
    });

    let server_fns = input.items.iter().filter_map(|i| {
        if let TraitItem::Fn(f) = i {
            let name = f.sig.ident.clone();
            let inputs = f
                .sig
                .inputs
                .pairs()
                .filter_map(|t| {
                    if let FnArg::Typed(a) = t.value() {
                        Some(a.pat.clone())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();

            let inputs2 = inputs.clone();

            let inner = quote::quote! {
            let resp = super::rpc::Response::#name(self.inner.#name(#(#inputs2,)*));
            crlf::bincode::serialize_into(&mut self.sender, &resp).expect("Oops");
            };

            if inputs.is_empty() {
                Some(quote::quote! { Ok(super::rpc::Request::#name(())) => { #inner }, })
            } else {
                Some(quote::quote! { Ok(super::rpc::Request::#name(#(#inputs,)*)) => { #inner }, })
            }
        } else {
            None
        }
    });

    let imp = quote::quote! {

    pub mod #service_mod {
    use super::*;
    pub(self) mod rpc {
	use super::*;
        use crlf::serde::{Serialize, Deserialize};
        #[allow(non_camel_case_types)]
        #[derive(Debug, Serialize, Deserialize)]
        #[serde(crate = "crlf::serde")]
        pub(super) enum Request {
        #(#ops)*
        }

        #[allow(non_camel_case_types)]
        #[derive(Debug, Serialize, Deserialize)]
        #[serde(crate = "crlf::serde")]
        pub(super) enum Response {
        #(#resps)*
        }
    }

    pub mod client {
	use super::*;
        pub struct #service_name<W, R> {
        pub sender: W,
        pub receiver: R,
        }

        impl<W: ::std::io::Write, R: ::std::io::Read> #service_name<W, R> {
        #(#client_impls)*
        }
    }

    pub mod server {
	use super::*;
        pub struct #service_name<W, R, S> {
        pub sender: W,
        pub receiver: R,
        pub inner: S,
        }

        impl<W: ::std::io::Write, R: ::std::io::Read, S: super::super::#service_name> #service_name<W, R, S> {
        pub fn run(&mut self) {
        loop {
        match crlf::bincode::deserialize_from::<_, super::rpc::Request>(&mut self.receiver).map_err(|c| *c) {
        #(#server_fns)*
        Err(crlf::bincode::ErrorKind::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
        e => panic!("Malformed RPC {:?}", e),
        }
        }
        }
        }
    }
    }
    };

    Extend::extend(&mut item, proc_macro::TokenStream::from(imp));
    item
}
