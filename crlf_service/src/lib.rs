extern crate proc_macro;
use syn::{parse_macro_input, FnArg, ItemTrait, ReturnType, TraitItem};

#[proc_macro_attribute]
pub fn service(
    _attr: proc_macro::TokenStream,
    mut item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let item1 = item.clone();
    let input = parse_macro_input!(item1 as ItemTrait);

    let service_name = input.ident;

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
            Some(quote::quote! {
            #name(#(#inputs,)*),
            })
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
                quote::quote! { #name, }
            })
        } else {
            None
        }
    });

    let fn_impls = input.items.iter().filter_map(|i| {
        if let TraitItem::Fn(f) = i {
            let sig = f.sig.clone();
            let name = sig.ident.clone();
            let args = sig.inputs.pairs().filter_map(|t| {
                if let FnArg::Typed(a) = t.value() {
                    Some(a.pat.clone())
                } else {
                    None
                }
            });
            Some(quote::quote! {
            pub #sig {
                let op = super::rpc::Request::#name(#(#args,)*);
                crlf::bincode::serialize_into(&mut self.sender, &op).expect("Oops");
                match crlf::bincode::deserialize_from(&mut self.receiver) {
                Ok(super::rpc::Response::#name(output)) => output,
                _ => panic!("Malformed RPC"),
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
            let inputs = f.sig.inputs.pairs().filter_map(|t| {
                if let FnArg::Typed(a) = t.value() {
                    Some(a.pat.clone())
                } else {
                    None
                }
            });
            let inputs2 = inputs.clone();
            Some(quote::quote! {
            Ok(super::rpc::Request::#name(#(#inputs,)*)) => {
                let resp = super::rpc::Response::#name(self.inner.#name(#(#inputs2,)*));
                crlf::bincode::serialize_into(&mut self.sender, &resp).expect("Oops");
            },
            })
        } else {
            None
        }
    });

    let imp = quote::quote! {

    pub(self) mod rpc {
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
        pub struct #service_name<W, R> {
        pub sender: W,
        pub receiver: R,
        }

        impl<W: ::std::io::Write, R: ::std::io::Read> #service_name<W, R> {
        #(#fn_impls)*
        }
    }

    pub mod server {
        pub struct #service_name<W, R, S> {
        pub sender: W,
        pub receiver: R,
        pub inner: S,
        }

        impl<W: ::std::io::Write, R: ::std::io::Read, S: super::#service_name> #service_name<W, R, S> {
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
    };

    Extend::extend(&mut item, proc_macro::TokenStream::from(imp));
    item
}
