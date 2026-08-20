(lang dune 3.18)
(name temporal-sdk)
(generate_opam_files false)
(source (github Analytical-Tradecraft-Technologies/ocaml-temporal))
(license Apache-2.0)
(authors "Michael Fowlie")
(maintainers "Michael Fowlie")
(maintenance_intent "(latest)")

(package
 (name temporal-sdk)
 (synopsis "Experimental Temporal workflows in modern OCaml")
 (description "An experimental typed OCaml 5 workflow SDK backed by Temporal Core")
 (tags (temporal workflow sdk experimental))
 (depends
  (ocaml (>= 5.2))
  conf-rust-2024
  conf-protoc
  (dune (>= 3.18))
  (logs (>= 0.10))
  (yojson (>= 3.0))))
