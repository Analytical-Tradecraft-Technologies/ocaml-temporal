# Digest-pinned ocaml/opam base image for the OCaml 5.2 compiler series.
#
# This single FROM line is the only source of truth for the series' base
# image: scripts/ocaml-image.sh reads it and the Makefile passes the reference
# to Dockerfile.dev as OCAML_IMAGE. It lives alone in its own directory so the
# matching Dependabot docker entry tracks exactly this tag and refreshes only
# its manifest-index digest (#1027). Never change the tag to another series
# here; add or remove a directory together with scripts/ci-matrix.py instead.
FROM ocaml/opam:debian-12-ocaml-5.2@sha256:2dc415f163a303dc79fe77b5a64a4fe9303b153ad7ea53af85653337037146b6
