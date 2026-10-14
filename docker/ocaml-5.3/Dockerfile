# Digest-pinned ocaml/opam base image for the OCaml 5.3 compiler series.
#
# This single FROM line is the only source of truth for the series' base
# image: scripts/ocaml-image.sh reads it and the Makefile passes the reference
# to Dockerfile.dev as OCAML_IMAGE. It lives alone in its own directory so the
# matching Dependabot docker entry tracks exactly this tag and refreshes only
# its manifest-index digest (#1027). Never change the tag to another series
# here; add or remove a directory together with scripts/ci-matrix.py instead.
FROM ocaml/opam:debian-12-ocaml-5.3@sha256:bbaac53e502f6602013d8967c3a54cfcb898b556f453ab72e8e23966c3c681df
