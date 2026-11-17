# Digest-pinned ocaml/opam base image for the OCaml 5.6 compiler series.
#
# 5.6 is the *next* series (#1164), outside the supported `< 5.6` bound: only
# the non-blocking readiness lane of master builds uses it (NEXT_SERIES in
# scripts/ci-matrix.py). Until 5.6.0 is released the tag carries the
# compiler's trunk (5.6.0+trunk) or an alpha. Dependabot refreshes the digest,
# so the lane follows upstream development.
#
# As for every series, this single FROM line is the only source of truth for
# the image: scripts/ocaml-image.sh reads it and the Makefile passes the
# reference to Dockerfile.dev as OCAML_IMAGE. Never change the tag to another
# series here; add or remove a directory together with scripts/ci-matrix.py.
FROM ocaml/opam:debian-12-ocaml-5.6@sha256:ae0187297967e9ca0a6355bf4481db6d5d788e2908586b00b9d52730672c9af7
