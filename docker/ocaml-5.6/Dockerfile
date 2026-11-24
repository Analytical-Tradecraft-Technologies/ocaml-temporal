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
FROM ocaml/opam:debian-12-ocaml-5.6@sha256:29943ff7c5d33b5dabfe6154919dd2ee189c28d9bcd45525cb2730c95fdf9fcd
