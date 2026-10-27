# Digest-pinned ocaml/opam base image for the OCaml 5.5 compiler series.
#
# This single FROM line is the only source of truth for the series' base
# image: scripts/ocaml-image.sh reads it and the Makefile passes the reference
# to Dockerfile.dev as OCAML_IMAGE. It lives alone in its own directory so the
# matching Dependabot docker entry tracks exactly this tag and refreshes only
# its manifest-index digest (#1027). Never change the tag to another series
# here; add or remove a directory together with scripts/ci-matrix.py instead.
FROM ocaml/opam:debian-12-ocaml-5.5@sha256:cf84af6ac7213c80046dc1f47732e7df6023cc8a8d42e86a2bd0d461e4071862
