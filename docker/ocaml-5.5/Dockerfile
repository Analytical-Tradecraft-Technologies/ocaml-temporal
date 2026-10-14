# Digest-pinned ocaml/opam base image for the OCaml 5.5 compiler series.
#
# This single FROM line is the only source of truth for the series' base
# image: scripts/ocaml-image.sh reads it and the Makefile passes the reference
# to Dockerfile.dev as OCAML_IMAGE. It lives alone in its own directory so the
# matching Dependabot docker entry tracks exactly this tag and refreshes only
# its manifest-index digest (#1027). Never change the tag to another series
# here; add or remove a directory together with scripts/ci-matrix.py instead.
FROM ocaml/opam:debian-12-ocaml-5.5@sha256:36803e379c31545ef00dfb64f8bdf3226b5e8c7b76746b636caedda811c625f3
