# Digest-pinned ocaml/opam base image for the OCaml 5.5 compiler series.
#
# This single FROM line is the only source of truth for the series' base
# image: scripts/ocaml-image.sh reads it and the Makefile passes the reference
# to Dockerfile.dev as OCAML_IMAGE. It lives alone in its own directory so the
# matching Dependabot docker entry tracks exactly this tag and refreshes only
# its manifest-index digest (#1027). Never change the tag to another series
# here; add or remove a directory together with scripts/ci-matrix.py instead.
FROM ocaml/opam:debian-12-ocaml-5.5@sha256:57f87030c6082e3f46f59988fbf12a84947945baf2974d4567a9dcee85780090
