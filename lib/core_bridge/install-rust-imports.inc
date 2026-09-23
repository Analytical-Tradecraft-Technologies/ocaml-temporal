;; Keep every generated MinGW import archive beside the installed cmxa so
;; $CAMLORIGIN follows a relocated SDK. Install files individually: Dune's
;; Windows symlink fallback copies files and cannot copy a directory target.
(install
 (section lib)
 (files
  (glob_files
   (rust-imports/* with_prefix __private__/temporal_core_bridge/rust-imports))))
