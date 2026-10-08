(** Binding operators for sequencing [result] values in direct-style code. *)

(** [let*] sequences explicit SDK failures in direct-style workflow helpers. *)
val ( let* ) : ('a, 'error) result -> ('a -> ('b, 'error) result) -> ('b, 'error) result

(** [let+] maps a successful result while preserving its error unchanged. *)
val ( let+ ) : ('a, 'error) result -> ('a -> 'b) -> ('b, 'error) result
