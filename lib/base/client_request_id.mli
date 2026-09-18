(** Fresh collision-resistant identity for one logical client request. Uses a
    new system-seeded random state and 128 random bits, without modifying the
    caller's random state. Generate once before transport retries; preserve
    explicitly supplied IDs. Must never be used for workflow commands. *)
val create : unit -> string
