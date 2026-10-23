(** Derives the [<pid>@<hostname>] default identity shared by clients and
    workers. See the interface for the validity guarantees. *)

let max_hostname_bytes = 255
let fallback_hostname = "unknown-host"

(** Replaces bytes that could be control characters, whitespace, NUL, or part
    of a non-UTF-8 sequence. Restricting the host name to printable ASCII is a
    simple way to guarantee valid UTF-8 without decoding, and real host names
    are ASCII in practice. Truncation happens before mapping so the result can
    never exceed the bound. *)
let sanitize_hostname hostname =
  let truncated =
    if String.length hostname > max_hostname_bytes then
      String.sub hostname 0 max_hostname_bytes
    else hostname
  in
  let sanitized =
    String.map
      (fun c -> if c >= '\x21' && c <= '\x7e' then c else '_')
      truncated
  in
  if String.equal sanitized "" then fallback_hostname else sanitized

let of_parts ~pid ~hostname =
  string_of_int pid ^ "@" ^ sanitize_hostname hostname

let default () =
  (* Host-name lookup is an operating-system call; an unusual sandbox may make
     it fail, which must not prevent client or worker construction. *)
  let hostname =
    try Unix.gethostname () with Unix.Unix_error _ -> fallback_hostname
  in
  of_parts ~pid:(Unix.getpid ()) ~hostname

let resolve = function Some identity -> identity | None -> default ()
