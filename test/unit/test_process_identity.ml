(** Checks the default [<pid>@<hostname>] client and worker identity without a
    Temporal service: it carries the process ID, always satisfies the bridge's
    identity validation, and never replaces an explicit identity. *)
module Identity = Temporal_base.Process_identity

(** Mirrors the OCaml and Rust identity validation: non-empty, bounded, valid
    UTF-8, and NUL-free. *)
let expect_valid identity =
  if String.equal identity "" then failwith "identity is empty";
  if String.length identity > 65_536 then failwith "identity exceeds limit";
  if not (String.is_valid_utf_8 identity) then failwith "identity not UTF-8";
  if String.contains identity '\000' then failwith "identity contains NUL"

(** The live default starts with this process's ID and a host-name suffix. *)
let test_default_contains_pid () =
  let identity = Identity.default () in
  expect_valid identity;
  let prefix = string_of_int (Unix.getpid ()) ^ "@" in
  let prefix_length = String.length prefix in
  if
    String.length identity <= prefix_length
    || not (String.equal (String.sub identity 0 prefix_length) prefix)
  then failwith ("default identity lacks pid@ prefix: " ^ identity);
  if not (String.equal (Identity.resolve None) identity) then
    failwith "resolve None must compute the default identity"

(** Ordinary host names pass through unchanged. *)
let test_ordinary_hostname () =
  if
    not
      (String.equal
         (Identity.of_parts ~pid:42 ~hostname:"worker-1.example.com")
         "42@worker-1.example.com")
  then failwith "ordinary host name was altered"

(** Unusual host names are sanitized, bounded, or replaced so the result stays
    valid for the bridge. *)
let test_unusual_hostnames () =
  let expect_equal expected actual =
    if not (String.equal expected actual) then
      failwith (Printf.sprintf "expected %S, got %S" expected actual)
  in
  expect_equal "7@unknown-host" (Identity.of_parts ~pid:7 ~hostname:"");
  expect_equal "7@a_b_c_d" (Identity.of_parts ~pid:7 ~hostname:"a\000b c\nd");
  expect_equal "7@h__" (Identity.of_parts ~pid:7 ~hostname:"h\xc3\xa9");
  let long = Identity.of_parts ~pid:7 ~hostname:(String.make 100_000 'x') in
  expect_valid long;
  expect_equal
    ("7@" ^ String.make Identity.max_hostname_bytes 'x')
    long

(** An explicitly supplied identity always wins over the default. *)
let test_explicit_identity_preserved () =
  if not (String.equal (Identity.resolve (Some "billing-worker")) "billing-worker")
  then failwith "explicit identity was replaced"

let () =
  test_default_contains_pid ();
  test_ordinary_hostname ();
  test_unusual_hostnames ();
  test_explicit_identity_preserved ()
