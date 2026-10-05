(** Regression for #768: when the OCaml runtime cannot spawn another Domain,
    [Sdk_supervisor.create] must return a typed error instead of letting the
    [Domain.spawn] exception escape its [result]-typed API.

    Dune runs this executable with [OCAMLRUNPARAM=d=2], so on runtimes that
    honour the parameter the main Domain and one helper Domain exhaust the
    limit deterministically without allocating many Domains on the runner. *)

(** The smallest backend that satisfies the supervisor signature. Creation
    succeeds so the test can prove recovery once a Domain is free again. *)
module Backend = struct
  type config = unit
  type state = unit
  type error = string
  (* No operation is ever performed, so the constructor is never built. *)
  type _ operation = Noop : unit operation [@@warning "-37"]

  (** Creates empty state on the owner Domain. *)
  let create () = Ok ()

  (** Never reached; the supervisor cannot be created. *)
  let perform : type value. state -> value operation -> (value, error) result =
   fun () Noop -> Ok ()

  (** Releases the empty state. *)
  let shutdown () = Ok ()
end

module Supervisor = Sdk_supervisor.Make (Backend)

(** Blocks helper Domains without spinning until the test releases them. *)
type gate = { mutex : Mutex.t; released : Condition.t; mutable open_ : bool }

(** Parks one helper Domain until [release] opens the gate. *)
let wait gate =
  Mutex.lock gate.mutex;
  while not gate.open_ do
    Condition.wait gate.released gate.mutex
  done;
  Mutex.unlock gate.mutex

(** Wakes every parked helper Domain. *)
let release gate =
  Mutex.lock gate.mutex;
  gate.open_ <- true;
  Condition.broadcast gate.released;
  Mutex.unlock gate.mutex

(** Upper bound on helper Domains. With [d=2] one helper exhausts the limit;
    runtimes that ignore the [d] parameter (OCaml 5.2 keeps its fixed limit of
    128) reach this bound instead, and the test is skipped rather than
    allocating a hundred Domains on the CI runner. *)
let max_helpers = 4

(** Fills the runtime's Domain limit with parked helpers, checks that creation
    is reported as [Owner_unavailable] rather than raising, then releases the
    helpers and checks that a later creation and shutdown succeed. *)
let () =
  let gate =
    { mutex = Mutex.create (); released = Condition.create (); open_ = false }
  in
  let rec fill helpers =
    if List.length helpers >= max_helpers then (helpers, false)
    else
      match Domain.spawn (fun () -> wait gate) with
      | helper -> fill (helper :: helpers)
      | exception Failure _ -> (helpers, true)
  in
  let helpers, exhausted = fill [] in
  let finish () =
    release gate;
    List.iter Domain.join helpers
  in
  if not exhausted then begin
    finish ();
    print_endline "SKIP Domain exhaustion: runtime ignores OCAMLRUNPARAM d"
  end
  else begin
    (match Supervisor.create ~capacity:1 () with
    | Error (Supervisor.Owner_unavailable _) -> ()
    | Error _ ->
        failwith "Domain exhaustion returned the wrong supervisor error"
    | Ok _ -> failwith "supervisor was created beyond the Domain limit"
    | exception exn ->
        failwith ("Domain exhaustion escaped as " ^ Printexc.to_string exn));
    finish ();
    match Supervisor.create ~capacity:1 () with
    | Ok supervisor -> (
        match Supervisor.shutdown supervisor with
        | Ok () -> print_endline "PASS Domain exhaustion"
        | Error _ ->
            failwith "supervisor created after recovery failed to shut down")
    | Error _ ->
        failwith "supervisor creation did not recover after a Domain was freed"
  end
