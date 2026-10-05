(** Regression for #768: when the OCaml runtime cannot spawn another Domain,
    [Sdk_supervisor.create] must return a typed error instead of letting the
    [Domain.spawn] exception escape its [result]-typed API.

    Dune runs this executable with [OCAMLRUNPARAM=d=2], so the main Domain and
    one helper Domain exhaust the runtime limit deterministically without
    allocating many Domains on the CI runner. *)

(** The smallest backend that satisfies the supervisor signature. Creation
    must never run because no owner Domain can be spawned. *)
module Backend = struct
  type config = unit
  type state = unit
  type error = string
  (* No operation is ever performed, so the constructor is never built. *)
  type _ operation = Noop : unit operation [@@warning "-37"]

  (** Fails the test if the supervisor reaches backend creation. *)
  let create () = failwith "backend create ran without an owner Domain"

  (** Never reached; the supervisor cannot be created. *)
  let perform : type value. state -> value operation -> (value, error) result =
   fun () Noop -> Ok ()

  (** Never reached; no state is created. *)
  let shutdown () = Ok ()
end

module Supervisor = Sdk_supervisor.Make (Backend)

(** Occupies the one Domain slot left by [d=2], then checks that creation is
    reported as [Supervisor_failed] rather than raising. *)
let () =
  let release = Atomic.make false in
  let holder =
    Domain.spawn (fun () ->
        while not (Atomic.get release) do
          Domain.cpu_relax ()
        done)
  in
  (match Supervisor.create ~capacity:1 () with
  | Error (Supervisor.Supervisor_failed _) -> ()
  | Error _ -> failwith "Domain exhaustion returned the wrong supervisor error"
  | Ok _ -> failwith "supervisor was created beyond the Domain limit"
  | exception exn ->
      failwith ("Domain exhaustion escaped as " ^ Printexc.to_string exn));
  Atomic.set release true;
  Domain.join holder
