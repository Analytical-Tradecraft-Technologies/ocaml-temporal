(** Regression for #768: when the OCaml runtime cannot spawn another Domain,
    [Sdk_supervisor.create] must return a typed error instead of letting the
    [Domain.spawn] exception escape its [result]-typed API.

    Dune runs this executable with [OCAMLRUNPARAM=d=2], so the main Domain and
    one helper Domain exhaust the runtime limit deterministically without
    allocating many Domains on the CI runner. *)

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

(** Occupies the one Domain slot left by [d=2], checks that creation is
    reported as [Owner_unavailable] rather than raising, then releases the slot
    and checks that a later creation and shutdown succeed. *)
let () =
  let release = Atomic.make false in
  let holder =
    Domain.spawn (fun () ->
        while not (Atomic.get release) do
          Domain.cpu_relax ()
        done)
  in
  (match Supervisor.create ~capacity:1 () with
  | Error (Supervisor.Owner_unavailable _) -> ()
  | Error _ -> failwith "Domain exhaustion returned the wrong supervisor error"
  | Ok _ -> failwith "supervisor was created beyond the Domain limit"
  | exception exn ->
      failwith ("Domain exhaustion escaped as " ^ Printexc.to_string exn));
  Atomic.set release true;
  Domain.join holder;
  match Supervisor.create ~capacity:1 () with
  | Ok supervisor -> (
      match Supervisor.shutdown supervisor with
      | Ok () -> ()
      | Error _ -> failwith "supervisor created after recovery failed to shut down")
  | Error _ -> failwith "supervisor creation did not recover after a Domain was freed"
