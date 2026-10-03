(** Converts runtime futures at the public package boundary. This module is
    Dune-private: it is the only place where a public future acquires a
    Temporal_base error conversion or a Future_store scheduler callback. *)

(* Wraps one runtime future while translating each result and preserving its
   scheduler callbacks. No runtime handle is stored in the public record. *)
(** Wraps one runtime future while translating its result and preserving the
    owner scheduler callbacks without storing the runtime record publicly. *)
let of_internal ?outside_error future =
  let outside_error =
    Option.value outside_error ~default:(fun () ->
        Error.defect
          ~message:"Temporal future awaited outside its workflow scheduler")
  in
  let map_error result = Result.map_error Error_private.of_base result in
  Temporal_sdk_kernel.Future.make
    ~await:(fun () ->
      map_error (Temporal_sdk_kernel.Future_store.await future))
    ~await_gate:(Temporal_sdk_kernel.Future_store.await_gate future)
    ~subscribe:(fun observer ->
      Temporal_sdk_kernel.Future_store.subscribe future (fun result ->
          observer (map_error result)))
    ~is_ready:(fun () -> Temporal_sdk_kernel.Future_store.is_ready future)
    ~peek:(fun () ->
      Option.map map_error (Temporal_sdk_kernel.Future_store.peek future))
    ~owner_id:(Temporal_sdk_kernel.Future_store.owner_id future)
    ~outside_error
    ~callbacks_live:(Temporal_sdk_kernel.Future_store.callback_liveness future)
    ~enqueue:(Temporal_sdk_kernel.Future_store.enqueue future)

(** Creates a settled public value for validation failures and detached calls;
    its callbacks are inert because no workflow scheduler owns the result. *)
let resolved ~outside_error result =
  Temporal_sdk_kernel.Future.make
    ~await:(fun () -> result)
    ~await_gate:(fun register -> register (fun () -> ()))
    ~subscribe:(fun observer -> observer result; fun () -> ())
    ~is_ready:(fun () -> true) ~peek:(fun () -> Some result) ~owner_id:(-1)
    ~outside_error ~callbacks_live:(fun () -> true)
    ~enqueue:(fun thunk -> thunk ())
