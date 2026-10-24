(** Provides the public, direct-style condition wait over the package-private
    scheduler condition store.  The store owns every continuation and is
    rechecked by the activation loop; this module exposes only typed results
    and ordinary OCaml predicates. *)

(** A result-aware condition predicate used by the public convenience API. *)
type predicate = unit -> (bool, Error.t) result

(** Converts an application predicate exception into a structured defect rather
    than allowing it to escape through the scheduler's activation boundary. *)
let protect_predicate predicate () =
  try predicate () with
  | exn ->
      Error
        (Error.defect
           ~message:(
             "Temporal.Condition predicate raised: " ^ Printexc.to_string exn))

(** Evaluates and waits through the current workflow context.  A call outside
    workflow execution returns a typed defect and retains no callback. *)
let wait_without_timeout predicate =
  match Temporal_sdk_kernel.Workflow_context_store.current () with
  | None ->
      Error
        (Error.defect
           ~message:
             "Temporal.Condition.wait_until used outside a workflow execution")
  | Some context ->
      let predicate () =
        match protect_predicate predicate () with
        | Ok value -> Ok value
        | Error error -> Error (Error_private.to_base error)
      in
      Temporal_sdk_kernel.Workflow_context_store.wait_until context ~predicate
      |> Result.map_error Error_private.of_base

(** The typed error returned when a bounded wait's durable timer fires before
    its predicate holds. It is an expected outcome, so it is retryable and
    carries no details. *)
let timeout_error () =
  Error.make ~category:`Timeout ~message:"workflow condition timed out" ()

(** Waits with an optional durable bound (#1134). The bound is an ordinary
    scoped workflow timer, and the predicate given to the condition store
    also reports [true] once that timer has fired, so the activation that
    delivers the firing wakes the waiter through the store's usual recheck.
    On wake the application predicate is evaluated once more: if it holds,
    the wait succeeds even when the timer fired in the same activation, and
    cancelling the scope emits [Cancel_timer] for a timer that is still
    pending, so a satisfied wait leaves no timer behind. Both outcomes are
    decided from replayed workflow state and history, so replay reaches the
    same one. A zero bound records no timer and checks the predicate once. *)
let wait_until_result ?timeout predicate =
  match (timeout, Temporal_sdk_kernel.Workflow_context_store.current ()) with
  | None, _ | Some _, None ->
      (* Without a workflow the plain wait reports the condition's own
         defect before any scope or timer could be created. *)
      wait_without_timeout predicate
  | Some timeout, Some _ -> (
      match Scope.create () with
      | Error _ as error -> error
      | Ok scope -> (
          let timer = Workflow.start_sleep ~scope timeout in
          let fired () =
            match Future.peek timer with Some (Ok ()) -> true | _ -> false
          in
          let finish result =
            (* Cancelling the private scope only ever cancels this wait's
               own timer; a failure to emit that command is surfaced rather
               than reported as success. *)
            match (result, Scope.cancel scope) with
            | (Error _ as error), _ -> error
            | Ok (), (Error _ as error) -> error
            | Ok (), Ok () -> Ok ()
          in
          (* Waits until the store reports the predicate held or the timer
             fired, then decides on the state as this fiber resumes. The
             store may queue several satisfied waiters before any of them
             runs, so an earlier one can falsify shared state again; such a
             stale wake neither succeeds nor times out (the timer has not
             fired), it simply waits again (Codex review on #1186). *)
          let rec wait () =
            match
              wait_without_timeout (fun () ->
                  match protect_predicate predicate () with
                  | Ok true -> Ok true
                  | Ok false -> Ok (fired ())
                  | Error _ as error -> error)
            with
            | Error error -> finish (Error error)
            | Ok () -> (
                (* Re-reading the predicate keeps satisfaction ahead of a
                   timeout delivered in the same activation. *)
                match protect_predicate predicate () with
                | Ok true -> finish (Ok ())
                | Ok false when fired () -> finish (Error (timeout_error ()))
                | Ok false -> wait ()
                | Error error -> finish (Error error))
          in
          match Future.peek timer with
          | Some (Error error) -> finish (Error error)
          | Some (Ok ()) | None -> wait ()))

(** Waits for a boolean predicate by adapting it to the typed predicate form. *)
let wait_until ?timeout predicate =
  wait_until_result ?timeout (fun () -> Ok (predicate ()))
