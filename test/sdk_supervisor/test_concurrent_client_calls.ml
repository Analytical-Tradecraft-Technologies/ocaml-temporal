(** Deterministic tests for submit-and-complete client calls (#807).

    A fake backend stands in for the Rust bridge behind the real generic
    supervisor and the real {!Sdk_supervisor.Client_call.await} loop. Its
    long-poll submissions (wait, query) never complete on their own, and its
    signal and start submissions complete at once, exactly like the native
    completion cells: the owner only creates the cell, and the caller waits
    on it from its own Domain. The tests show that a pending wait or query
    does not delay a concurrent signal or start, that shutdown closes calls
    in flight with the typed [Closed] error, and that a deadline which
    expires while its request waits in the mailbox completes the call
    without submitting anything. *)

module Bridge = Temporal_core_bridge.Native_bridge
module Client_call = Sdk_supervisor.Client_call
module Protocol_adapter = Sdk_supervisor.Native.Protocol_adapter
module Client = Temporal_protocol.Client_protocol

(** Fails with [label] when [condition] does not hold. *)
let check label condition = if not condition then failwith label

(** One fake completion cell, mirroring a Rust call slot. All fields are
    protected by [mutex]; [changed] is broadcast on every state change. *)
type cell = {
  mutex : Mutex.t;
  changed : Condition.t;
  mutable state : [ `Pending | `Ready of (bytes, Bridge.error) result | `Closed ];
}

(** Allocates a pending cell. *)
let pending_cell () =
  { mutex = Mutex.create (); changed = Condition.create (); state = `Pending }

(** Runs [operation] with [cell]'s mutex held. *)
let with_cell cell operation =
  Mutex.lock cell.mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock cell.mutex) (fun () -> operation ())

(** Publishes [outcome] into a pending cell, as a finished Rust task does. *)
let complete cell outcome =
  with_cell cell (fun () ->
      if cell.state = `Pending then (
        cell.state <- `Ready outcome;
        Condition.broadcast cell.changed))

(** Closes a pending cell, as runtime disconnect does for calls in flight. *)
let close cell =
  with_cell cell (fun () ->
      if cell.state = `Pending then (
        cell.state <- `Closed;
        Condition.broadcast cell.changed))

(** The fake [poll] given to [Client_call.await]: one bounded wait that
    reports [Not_ready] while the cell is pending, [Invalid_state] once it
    is closed, and otherwise the published outcome. Like the native await it
    runs on the caller's Domain and never involves the supervisor owner. *)
let poll cell ~timeout_ms =
  let deadline = Unix.gettimeofday () +. (float_of_int timeout_ms /. 1000.) in
  let rec loop () =
    let state = with_cell cell (fun () -> cell.state) in
    match state with
    | `Ready outcome -> outcome
    | `Closed ->
        Error { Bridge.status = Invalid_state; message = "call closed" }
    | `Pending when Unix.gettimeofday () >= deadline ->
        Error { Bridge.status = Not_ready; message = "pending" }
    | `Pending ->
        Thread.delay 0.001;
        loop ()
  in
  loop ()

(** A manually opened gate used to hold the owner Domain inside one
    operation. *)
type gate = { gate_mutex : Mutex.t; opened : Condition.t; mutable open_ : bool }

(** Creates a closed gate. *)
let create_gate () =
  { gate_mutex = Mutex.create (); opened = Condition.create (); open_ = false }

(** Blocks the current thread until [gate] opens. *)
let await_gate gate =
  Mutex.lock gate.gate_mutex;
  while not gate.open_ do
    Condition.wait gate.opened gate.gate_mutex
  done;
  Mutex.unlock gate.gate_mutex

(** Opens [gate] and wakes every waiter. *)
let open_gate gate =
  Mutex.lock gate.gate_mutex;
  gate.open_ <- true;
  Condition.broadcast gate.opened;
  Mutex.unlock gate.gate_mutex

(** Fake client backend. Its state is touched only on the owner Domain,
    except for the cells, which are synchronized by their own mutexes and
    are the only thing callers wait on. *)
module Backend = struct
  (** Names of the requests that reached the fake bridge, newest first, so a
      test can prove an expired request was never sent. Written only by the
      owner Domain and read by tests after shutdown has joined it. *)
  type config = string list ref

  (** [cells] lists every cell created, so shutdown can close the pending
      ones. Owner-only. *)
  type state = { cells : cell list ref; submitted : string list ref }

  type error = Bridge.error

  (** The fake request language. Submissions return a call whose ticket is
      a fake cell. [Block] holds the owner Domain until its gate opens. *)
  type _ operation =
    | Submit_long : string -> (string, cell) Client_call.t operation
    | Submit_short :
        string * Client.rpc_deadline option
        -> (string, cell) Client_call.t operation
    | Block : gate -> unit operation

  let create submitted = Ok { cells = ref []; submitted }

  (** Decodes a fake outcome: the bytes as a string, failures unchanged. *)
  let decode = Result.map Bytes.to_string

  (** Registers a new cell for [name] and returns it as an in-flight call. *)
  let submit state name =
    let cell = pending_cell () in
    state.cells := cell :: !(state.cells);
    state.submitted := name :: !(state.submitted);
    (cell, Client_call.In_flight { ticket = cell; decode })

  let perform : type value. state -> value operation -> (value, error) result =
   fun state -> function
    | Submit_long name -> Ok (snd (submit state name))
    | Submit_short (name, deadline) ->
        (* The same deadline resolution the native backend applies when the
           owner dispatches a request (#499). *)
        Protocol_adapter.with_rpc_deadline ~now_ns:(Bridge.monotonic_now_ns ())
          deadline
          ~expired:(fun () ->
            Ok
              (Client_call.Completed
                 (decode (Error Protocol_adapter.expired_rpc_deadline_error))))
          ~live:(fun _ ->
            let cell, call = submit state name in
            (* A short RPC: the fake transport answers at once. *)
            complete cell (Ok (Bytes.of_string (name ^ " ok")));
            Ok call)
    | Block gate ->
        await_gate gate;
        Ok ()

  (** Closes every call still in flight, as native disconnect does. *)
  let shutdown state =
    List.iter close !(state.cells);
    Ok ()
end

module Supervisor = Sdk_supervisor.Make (Backend)

(** Submits [operation] through the owner and awaits the call on this
    Domain, mapping a closed call to [`Closed]. *)
let call supervisor operation =
  match Supervisor.perform supervisor operation with
  | Error Supervisor.Closed -> Error `Closed
  | Error _ -> Error `Supervisor
  | Ok submitted -> (
      match Client_call.await ~poll ~slice_ms:20 submitted with
      | Ok value -> Ok value
      | Error Client_call.Closed -> Error `Closed
      | Error (Client_call.Failed error) -> Error (`Failed error))

(** Spawns a Domain that performs one long call and records when it has
    been submitted. *)
let spawn_long supervisor name =
  let submitted = Atomic.make false in
  let domain =
    Domain.spawn (fun () ->
        match Supervisor.perform supervisor (Backend.Submit_long name) with
        | Error _ -> Error `Supervisor
        | Ok submitted_call -> (
            Atomic.set submitted true;
            match Client_call.await ~poll ~slice_ms:20 submitted_call with
            | Ok value -> Ok value
            | Error Client_call.Closed -> Error `Closed
            | Error (Client_call.Failed error) -> Error (`Failed error)))
  in
  while not (Atomic.get submitted) do
    Domain.cpu_relax ()
  done;
  domain

(** Elapsed wall-clock seconds of [operation] and its result. *)
let timed operation =
  let started = Unix.gettimeofday () in
  let result = operation () in
  (Unix.gettimeofday () -. started, result)

(** With a wait and a query pending on two other Domains, a signal and a
    start on the same supervisor still complete at once: neither long call
    occupies the owner. Shutdown then closes both long calls with the typed
    [Closed] error, and later submissions are refused. *)
let test_long_calls_do_not_delay_short_calls () =
  let supervisor = Result.get_ok (Supervisor.create ~capacity:4 (ref [])) in
  let wait = spawn_long supervisor "wait" in
  let query = spawn_long supervisor "query" in
  List.iter
    (fun name ->
      let elapsed, result =
        timed (fun () -> call supervisor (Backend.Submit_short (name, None)))
      in
      check (name ^ " result") (result = Ok (name ^ " ok"));
      (* Generous for a loaded CI host; a blocked owner would never answer. *)
      check (name ^ " was delayed by a pending long call") (elapsed < 2.0))
    [ "signal"; "start"; "signal" ];
  check "shutdown" (Supervisor.shutdown supervisor = Ok ());
  check "wait closed" (Domain.join wait = Error `Closed);
  check "query closed" (Domain.join query = Error `Closed);
  check "submission after shutdown"
    (call supervisor (Backend.Submit_short ("signal", None)) = Error `Closed)

(** A long call completed by its transport delivers its own outcome to its
    own caller, while another long call on the same supervisor stays
    pending until shutdown closes it. *)
let test_completion_reaches_only_its_caller () =
  let supervisor = Result.get_ok (Supervisor.create ~capacity:4 (ref [])) in
  let cells = ref [] in
  (* Submit directly to keep each cell, then await on separate Domains. *)
  let submit name =
    match Supervisor.perform supervisor (Backend.Submit_long name) with
    | Ok (Client_call.In_flight { ticket; _ } as submitted) ->
        cells := (name, ticket) :: !cells;
        submitted
    | Ok (Client_call.Completed _) | Error _ -> failwith "long call was not in flight"
  in
  let first = submit "first" and second = submit "second" in
  let await submitted =
    Domain.spawn (fun () -> Client_call.await ~poll ~slice_ms:20 submitted)
  in
  let first_waiter = await first and second_waiter = await second in
  complete (List.assoc "first" !cells) (Ok (Bytes.of_string "first done"));
  check "first outcome" (Domain.join first_waiter = Ok "first done");
  check "shutdown" (Supervisor.shutdown supervisor = Ok ());
  check "second closed" (Domain.join second_waiter = Error Client_call.Closed)

(** A deadline that expires while its request waits in the mailbox behind a
    busy owner completes the call with the typed [deadline_exceeded] RPC
    failure, and the request is never submitted; a request with a live
    deadline queued alongside it is submitted normally. *)
let test_deadline_expires_while_queued () =
  let submitted = ref [] in
  let supervisor = Result.get_ok (Supervisor.create ~capacity:4 submitted) in
  let gate = create_gate () in
  let blocker =
    Domain.spawn (fun () -> Supervisor.perform supervisor (Backend.Block gate))
  in
  (* Give the owner time to enter [Block] before the deadline is fixed. *)
  Thread.delay 0.05;
  let short_deadline =
    Some (Client.rpc_deadline ~now_ns:(Bridge.monotonic_now_ns ()) ~timeout_ms:20L)
  in
  let long_deadline =
    Some (Client.rpc_deadline ~now_ns:(Bridge.monotonic_now_ns ()) ~timeout_ms:60_000L)
  in
  let expired =
    Domain.spawn (fun () ->
        call supervisor (Backend.Submit_short ("expired", short_deadline)))
  in
  let live =
    Domain.spawn (fun () ->
        call supervisor (Backend.Submit_short ("live", long_deadline)))
  in
  Thread.delay 0.1;
  open_gate gate;
  check "blocker" (Domain.join blocker = Ok ());
  (match Domain.join expired with
  | Error (`Failed error) ->
      check "expired deadline error" (error = Protocol_adapter.expired_rpc_deadline_error)
  | _ -> failwith "an expired queued request was not completed with its deadline");
  check "live deadline" (Domain.join live = Ok "live ok");
  check "shutdown" (Supervisor.shutdown supervisor = Ok ());
  check "only the live request was submitted" (!submitted = [ "live" ])

let () =
  test_long_calls_do_not_delay_short_calls ();
  test_completion_reaches_only_its_caller ();
  test_deadline_expires_while_queued ();
  print_endline "concurrent client call tests passed"
