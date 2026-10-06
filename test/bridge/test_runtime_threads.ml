(** Regression for #832: the Tokio worker-thread bound of each native runtime
    is configurable, validated as a typed result before anything is allocated,
    and accepted identically by every backend. The native runtime cases create
    and close a real Core runtime without network access; the public cases use
    the [mock://] backend and an unreachable [http://] target that must be
    rejected before any connection attempt. *)

module Bridge = Temporal_core_bridge.Native_bridge

(** Fails the test with [label] unless [result] is [Ok]. *)
let expect_ok label = function
  | Ok value -> value
  | Error (error : Bridge.error) ->
      failwith (Printf.sprintf "%s failed: %s" label error.message)

(** Asserts that a bridge call failed with [Invalid_argument]. *)
let expect_invalid_argument label = function
  | Error { Bridge.status = Bridge.Invalid_argument; message } ->
      assert (String.length message > 0)
  | Error { Bridge.message; _ } ->
      failwith (Printf.sprintf "%s: unexpected error %s" label message)
  | Ok _ -> failwith (Printf.sprintf "%s: invalid count was accepted" label)

(** Asserts that a public call returned a [`Defect] naming [runtime_threads]. *)
let expect_defect label = function
  | Error error ->
      let view = Temporal.Error.view error in
      if view.category <> `Defect then
        failwith (Printf.sprintf "%s: expected defect, got %s" label view.message);
      assert (String.starts_with ~prefix:"runtime_threads must be" view.message)
  | Ok _ -> failwith (Printf.sprintf "%s: invalid count was accepted" label)

(** Out-of-range counts used by every rejection case. *)
let invalid_counts = [ 0; -1; Bridge.max_runtime_worker_threads + 1; max_int ]

(** The bridge-level validator accepts exactly [None] and [1..max]. *)
let test_bridge_validation () =
  List.iter
    (fun count -> expect_ok "valid count" (Bridge.validate_runtime_worker_threads count))
    [ None; Some 1; Some Bridge.max_runtime_worker_threads ];
  List.iter
    (fun count ->
      expect_invalid_argument "validator"
        (Bridge.validate_runtime_worker_threads (Some count)))
    invalid_counts

(** A real runtime is created and released with an explicit and a default
    count, while an invalid count fails before allocating the runtime. *)
let test_native_runtime_creation () =
  List.iter
    (fun worker_threads ->
      let runtime =
        expect_ok "runtime creation" (Bridge.runtime_create ?worker_threads ())
      in
      expect_ok "runtime close" (Bridge.runtime_close runtime))
    [ None; Some 1; Some 2 ];
  List.iter
    (fun count ->
      expect_invalid_argument "runtime creation"
        (Bridge.runtime_create ~worker_threads:count ()))
    invalid_counts

(** A trivial workflow so a worker registration list is non-empty. *)
let workflow =
  Temporal.Workflow.define ~name:"unit.runtime-threads"
    ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () -> Ok ())

(** Creates a mock or native worker with [runtime_threads]. *)
let create_worker ~target_url runtime_threads =
  Temporal.Worker.create ?runtime_threads ~target_url ~namespace:"unit-test"
    ~task_queue:"unit-test"
    ~workflows:[ Temporal.Worker.workflow workflow ]
    ~activities:[] ()

(** Public constructors validate the bound for every target. A valid count is
    accepted by the mock backend, and an invalid one is a typed defect even
    for a native target that is never contacted. *)
let test_public_validation () =
  let client =
    match
      Temporal.Client.create ~runtime_threads:2 ~target_url:"mock://threads"
        ~namespace:"unit-test" ()
    with
    | Ok client -> client
    | Error error -> failwith (Temporal.Error.message error)
  in
  (match Temporal.Client.shutdown client with
  | Ok () -> ()
  | Error error -> failwith (Temporal.Error.message error));
  (match create_worker ~target_url:"mock://threads" (Some 2) with
  | Ok worker -> (
      match Temporal.Worker.shutdown worker with
      | Ok () -> ()
      | Error error -> failwith (Temporal.Error.message error))
  | Error error -> failwith (Temporal.Error.message error));
  List.iter
    (fun target_url ->
      List.iter
        (fun count ->
          expect_defect "client"
            (Temporal.Client.create ~runtime_threads:count ~target_url
               ~namespace:"unit-test" ());
          expect_defect "worker" (create_worker ~target_url (Some count)))
        invalid_counts)
    [ "mock://threads"; "http://127.0.0.1:1" ]

let () =
  test_bridge_validation ();
  test_native_runtime_creation ();
  test_public_validation ()
