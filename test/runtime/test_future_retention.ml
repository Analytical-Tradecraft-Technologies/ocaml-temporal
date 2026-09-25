(** Retained summary futures must not keep decoded activity bodies alive. These
    probes use public unscoped activities with a live owner and measure live
    heap words, rather than RSS or cumulative allocations. *)
module Scheduler = Temporal_runtime.Scheduler
module Context = Temporal_runtime.Workflow_context_store
module Activation = Temporal_runtime.Activation

(** Payload large enough to distinguish a retained body from future metadata. *)
let body_size = 1024 * 1024

(** Real activity decoding exercises the production public future adapter. *)
let activity =
  Temporal.Activity.remote ~name:"future-retention"
    ~input:Temporal.Codec.unit ~output:Temporal.Codec.bytes

(** Unwraps a public operation, preserving its diagnostic on failure. *)
let public = function
  | Ok value -> value
  | Error error -> failwith (Temporal.Error.message error)

(** Unwraps synthetic activation setup and completion results. *)
let internal = function
  | Ok value -> value
  | Error error -> failwith (Temporal_base.Error.view error).message

(** Runs one workflow fiber and drains its callbacks without closing its owner. *)
let run scheduler context action =
  Scheduler.spawn scheduler (fun () -> Context.with_context context action);
  match Scheduler.run scheduler with
  | Scheduler.Failed error -> raise error
  | Scheduler.Complete | Scheduler.Blocked -> ()

(** Completes exactly one unscoped activity with a newly allocated byte body. *)
let complete context =
  let seq =
    match Context.take_commands context with
    | [ Activation.Schedule_activity command ] -> command.seq
    | _ -> failwith "expected one activity command"
  in
  let payload =
    internal
      (Temporal_base.Codec.encode Temporal_base.Codec.bytes
         (Bytes.make body_size 'x'))
  in
  internal (Context.resolve_activity context ~seq (Ok payload))

(** Full collections eliminate dead callback environments before measurement. *)
let live_words () =
  Gc.full_major ();
  Gc.full_major ();
  (Gc.stat ()).live_words

(** Compares a mapped path against same-owner small futures with no dependency
    on the source. Each batch retains more futures and checks that their values
    remain usable while every original body has become collectable. *)
let test_activity_summaries ~control =
  let scheduler = Scheduler.create () in
  let context = Context.create scheduler in
  let retained = ref [] in
  let weak = Weak.create 30 in
  let baseline = live_words () in
  for batch = 0 to 2 do
    for offset = 0 to 9 do
      let slot = batch * 10 + offset in
      run scheduler context (fun () ->
          let source =
            Temporal.Activity.future (Temporal.Activity.start_handle activity ())
          in
          let summary =
            if control then (
              complete context;
              let body = public (Temporal.Future.await source) in
              Weak.set weak slot (Some body);
              let size = Bytes.length body in
              Temporal.Future.map (fun _ -> size) (Temporal.Future.all []))
            else (
              let mapped =
                Temporal.Future.map
                  (fun body ->
                    Weak.set weak slot (Some body);
                    Bytes.length body)
                  source
              in
              complete context;
              mapped)
          in
          assert (public (Temporal.Future.await summary) = body_size);
          retained := summary :: !retained)
    done;
    let words = live_words () - baseline in
    let bodies = ref 0 in
    for slot = 0 to Weak.length weak - 1 do
      if Weak.check weak slot then incr bodies
    done;
    Printf.printf
      "future retention: control=%b completed=%d bodies=%d live_bytes=%d\n%!"
      control ((batch + 1) * 10) !bodies (words * (Sys.word_size / 8));
    assert (Scheduler.is_active scheduler);
    List.iter
      (fun summary -> assert (Temporal.Future.peek summary = Some (Ok body_size)))
      !retained;
    if !bodies <> 0 then failwith "retained summary kept decoded activity bodies";
    (* Allow far more than 30 small futures, but less than one 1 MiB body. *)
    if words > 65_536 then failwith "summary futures retained excessive live heap"
  done;
  Context.shutdown context

let () =
  test_activity_summaries ~control:true;
  test_activity_summaries ~control:false
