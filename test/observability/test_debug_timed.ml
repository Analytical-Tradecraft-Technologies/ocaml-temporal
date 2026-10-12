(** Contract of [Observability.debug_timed], the opt-in codec timing used by
    the live fan-out benchmark (#528): it is invisible unless its source is
    at [Debug], and it never changes the wrapped result. *)
module Observability = Temporal_base.Observability

(** A private source, so the test cannot depend on SDK source levels. *)
let src = Logs.Src.create "temporal.test.debug_timed"

(** Operations and durations of every record [src] delivered, newest first. *)
let records = ref []

(** Captures [src] records with their tags; other sources are ignored. *)
let reporter =
  let report record_src level ~over continuation messagef =
    messagef (fun ?header:_ ?(tags = Logs.Tag.empty) format ->
        Format.ikfprintf
          (fun _ ->
            if record_src == src then (
              assert (level = Logs.Debug);
              records :=
                ( Logs.Tag.find Observability.Tag.operation tags,
                  Logs.Tag.find Observability.Tag.duration_ms tags )
                :: !records);
            over ();
            continuation ())
          Format.str_formatter format)
  in
  { Logs.report }

(** With the source below [Debug], the action runs exactly once and no
    record is emitted. *)
let test_disabled_runs_action_without_record () =
  Logs.Src.set_level src (Some Logs.Info);
  records := [];
  let calls = ref 0 in
  let value =
    Observability.debug_timed ~src ~operation:"disabled" (fun () ->
        incr calls;
        42)
  in
  assert (value = 42 && !calls = 1 && !records = [])

(** At [Debug], one record carries the operation and a valid duration. *)
let test_enabled_reports_operation_and_duration () =
  Logs.Src.set_level src (Some Logs.Debug);
  records := [];
  let value =
    Observability.debug_timed ~src ~operation:"enabled" (fun () -> "result")
  in
  assert (value = "result");
  match !records with
  | [ (Some "enabled", Some duration) ] ->
      assert (Float.is_finite duration && duration >= 0.)
  | _ -> failwith "expected exactly one tagged debug_timed record"

(** An exception propagates unchanged and leaves no record. *)
let test_exception_propagates_without_record () =
  Logs.Src.set_level src (Some Logs.Debug);
  records := [];
  (match
     Observability.debug_timed ~src ~operation:"raises" (fun () ->
         raise Exit)
   with
  | () -> failwith "expected the action's exception"
  | exception Exit -> ());
  assert (!records = [])

let () =
  Logs.set_reporter reporter;
  test_disabled_runs_action_without_record ();
  test_enabled_reports_operation_and_duration ();
  test_exception_propagates_without_record ()
