(** Unit tests for the example workflow, written the way an application tests
    its own workflows: with [Temporal.Testing], in-process, without a Temporal
    Server. The workflow under test is the one the example workflow worker
    registers, including its durable 250 ms timer, which the environment
    skips in virtual time. *)

open Example_support.Definitions

(** Fails with [label] and the error message when [result] is an error. *)
let ok label = function
  | Ok value -> value
  | Error error -> failwith (label ^ ": " ^ Temporal.Error.message error)

(** Runs [name] through the example workflow in a fresh environment whose
    activity registrations are [activities], and always releases it. *)
let run_example ~activities name =
  let environment =
    ok "create environment"
      (Temporal.Testing.create
         ~workflows:[ Temporal.Testing.workflow local_compose_message ]
         ~activities ())
  in
  Fun.protect
    ~finally:(fun () -> Temporal.Testing.shutdown environment)
    (fun () -> Temporal.Testing.execute environment local_compose_message name)

(** The real activity implementation produces the documented client output. *)
let test_with_real_activity () =
  let message =
    ok "execute"
      (run_example
         ~activities:[ Temporal.Testing.activity local_render_message ]
         "Ada Lovelace")
  in
  if
    not
      (String.equal message
         "Hello, Ada Lovelace!\n\
          Next: review the Temporal result for Ada Lovelace.")
  then failwith ("unexpected message: " ^ message)

(** A stub replaces the activity the workflow schedules by its remote
    reference, isolating the workflow's own logic: concurrent scheduling and
    result order. *)
let test_with_stubbed_activity () =
  let message =
    ok "execute"
      (run_example
         ~activities:
           [
             Temporal.Testing.mock_activity remote_render_message (fun input ->
                 Ok ("<" ^ input ^ ">"));
           ]
         "Grace")
  in
  if not (String.equal message "<greeting:Grace>\n<next-step:Grace>") then
    failwith ("unexpected stubbed message: " ^ message)

(** A blank name fails the workflow with its business error. *)
let test_blank_name () =
  match run_example ~activities:[] "  " with
  | Ok _ -> failwith "a blank name unexpectedly succeeded"
  | Error error ->
      if not (String.equal (Temporal.Error.message error) "a name is required")
      then failwith ("unexpected failure: " ^ Temporal.Error.message error)

let () =
  test_with_real_activity ();
  test_with_stubbed_activity ();
  test_blank_name ()
