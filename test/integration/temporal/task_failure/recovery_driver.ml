(** Independent client holding the exact original handles across worker
    replacement. It cannot execute workflow code or follow another run. *)

(** Serializes only controlled fixture names and server-issued identities. *)
let identity name handle = Printf.sprintf "%s\t%s\t%s\n" name
    (Temporal.Client.workflow_id handle) (Temporal.Client.run_id handle)

(** Verifies typed public results, including deliberate application retryability. *)
let assert_outcome name = function
  | Temporal.Client.Completed "recovered" when List.mem name [ "body"; "encoder"; "missing" ] -> ()
  | Temporal.Client.Failed error when List.mem name [ "business-retryable"; "business-permanent" ] ->
      let view = Temporal.Error.view error in
      (* Native client diagnostics retain structured metadata after the original
         message; raw server history separately asserts the exact message. *)
      if view.category <> `Workflow ||
          view.non_retryable <> String.equal name "business-permanent" ||
          not (String.starts_with ~prefix:"intentional business failure " view.message) then
        failwith ("business failure contract changed: " ^ name)
  | _ -> failwith ("unexpected terminal outcome: " ^ name)

(** Starts every run before waiting, so missing-registration and encoder
    failures are observed alongside the body failure by the host controller. *)
let () =
  Failure_support.require_live ();
  let client = Failure_support.require (Temporal.Client.create
      ~target_url:(Sys.getenv "TEMPORAL_ADDRESS")
      ~namespace:(Sys.getenv "TEMPORAL_NAMESPACE") ~identity:"task-failure-client" ()) in
  Fun.protect ~finally:(fun () -> Failure_support.require (Temporal.Client.shutdown client))
    (fun () ->
      let handles = List.map (fun name ->
          let handle = Failure_support.require (Temporal.Client.start client
              ~workflow:(Failure_support.reference name) ~task_queue:Failure_support.task_queue
              ~id:("task-failure-" ^ name) ~input:() ()) in
          name, handle) Failure_support.cases in
      let identities = String.concat "" (List.map (fun (name, handle) -> identity name handle) handles) in
      Failure_support.publish "accepted.tsv" identities;
      List.iter (fun (name, handle) ->
          assert_outcome name (Failure_support.require (Temporal.Client.wait handle))) handles;
      Failure_support.publish "completed.tsv" identities)
