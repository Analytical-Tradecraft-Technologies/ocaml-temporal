(** Exercises client request identity allocation across fresh processes and
    concurrent Domains without requiring a Temporal service. *)
module Id = Temporal_base.Client_request_id

(** Checks collision resistance across independent producers, without relying
    on the generator's prefix, encoding, or a particular random sequence. *)
let expect_distinct ids =
  let seen = Hashtbl.create (List.length ids) in
  List.iter
    (fun id ->
      if String.equal id "" || Hashtbl.mem seen id then
        failwith "independent client operations reused a request ID";
      Hashtbl.add seen id ())
    ids

(** Re-executes this test so initial state is not shared with the parent. *)
let subprocess_ids () =
  let input =
    Unix.open_process_args_in Sys.executable_name
      [| Sys.executable_name; "--emit-ids" |]
  in
  let ids = List.init 128 (fun _ -> input_line input) in
  match Unix.close_process_in input with
  | Unix.WEXITED 0 -> ids
  | _ -> failwith "request ID subprocess failed"

(** Runs generation from independently seeded caller states and concurrent
    Domains; caller Random.init must not reset the SDK's identity namespace. *)
let test_ids () =
  Random.init 7;
  let first = Id.create () in
  Random.init 7;
  let second = Id.create () in
  let domains =
    List.init 8 (fun _ ->
        Domain.spawn (fun () -> List.init 256 (fun _ -> Id.create ())))
  in
  let concurrent = List.concat_map Domain.join domains in
  expect_distinct
    (first :: second :: (concurrent @ subprocess_ids () @ subprocess_ids ()))

(** A helper mode exposes only freshly generated identifiers to the parent. *)
let () =
  if Array.length Sys.argv = 2 && Sys.argv.(1) = "--emit-ids" then
    for _ = 1 to 128 do print_endline (Id.create ()) done
  else test_ids ()
