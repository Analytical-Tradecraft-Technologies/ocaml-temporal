module Bridge = Temporal_core_bridge.Native_bridge

type raw_response
(** Direct private C primitives let this test share one result owner between
    Domains. The public bridge normally decodes and frees it in one call. *)

external echo_raw : bytes -> raw_response = "ocaml_temporal_echo"

external check_abi_version_raw : int32 -> raw_response
  = "ocaml_temporal_check_abi_version"

external response_status : raw_response -> int
  = "ocaml_temporal_response_status"

external response_value : raw_response -> bytes
  = "ocaml_temporal_response_value"

external response_error : raw_response -> string
  = "ocaml_temporal_response_error"

external response_free : raw_response -> unit = "ocaml_temporal_response_free"

(** The only acceptable failed read after another Domain closes the owner. *)
let freed_message = "Temporal native response has already been freed"

(** Check that a closed result cannot expose either its status or Rust bytes. *)
let expect_freed read =
  match read () with
  | exception Invalid_argument message when String.equal message freed_message
    ->
      ()
  | exception error -> raise error
  | _ -> failwith "a freed native response remained readable"

(** Race a byte copy against explicit release. A completed copy must contain
    every original byte; if free wins, the binding must report the closed owner.
    Both outcomes are valid, and the deterministic post-free read below verifies
    the gate after each scheduling order. *)
let race_copy_with_free ~read ~check response =
  let start = Atomic.make false in
  let reader =
    Domain.spawn (fun () ->
        while not (Atomic.get start) do
          Domain.cpu_relax ()
        done;
        match read response with
        | copied -> Some copied
        | exception Invalid_argument message
          when String.equal message freed_message ->
            None)
  in
  let freer =
    Domain.spawn (fun () ->
        while not (Atomic.get start) do
          Domain.cpu_relax ()
        done;
        response_free response)
  in
  Atomic.set start true;
  let copied = Domain.join reader in
  Domain.join freer;
  Option.iter check copied;
  expect_freed (fun () -> response_status response);
  expect_freed (fun () -> read response);
  response_free response

(** The native result ABI represents an empty Rust allocation as [{ NULL, 0 }].
    This test exercises the OCaml copy path for that exact representation,
    rather than only checking the Rust-side buffer helper. *)
let () =
  match Bridge.echo Bytes.empty with
  | Ok value ->
      assert (Bytes.length value = 0);
      assert (Bytes.equal value Bytes.empty)
  | Error error ->
      failwith
        (Printf.sprintf "empty native response was rejected: %s" error.message)

(** Successful Rust-owned buffers are large enough to keep the copy/free
    interleaving observable without retaining many simultaneous allocations. *)
let () =
  let payload =
    Bytes.init (256 * 1024) (fun index -> Char.chr (index land 255))
  in
  let response = echo_raw payload in
  assert (Bytes.equal (response_value response) payload);
  response_free response;
  expect_freed (fun () -> response_value response);
  for _iteration = 1 to 24 do
    let response = echo_raw payload in
    assert (response_status response = 0);
    race_copy_with_free ~read:response_value
      ~check:(fun copied -> assert (Bytes.equal copied payload))
      response
  done

(** Error buffers have the same lifetime gate as successful value buffers. *)
let () =
  let response = check_abi_version_raw 0l in
  assert (String.length (response_error response) > 0);
  response_free response;
  expect_freed (fun () -> response_error response);
  for _iteration = 1 to 24 do
    let response = check_abi_version_raw 0l in
    assert (response_status response <> 0);
    race_copy_with_free ~read:response_error
      ~check:(fun copied -> assert (String.length copied > 0))
      response
  done
