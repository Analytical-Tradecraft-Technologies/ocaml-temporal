(** Public facade over the private shared Core runtime (#832). Attachment
    accounting and the native lifecycle live in
    [Temporal_sdk_kernel.Shared_runtime]; this module validates arguments and
    translates its errors into the public vocabulary. *)

module Shared = Temporal_sdk_kernel.Shared_runtime

type t = Shared.t

let create ?io_threads () =
  match Backend.validate_io_threads io_threads with
  | Error _ as error -> error
  | Ok () -> (
      match Shared.create ?worker_threads:io_threads () with
      | Ok runtime -> Ok runtime
      | Error { Temporal_sdk_kernel.Bridge.message; _ } ->
          Error
            (Error.make ~category:`Bridge
               ~message:("runtime creation failed: " ^ message)
               ()))

let attached = Shared.attached

let shutdown runtime =
  match Shared.shutdown runtime with
  | Ok () -> Ok ()
  | Error (Shared.Still_attached count) ->
      Error
        (Error.defect
           ~message:
             (Printf.sprintf
                "Runtime.shutdown: %d client(s) or worker(s) are still \
                 attached; shut them down first"
                count))
  | Error (Shared.Native { Temporal_sdk_kernel.Bridge.message; _ }) ->
      Error
        (Error.make ~category:`Bridge
           ~message:("runtime shutdown failed: " ^ message)
           ())
