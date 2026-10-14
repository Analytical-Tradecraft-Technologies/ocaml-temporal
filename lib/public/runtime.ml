(** Public facade over the private shared Core runtime (#832). Attachment
    accounting and the native lifecycle live in
    [Temporal_sdk_kernel.Shared_runtime]; this module validates arguments and
    translates its errors into the public vocabulary. *)

module Shared = Temporal_sdk_kernel.Shared_runtime

type t = Shared.t

(** Thin typed facade over [Temporal_base.Metrics_config], which owns the
    validation rules and the bridge document. *)
module Metrics = struct
  type t = Temporal_base.Metrics_config.t

  let prometheus ?global_tags ?counters_total_suffix ?unit_suffix
      ?durations_as_seconds ~bind_address () =
    Temporal_base.Metrics_config.prometheus ?global_tags ?counters_total_suffix
      ?unit_suffix ?durations_as_seconds ~bind_address ()
    |> Result.map_error (fun message ->
           Error.defect ~message:("Runtime.Metrics.prometheus: " ^ message))
end

let create ?io_threads ?metrics () =
  match Backend.validate_io_threads io_threads with
  | Error _ as error -> error
  | Ok () -> (
      let telemetry = Option.map Temporal_base.Metrics_config.to_json metrics in
      match Shared.create ?worker_threads:io_threads ?telemetry () with
      | Ok runtime -> Ok runtime
      | Error { Temporal_sdk_kernel.Bridge.message; _ } ->
          Error
            (Error.make ~category:`Bridge
               ~message:("runtime creation failed: " ^ message)
               ()))

let attached = Shared.attached
let metrics_address = Shared.metrics_address

let shutdown runtime =
  match Shared.shutdown runtime with
  | Ok () -> Ok ()
  | Error (Shared.Still_attached count) ->
      Error
        (Error.defect
           ~message:
             (Printf.sprintf
                "Runtime.shutdown: %d client(s) or worker(s) are still \
                 attached; shut them down first (a worker whose shutdown \
                 reported a detached native teardown stays attached until \
                 that teardown finishes)"
                count))
  | Error (Shared.Native { Temporal_sdk_kernel.Bridge.message; _ }) ->
      Error
        (Error.make ~category:`Bridge
           ~message:("runtime shutdown failed: " ^ message)
           ())
