(** Domain-local construction scope; all observer policy belongs to its caller. *)

type t = {
  on_activation : (Native_worker_execution.activation_info -> unit) option;
  on_completion : (Native_worker_execution.activation_info -> unit) option;
}
(** Immutable callback selection captured by the workflow adapter. *)

(** Uninstrumented construction is the default for every Domain and thread. *)
let empty = { on_activation = None; on_completion = None }

(** Systhreads share Domain-local storage. A per-thread table prevents an
    unrelated constructor on the same Domain from capturing a fixture scope
    while its owner is blocked in native startup. Entries exist only inside
    [with_callbacks]; the mutex is never held while calling the constructor. *)
let scope = Domain.DLS.new_key (fun () -> (Mutex.create (), Hashtbl.create 2))

(** Serializes one bounded table operation, including exception cleanup. *)
let access body =
  let mutex, scopes = Domain.DLS.get scope in
  Mutex.lock mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock mutex) (fun () ->
      body scopes (Thread.id (Thread.self ())))

(** Snapshots this Domain's optional callbacks for one worker construction. *)
let current () =
  access (fun scopes thread -> Option.value (Hashtbl.find_opt scopes thread) ~default:empty)

(** Restores the previous callbacks even when construction fails or raises. *)
let with_callbacks callbacks create =
  let previous =
    access (fun scopes thread ->
        let previous = Hashtbl.find_opt scopes thread in
        Hashtbl.replace scopes thread callbacks;
        previous)
  in
  Fun.protect
    ~finally:(fun () ->
      access (fun scopes thread ->
          match previous with
          | None -> Hashtbl.remove scopes thread
          | Some callbacks -> Hashtbl.replace scopes thread callbacks))
    create
