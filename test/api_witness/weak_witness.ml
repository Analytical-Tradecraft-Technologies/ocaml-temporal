(* Negative fixture for the API witness gate (#1230). Each binding names a
   public value with an annotation that pins nothing; the gate must exit 3.
   It is only parsed, never compiled. *)
module T = Temporal

let _cancel : _ = T.Scope.cancel
let _map : 'a = T.Future.map
let _is_unit : 'a T.Codec.t -> bool = T.Codec.is_unit
