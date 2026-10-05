(* Completeness gate for the public API compatibility witness.

   [test/fixtures/install-consumer/public_api.ml] is only useful if it names
   every supported value: a value that the witness never mentions can change
   its type or disappear without failing any build.  This program derives the
   expected value set from the sources that define the public surface and
   fails, listing each omission, when the witness does not reference one.

   The expected set is computed from two inputs:
   - [lib/public/temporal.ml], whose [module X = Y] aliases are the public
     module allow-list (private implementation modules are absent from it);
   - [lib/public/<x>.mli] for each listed module, whose [val] declarations,
     including those inside nested [module M : sig ... end] declarations, are
     the supported values.

   The witness side is every identifier expression whose path starts with the
   [T] or [Temporal] module, for example [T.Activity.Context.heartbeat].  Only
   value references count; type paths, constructors, and record fields are
   checked by the witness's own type annotations instead.

   The program uses only the OCaml parser from [compiler-libs], through
   [Ast_iterator] hooks and [Longident.flatten], because those entry points are
   stable across the supported compiler series while the concrete Parsetree
   constructors are not.  Constructs it cannot resolve syntactically ([include]
   or a named module type in a public interface) are reported as errors rather
   than silently skipped, so an interface refactor cannot weaken the gate. *)

(** [fail fmt] prints a diagnostic to stderr and exits with status 2.  It is
    reserved for malformed inputs and unsupported interface constructs, which
    are distinct from the ordinary "witness is incomplete" failure (status 1). *)
let fail fmt =
  Printf.ksprintf
    (fun message ->
      prerr_endline ("check_api_witness: " ^ message);
      exit 2)
    fmt

(** [read_file path] returns the complete contents of [path] in binary mode,
    so CRLF checkouts on Windows do not change parser locations. *)
let read_file path =
  In_channel.with_open_bin path In_channel.input_all

(** [lexbuf_of_file path] creates a lexer buffer whose locations name [path],
    which keeps parser error messages actionable. *)
let lexbuf_of_file path =
  let lexbuf = Lexing.from_string (read_file path) in
  Lexing.set_filename lexbuf path;
  lexbuf

(** [parse parser path] runs a compiler-libs parser and turns a syntax error
    into a located diagnostic instead of an uncaught exception. *)
let parse parser path =
  try parser (lexbuf_of_file path)
  with exn ->
    Location.report_exception Format.err_formatter exn;
    fail "could not parse %s" path

(** [public_modules root_ml] returns the module names bound at the top level of
    the [Temporal] root module, in source order.  Only top-level bindings are
    collected; the root is expected to contain nothing but aliases. *)
let public_modules root_ml =
  let structure = parse Parse.implementation root_ml in
  let names = ref [] in
  let iterator =
    {
      Ast_iterator.default_iterator with
      module_binding =
        (fun _ binding ->
          match binding.pmb_name.txt with
          | Some name -> names := name :: !names
          | None -> ());
    }
  in
  iterator.structure iterator structure;
  List.rev !names

(** [interface_values ~module_name mli] returns the dotted path, relative to
    the [Temporal] root, of every value declared by [mli].  A [module_name]
    prefix is pushed for the file itself and for every nested module
    declaration, so [val heartbeat] inside [module Context] of
    [activity.mli] yields ["Activity.Context.heartbeat"]. *)
let interface_values ~module_name mli =
  let signature = parse Parse.interface mli in
  let path = ref [ module_name ] in
  let values = ref [] in
  let unsupported what (location : Location.t) =
    fail "%s:%d: %s is not supported by the API witness completeness check"
      location.loc_start.pos_fname location.loc_start.pos_lnum what
  in
  let iterator =
    {
      Ast_iterator.default_iterator with
      value_description =
        (fun _ description ->
          let name = description.pval_name.txt in
          values := String.concat "." (List.rev (name :: !path)) :: !values);
      module_declaration =
        (fun self declaration ->
          match declaration.pmd_name.txt with
          | None -> ()
          | Some name ->
              (* The stack discipline keeps sibling modules independent:
                 every push is matched by a pop after the nested signature is
                 visited, even though the iterator is otherwise stateless. *)
              path := name :: !path;
              Ast_iterator.default_iterator.module_declaration self declaration;
              path := List.tl !path);
      include_description =
        (fun _ include_ -> unsupported "include" include_.pincl_loc);
      module_type_declaration =
        (fun _ declaration ->
          unsupported "a module type declaration" declaration.pmtd_loc);
    }
  in
  iterator.signature iterator signature;
  List.rev !values

(** [witness_references witness_ml] returns the set of dotted value paths that
    the witness mentions through the [T] or [Temporal] root, with the root
    prefix removed so they compare directly with {!interface_values}. *)
let witness_references witness_ml =
  let structure = parse Parse.implementation witness_ml in
  let references = Hashtbl.create 256 in
  let iterator =
    {
      Ast_iterator.default_iterator with
      expr =
        (fun self expression ->
          (match expression.pexp_desc with
          | Pexp_ident { txt; _ } -> (
              match Longident.flatten txt with
              | ("T" | "Temporal") :: (_ :: _ :: _ as rest) ->
                  Hashtbl.replace references (String.concat "." rest) ()
              | _ -> ())
          | _ -> ());
          Ast_iterator.default_iterator.expr self expression);
    }
  in
  iterator.structure iterator structure;
  references

(** Entry point: [check_api_witness ROOT_ML PUBLIC_DIR WITNESS_ML].  Exits 0
    when every public value is referenced, 1 with a list of omissions
    otherwise, and 2 for unusable inputs. *)
let () =
  match Sys.argv with
  | [| _; root_ml; public_dir; witness_ml |] ->
      let expected =
        public_modules root_ml
        |> List.concat_map (fun module_name ->
               let mli =
                 Filename.concat public_dir
                   (String.uncapitalize_ascii module_name ^ ".mli")
               in
               if not (Sys.file_exists mli) then
                 fail "public module %s has no interface %s" module_name mli;
               interface_values ~module_name mli)
      in
      if expected = [] then fail "no public values found under %s" public_dir;
      let referenced = witness_references witness_ml in
      let missing =
        List.filter (fun value -> not (Hashtbl.mem referenced value)) expected
      in
      if missing <> [] then begin
        Printf.eprintf
          "%s does not reference %d public value(s):\n"
          witness_ml (List.length missing);
        List.iter (fun value -> Printf.eprintf "  Temporal.%s\n" value) missing;
        prerr_endline
          "Add a binding with an explicit type annotation for each value; see \
           docs/reference/api-stability.md.";
        exit 1
      end
  | _ ->
      fail "usage: %s ROOT_ML PUBLIC_DIR WITNESS_ML" Sys.executable_name
