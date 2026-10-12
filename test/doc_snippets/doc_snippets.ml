(* Keeps the API documentation landing page honest (#1013).

   odoc renders [{[ ... ]}] code blocks verbatim and never type-checks them,
   and its [{!modules: ...}] index lists exactly the names written there. This
   program gives the build two checks of [lib/public/index.mld]:

   - [extract PAGE] prints every [{[ ... ]}] block of [PAGE], in order, as
     one OCaml compilation unit. Each block is preceded by a line directive,
     so a compiler error points at the line in [PAGE], not at the generated
     file. The dune rule compiles the output against the in-tree library.
     A page with no code block is an error, so a markup change cannot make
     the check vacuous.
   - [check-modules PAGE ROOT] fails, naming each omission, unless the
     page's [{!modules: ...}] index lists every [module X = ...] alias of the
     public root module [ROOT] ([lib/public/temporal.ml]) as [Temporal.X],
     and nothing else.

   Only the standard library is used, so the check builds before and
   independently of the SDK. *)

(** Reads a whole file as text. *)
let read_file path = In_channel.with_open_bin path In_channel.input_all

(** Splits text into lines, dropping a trailing carriage return so CRLF
    checkouts behave like LF ones. *)
let lines text =
  String.split_on_char '\n' text
  |> List.map (fun line ->
         let length = String.length line in
         if length > 0 && line.[length - 1] = '\r' then
           String.sub line 0 (length - 1)
         else line)

(** Prints the code blocks of [page] as one compilation unit. A block opens on
    a line that is exactly [{[] and closes on a line that is exactly [\]}];
    other verbatim forms are not code and are skipped. *)
let extract page =
  let blocks = ref 0 in
  let inside = ref false in
  List.iteri
    (fun index line ->
      let number = index + 1 in
      match (!inside, String.trim line) with
      | false, "{[" ->
          inside := true;
          incr blocks;
          Printf.printf "# %d %S\n" (number + 1) page
      | true, "]}" ->
          inside := false;
          print_newline ()
      | true, _ -> print_endline line
      | false, _ -> ())
    (lines (read_file page));
  if !inside then failwith (page ^ ": unterminated {[ code block");
  if !blocks = 0 then failwith (page ^ ": no {[ code blocks found")

(** Words of the first [{!modules: ...}] index in [page]. *)
let indexed_modules page =
  let text = read_file page in
  let marker = "{!modules:" in
  let rec find from =
    if from + String.length marker > String.length text then
      failwith (page ^ ": no {!modules: ...} index found")
    else if String.sub text from (String.length marker) = marker then from
    else find (from + 1)
  in
  let start = find 0 + String.length marker in
  let stop =
    match String.index_from_opt text start '}' with
    | Some stop -> stop
    | None -> failwith (page ^ ": unterminated {!modules: ...} index")
  in
  String.sub text start (stop - start)
  |> String.split_on_char '\n'
  |> List.concat_map (String.split_on_char ' ')
  |> List.map String.trim
  |> List.filter (fun word -> word <> "")

(** [Temporal.X] for every [module X = ...] line of the public root. *)
let public_modules root =
  lines (read_file root)
  |> List.filter_map (fun line ->
         match String.split_on_char ' ' (String.trim line) with
         | "module" :: name :: "=" :: _ -> Some ("Temporal." ^ name)
         | _ -> None)

(** Fails unless the page index and the public root list the same modules. *)
let check_modules page root =
  let indexed = indexed_modules page in
  let public = public_modules root in
  if public = [] then failwith (root ^ ": no module aliases found");
  let missing = List.filter (fun name -> not (List.mem name indexed)) public in
  let unknown = List.filter (fun name -> not (List.mem name public)) indexed in
  List.iter
    (fun name -> Printf.eprintf "%s: {!modules: ...} omits %s\n" page name)
    missing;
  List.iter
    (fun name ->
      Printf.eprintf "%s: {!modules: ...} lists %s, which %s does not export\n"
        page name root)
    unknown;
  if missing <> [] || unknown <> [] then exit 1

let () =
  match Array.to_list Sys.argv with
  | [ _; "extract"; page ] -> extract page
  | [ _; "check-modules"; page; root ] -> check_modules page root
  | _ ->
      prerr_endline
        "usage: doc_snippets extract PAGE | doc_snippets check-modules PAGE ROOT";
      exit 2
