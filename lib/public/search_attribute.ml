(** Implements typed search-attribute keys over the raw payload lists the
    client and workflow APIs already carry (#1154). Every value is encoded
    with the standard [json/plain] codecs, so the representation matches the
    one other Temporal SDKs write and Temporal Server decodes. *)

type kind =
  [ `Text | `Keyword | `Int | `Double | `Bool | `Datetime | `Keyword_list ]

(** Ties each indexed type to the OCaml type of its values. The GADT lets
    {!encode} and {!decode} select the JSON shape from the key alone, so a
    value can never be encoded with another type's representation. *)
type _ witness =
  | Text : string witness
  | Keyword : string witness
  | Int : int64 witness
  | Double : float witness
  | Bool : bool witness
  | Datetime : Time.t witness
  | Keyword_list : string list witness

type 'a key = { name : string; witness : 'a witness }

let text name = { name; witness = Text }
let keyword name = { name; witness = Keyword }
let int64 name = { name; witness = Int }
let double name = { name; witness = Double }
let bool name = { name; witness = Bool }
let datetime name = { name; witness = Datetime }
let keyword_list name = { name; witness = Keyword_list }
let name key = key.name

(** Erases the value type of a witness for the public {!kind}. *)
let kind_of_witness : type a. a witness -> kind = function
  | Text -> `Text
  | Keyword -> `Keyword
  | Int -> `Int
  | Double -> `Double
  | Bool -> `Bool
  | Datetime -> `Datetime
  | Keyword_list -> `Keyword_list

let kind key = kind_of_witness key.witness

(** The name Temporal Server writes in a search-attribute payload's ["type"]
    metadata for each indexed type (the [IndexedValueType] enum without its
    prefix), as the Go, Python, and TypeScript SDKs also write it. *)
let type_name : kind -> string = function
  | `Text -> "Text"
  | `Keyword -> "Keyword"
  | `Int -> "Int"
  | `Double -> "Double"
  | `Bool -> "Bool"
  | `Datetime -> "Datetime"
  | `Keyword_list -> "KeywordList"

(** Every name {!type_name} produces, so {!decode} can tell a conflicting
    known type from metadata it does not recognize. *)
let known_type_names =
  List.map type_name
    [ `Text; `Keyword; `Int; `Double; `Bool; `Datetime; `Keyword_list ]

(** A key and value whose types agree, hidden behind an existential so
    updates of different types share one list. *)
type update = Set : 'a key * 'a -> update | Unset : 'a key -> update

let set key value = Set (key, value)
let unset key = Unset key

(** Days from 1970-01-01 to the proleptic Gregorian date [year-month-day]
    (Howard Hinnant's [days_from_civil]). Integer-only, so encoding a
    datetime never depends on the host time zone or C library. *)
let days_from_civil ~year ~month ~day =
  let year = if month <= 2 then year - 1 else year in
  let era = (if year >= 0 then year else year - 399) / 400 in
  let year_of_era = year - (era * 400) in
  let day_of_year =
    (((153 * (if month > 2 then month - 3 else month + 9)) + 2) / 5) + day - 1
  in
  let day_of_era =
    (year_of_era * 365) + (year_of_era / 4) - (year_of_era / 100) + day_of_year
  in
  (era * 146_097) + day_of_era - 719_468

(** Inverse of {!days_from_civil}: the [(year, month, day)] of a day count
    from 1970-01-01. *)
let civil_from_days days =
  let days = days + 719_468 in
  let era = (if days >= 0 then days else days - 146_096) / 146_097 in
  let day_of_era = days - (era * 146_097) in
  let year_of_era =
    (day_of_era - (day_of_era / 1460) + (day_of_era / 36_524)
    - (day_of_era / 146_096))
    / 365
  in
  let day_of_year =
    day_of_era - ((365 * year_of_era) + (year_of_era / 4) - (year_of_era / 100))
  in
  let month_index = ((5 * day_of_year) + 2) / 153 in
  let day = day_of_year - (((153 * month_index) + 2) / 5) + 1 in
  let month = if month_index < 10 then month_index + 3 else month_index - 9 in
  let year = year_of_era + (era * 400) + if month <= 2 then 1 else 0 in
  (year, month, day)

(** Unix seconds of 0001-01-01T00:00:00Z and 9999-12-31T23:59:59Z, the range
    RFC 3339's four-digit year can represent. *)
let min_datetime_seconds = -62_135_596_800L

let max_datetime_seconds = 253_402_300_799L

(** Formats an instant as an RFC 3339 UTC timestamp, the [time.RFC3339Nano]
    shape Temporal Server parses: trailing zeros of the fraction are dropped
    and a whole second has no fraction. *)
let format_datetime time =
  let seconds = Time.seconds time in
  if
    Int64.compare seconds min_datetime_seconds < 0
    || Int64.compare seconds max_datetime_seconds > 0
  then Error (Error.codec ~message:"datetime is outside years 1 to 9999")
  else
    let seconds = Int64.to_int seconds in
    (* Floor division keeps the time of day non-negative before 1970. *)
    let days = if seconds >= 0 then seconds / 86_400 else ((seconds + 1) / 86_400) - 1 in
    let second_of_day = seconds - (days * 86_400) in
    let year, month, day = civil_from_days days in
    let fraction =
      match Time.nanoseconds time with
      | 0 -> ""
      | nanoseconds ->
          let digits = Printf.sprintf "%09d" nanoseconds in
          let length = ref 9 in
          while digits.[!length - 1] = '0' do decr length done;
          "." ^ String.sub digits 0 !length
    in
    Ok
      (Printf.sprintf "%04d-%02d-%02dT%02d:%02d:%02d%sZ" year month day
         (second_of_day / 3600) (second_of_day / 60 mod 60) (second_of_day mod 60)
         fraction)

(** Parses an RFC 3339 timestamp: [YYYY-MM-DDTHH:MM:SS], an optional
    fraction of up to nine digits, and [Z] or a [+HH:MM]/[-HH:MM] offset.
    [T] and [Z] may be lower case, as RFC 3339 allows. A leap second, an
    invalid calendar date, or any other shape is a codec error. *)
let parse_datetime text =
  let invalid () =
    Error (Error.codec ~message:("invalid RFC 3339 datetime: " ^ text))
  in
  let length = String.length text in
  let digits offset count =
    if offset + count > length then None
    else
      let rec loop index value =
        if index = count then Some value
        else
          match text.[offset + index] with
          | '0' .. '9' as digit ->
              loop (index + 1) ((value * 10) + Char.code digit - Char.code '0')
          | _ -> None
      in
      loop 0 0
  in
  let at offset expected =
    offset < length && List.mem text.[offset] expected
  in
  let leap year = (year mod 4 = 0 && year mod 100 <> 0) || year mod 400 = 0 in
  let days_in_month year = function
    | 2 -> if leap year then 29 else 28
    | 4 | 6 | 9 | 11 -> 30
    | _ -> 31
  in
  match (digits 0 4, digits 5 2, digits 8 2, digits 11 2, digits 14 2, digits 17 2) with
  | Some year, Some month, Some day, Some hour, Some minute, Some second
    when at 4 [ '-' ] && at 7 [ '-' ] && at 10 [ 'T'; 't' ] && at 13 [ ':' ]
         && at 16 [ ':' ] && year >= 1 && month >= 1 && month <= 12 && day >= 1
         && day <= days_in_month year month && hour <= 23 && minute <= 59
         && second <= 59 -> (
      (* The fraction, if any, runs from offset 19 to the first non-digit. *)
      let fraction_end =
        if at 19 [ '.' ] then
          let rec scan index =
            if index < length && text.[index] >= '0' && text.[index] <= '9' then
              scan (index + 1)
            else index
          in
          scan 20
        else 19
      in
      let fraction_digits = fraction_end - 20 in
      let nanoseconds =
        if fraction_end = 19 then Some 0
        else if fraction_digits < 1 || fraction_digits > 9 then None
        else
          Option.map
            (fun value ->
              let rec scale value count =
                if count = 9 then value else scale (value * 10) (count + 1)
              in
              scale value fraction_digits)
            (digits 20 fraction_digits)
      in
      let offset_seconds =
        if fraction_end = length - 1 && at fraction_end [ 'Z'; 'z' ] then Some 0
        else if fraction_end + 6 = length && at fraction_end [ '+'; '-' ]
                && at (fraction_end + 3) [ ':' ] then
          match (digits (fraction_end + 1) 2, digits (fraction_end + 4) 2) with
          | Some hours, Some minutes when hours <= 23 && minutes <= 59 ->
              let magnitude = (hours * 3600) + (minutes * 60) in
              Some (if text.[fraction_end] = '-' then -magnitude else magnitude)
          | _ -> None
        else None
      in
      match (nanoseconds, offset_seconds) with
      | Some nanoseconds, Some offset_seconds ->
          let local =
            (days_from_civil ~year ~month ~day * 86_400)
            + (hour * 3600) + (minute * 60) + second
          in
          Time.of_unix ~seconds:(Int64.of_int (local - offset_seconds)) ~nanoseconds
      | _ -> invalid ())
  | _ -> invalid ()

(** Prefixes [error]'s message with the attribute name so a failure in a
    list of updates identifies its entry, keeping the error's category. *)
let for_key name error =
  let view = Error.view error in
  Error.make ~category:view.category ~non_retryable:view.non_retryable
    ~message:(Printf.sprintf "search attribute %S: %s" name view.message)
    ()

(** Encodes one value with the [json/plain] codec of its type. *)
let encode_value : type a. a witness -> a -> (Payload.t, Error.t) result =
 fun witness value ->
  match witness with
  | Text -> Codec.encode Codec.string value
  | Keyword -> Codec.encode Codec.string value
  | Int -> Codec.encode Codec.int64 value
  | Double -> Codec.encode Codec.float value
  | Bool -> Codec.encode Codec.bool value
  | Datetime ->
      Result.bind (format_datetime value) (Codec.encode Codec.string)
  | Keyword_list ->
      Codec.encode Codec.json (`List (List.map (fun item -> `String item) value))

(** Rejects a name that cannot cross the bridge as a payload-map key, with
    the same rules as every other Temporal identifier in this SDK. *)
let validate_name name =
  if String.equal name "" then
    Error (Error.defect ~message:"search-attribute name is empty")
  else if String.contains name '\000' then
    Error (Error.defect ~message:"search-attribute name contains a NUL byte")
  else if String.length name > 65_536 then
    Error (Error.defect ~message:"search-attribute name exceeds 65536 bytes")
  else if not (Temporal_base.Codec.valid_utf_8 name) then
    Error (Error.defect ~message:"search-attribute name must be valid UTF-8")
  else Ok ()

(** The canonical removal payload: exactly [binary/null] metadata and no
    data, which Temporal Server compares byte for byte when it merges an
    upsert, so no ["type"] entry is added. *)
let removal () = { Payload.metadata = [ ("encoding", "binary/null") ]; data = Bytes.empty }

let encode updates =
  let rec loop seen encoded = function
    | [] -> Ok (List.rev encoded)
    | update :: rest -> (
        let name, payload =
          match update with
          | Set (key, value) ->
              ( key.name,
                Result.map
                  (fun (payload : Payload.t) ->
                    {
                      payload with
                      Payload.metadata =
                        payload.metadata
                        @ [ ("type", type_name (kind_of_witness key.witness)) ];
                    })
                  (encode_value key.witness value) )
          | Unset key -> (key.name, Ok (removal ()))
        in
        match validate_name name with
        | Error error -> Error error
        | Ok () when List.mem name seen ->
            Error
              (Error.defect
                 ~message:(Printf.sprintf "search attribute %S is repeated" name))
        | Ok () -> (
            match payload with
            | Error error -> Error (for_key name error)
            | Ok payload -> loop (name :: seen) ((name, payload) :: encoded) rest))
  in
  loop [] [] updates

(** Decodes one present value with the codec of its type. *)
let decode_value : type a. a witness -> Payload.t -> (a, Error.t) result =
 fun witness payload ->
  match witness with
  | Text -> Codec.decode Codec.string payload
  | Keyword -> Codec.decode Codec.string payload
  | Int -> Codec.decode Codec.int64 payload
  | Double -> Codec.decode Codec.float payload
  | Bool -> Codec.decode Codec.bool payload
  | Datetime -> Result.bind (Codec.decode Codec.string payload) parse_datetime
  | Keyword_list ->
      Result.bind (Codec.decode Codec.json payload) (function
        | `List items ->
            List.fold_right
              (fun item result ->
                match (item, result) with
                | `String value, Ok values -> Ok (value :: values)
                | _, (Error _ as error) -> error
                | _, Ok _ ->
                    Error (Error.codec ~message:"keyword list holds a non-string value"))
              items (Ok [])
        | _ -> Error (Error.codec ~message:"payload is not a JSON array"))

let decode key attributes =
  match List.assoc_opt key.name attributes with
  | None -> Ok None
  | Some (payload : Payload.t) -> (
      let metadata name = List.assoc_opt name payload.metadata in
      let expected = type_name (kind_of_witness key.witness) in
      match (metadata "encoding", metadata "type") with
      | Some "binary/null", _ -> Ok None
      | _, Some actual
        when List.mem actual known_type_names && not (String.equal actual expected) ->
          Error
            (for_key key.name
               (Error.codec
                  ~message:
                    (Printf.sprintf "payload has type %S, expected %S" actual expected)))
      | _ -> (
          match decode_value key.witness payload with
          | Ok value -> Ok (Some value)
          | Error error -> Error (for_key key.name error)))
