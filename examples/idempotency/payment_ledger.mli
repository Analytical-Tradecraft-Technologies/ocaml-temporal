(** A small file-backed payment ledger that stands in for an external system
    with an atomic idempotency contract (#504).

    Real payment providers, databases and message brokers offer the same
    contract under different names: an idempotency-key header, a unique
    constraint written in the same transaction as the effect, or a
    conditional write. Its essential property is that recording "key K is
    done" and performing the effect are one atomic step, so a repeated
    request with K cannot perform the effect twice, however many processes
    send it and whenever an earlier sender died.

    This ledger provides that contract with the file system. A ledger is a
    directory, and posting under key K publishes one completely written,
    synced record file named after K with a hard link ([link(2)]), which
    atomically fails when the name already exists. The first request for K
    therefore performs the posting, and every later request, from any
    process, gets the record of that first posting back. A directory is used
    so that worker processes, a replacement worker after a crash, and a
    checking driver can share the ledger across process boundaries without
    any other dependency. It is an example, not a production store: it
    assumes a local file system with hard links that fail atomically when
    the target exists (POSIX file systems and NTFS), and it is not designed
    for many concurrent writers or very many keys. *)

(** A ledger rooted at one directory. The value holds only the path; every
    operation reads or writes the directory, so separate processes that open
    the same directory share one ledger. *)
type t

(** The business content of one posting. Two distinct operations may carry
    identical postings (for example two equal charges), which is why the
    idempotency key, not the posting, identifies an operation. *)
type posting = { account : string; amount_cents : int }

(** Which activity attempt asked for a posting. It is recorded with every
    request so the ledger's files correlate each effect with the exact
    workflow run and attempt that caused it. [worker] is a label chosen by
    the application, such as a worker identity or process generation. *)
type correlation = {
  workflow_id : string;
  run_id : string;
  activity_id : string;
  attempt : int;
  worker : string;
}

(** One committed posting: its key, its content, and the request that made
    it. A record never changes after it is committed. *)
type record = { key : string; posting : posting; committed_by : correlation }

(** The result of one {!post} request. *)
type outcome =
  | Committed of record
      (** This request performed the posting; the record is the new one. *)
  | Duplicate of record
      (** An earlier request with the same key and an identical posting
          already performed it. The record is that earlier one, unchanged;
          nothing was posted again. This is the normal answer to a retried
          or redelivered request. *)
  | Conflict of record
      (** The key was already used for a different posting. Nothing was
          posted. This is an application defect: one key was given to two
          different operations. *)

(** The kind of an {!outcome}, as written to the request journal. *)
type outcome_kind = Committed_kind | Duplicate_kind | Conflict_kind

(** One line of the request journal: which request asked for which key and
    what the ledger answered. *)
type journal_entry = {
  journal_key : string;
  requested_by : correlation;
  answer : outcome_kind;
}

(** [open_ledger directory] returns the ledger stored in [directory],
    creating the directory (but not its parent) if it does not exist. An
    [Error] carries a description of the file-system failure. *)
val open_ledger : string -> (t, string) result

(** Returns the directory the ledger is stored in. *)
val directory : t -> string

(** [post ledger ~key posting correlation] performs [posting] under the
    idempotency [key] at most once.

    The record file is written and synced under a unique temporary name
    first and then published under the key's name with a hard link, which
    the file system performs atomically and refuses if the name exists. A
    request that wins the link has committed the posting; a request that
    loses it reads the published record and returns {!Duplicate} or
    {!Conflict}. A process that dies at any point leaves either no record or
    a complete one, never a partial one; at worst it leaves a temporary file
    behind, which is never read.

    After answering, the request is appended to the ledger's journal. The
    journal is an audit trail for diagnosis and tests, not part of the
    contract: a process that dies between publishing a record and appending
    its journal line leaves the record without that line. Appends from
    concurrent Domains and processes never lose or interleave lines; on
    Windows, where OCaml's [O_APPEND] is not atomic, they are made under an
    exclusive file lock.

    [key] must be non-empty. An [Error] reports an invalid key or a
    file-system failure; the request may then be retried with the same key,
    which is safe because the outcome is decided only by the atomic link. *)
val post : t -> key:string -> posting -> correlation -> (outcome, string) result

(** Returns the committed record for [key], if any. *)
val find : t -> key:string -> (record option, string) result

(** Returns every committed record, ordered by key. *)
val records : t -> (record list, string) result

(** Returns the request journal, oldest request first. *)
val journal : t -> (journal_entry list, string) result

(** Returns the record of a {!Committed}, {!Duplicate} or {!Conflict}
    outcome. *)
val outcome_record : outcome -> record

(** Returns the kind of an outcome. *)
val outcome_kind : outcome -> outcome_kind

(** Returns ["committed"], ["duplicate"] or ["conflict"]. *)
val string_of_outcome_kind : outcome_kind -> string

(** Renders a correlation as
    [workflow=W run=R activity=A attempt=N worker=X], for logs and test
    failure messages. *)
val describe_correlation : correlation -> string
