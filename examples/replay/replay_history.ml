(** The example application's offline replay checker.

    This is the only application-specific part of the replay command: it
    links the example application's workflow definitions and codecs from
    [Example_support] and hands their registrations to [Replay_command]. An
    application copies both files and replaces this list with the workflows,
    and the same signal, query, and update handlers, that its production
    worker registers. Activities are not listed because replay never runs
    them; their recorded results come from the history. *)

let () =
  Replay_command.main
    ~workflows:
      [
        Temporal.Replay.workflow
          Example_support.Definitions.local_compose_message;
      ]
