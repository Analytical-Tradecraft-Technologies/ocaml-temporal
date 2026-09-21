# Publication boundary for synthetic live fixtures. Unknown JSON fields are
# dropped, never copied through: raw Temporal payloads, memo, headers, search
# attributes, credentials and arbitrary failure messages have no schema here.
# The log boundary is intentionally conservative: after a sensitive record,
# the rest of that source is suppressed because unlabeled continuation lines
# cannot be reliably classified. Filtering precedes the rolling-tail limit.
def sensitive:
  test("(?i)(password|passwd|secret|token|authorization|credential|bearer|private.key|payload|(?:input|result|output)[\"'[:space:]]*[=:]|://[^ /]+@|(?:BEGIN|END) .*?(?:KEY|CERTIFICATE))")
  or test("^\\s*[A-Za-z0-9+/=]{24,}\\s*$");
def secret_values:
  [env | to_entries[]
   | select(.key | test("(?i)(token|password|passwd|secret|credential|api.key)"))
   | .value | split("\n")[] | select(length >= 4)];
def safe_text:
  if sensitive then "[redacted sensitive value]" else . end
  | reduce secret_values[] as $secret (. ; split($secret) | join("[redacted]"));

# Values are a constrained scalar vocabulary. Synthetic IDs are retained so
# executions can be correlated; unexpected strings are not useful evidence.
def scalar:
  if type == "string" then
    if length <= 180 and test("^[A-Za-z0-9_.:-]+$") then safe_text
    else "[redacted string]" end
  elif type == "number" or type == "boolean" or . == null then .
  else null end;

# Closed field union of the existing history/observer/controller contracts.
# This projection intentionally remains independent of their validators: a
# failed/partial document is evidence, never proof that the scenario passed.
def diagnostic:
  if type == "array" then
    if length > 256 then error("diagnostic array exceeds 256 records")
    else map(diagnostic) end
  elif type == "object" then
    with_entries(select(.key | test("^(workflow_id|run_id|parent_workflow_id|parent_run_id|child_workflow_id|child_run_id|counterpart_workflow_id|counterpart_run_id|legacy_workflow_id|legacy_run_id|new_workflow_id|new_run_id|removal_workflow_id|removal_run_id|events|records|parent|child|id|event_id|parent_initiated_event_id|type|phase|generation|is_replaying|history_length|reason|step|status|stage|role|scenario|outcome|container_id|exit_code|replacement_mode|shutdown_marker|event_count|initiated_event_id|started_event_id|worker_version|branch|marker_count|marker_deprecated|readiness_generation|fresh_container|temporal_healthy|stale_project_volumes_before_cleanup|remaining_project_volumes_before_start|remaining_project_volumes|remaining_worker_containers|truncated)$")))
    | with_entries(.value |= diagnostic)
  else scalar end;

if $mode == "json" then diagnostic
elif $mode == "phases" then
  # Exact machine phase lines are a separate closed projection, so conservative
  # suppression of a preceding free-text log cannot erase durable outcomes.
  reduce inputs as $line ({events:[],truncated:false};
    ($line | [capture("^(?:two-binary|cache eviction) phase=(?<phase>[a-z0-9_:-]+) status=(?<status>[a-z0-9_:-]+) workflow_id=(?<workflow_id>two-binary-[a-z0-9-]+) run_id=(?<run_id>[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})(?: duration_ms=[0-9.]+)?$")]) as $records
    | if (.events | length) + ($records | length) > 256 then .truncated = true
      else .events += $records end)
  | diagnostic
elif $mode == "stream" then
  secret_values as $secrets
  | foreach inputs as $line ({blocked:false,line:null};
      if .blocked then .line = null
      elif ($line | sensitive) or any($secrets[]; . as $secret | $line | contains($secret)) then
        .blocked = true | .line = "[redacted sensitive record and subsequent lines]"
      else .line = $line end;
      .line // empty)
else
  secret_values as $secrets
  | reduce inputs as $line ({blocked:false,lines:[]};
      if .blocked then .
      elif ($line | sensitive) or any($secrets[]; . as $secret | $line | contains($secret)) then
        .blocked = true | .lines += ["[redacted sensitive record and subsequent lines]"]
      else .lines += [$line[:768]] end
      | .lines = .lines[-64:])
  | .lines[]
end
