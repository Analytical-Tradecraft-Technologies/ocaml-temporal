TEMPORAL_FIXTURE_DIR := $(CURDIR)/test/integration/temporal
TEMPORAL_COMPOSE_FILE := $(TEMPORAL_FIXTURE_DIR)/compose.yaml
TEMPORAL_COMPOSE_PROJECT ?= ocaml-temporal-integration
COMPOSE := docker compose --project-directory "$(TEMPORAL_FIXTURE_DIR)" --file "$(TEMPORAL_COMPOSE_FILE)" --project-name "$(TEMPORAL_COMPOSE_PROJECT)"
# Compose services bind-mount the repository. Propagate the invoking user's
# numeric identity, selected OCaml image, and bounded driver timeout so every
# service shares Dune's lock/build ownership and overrides behave predictably.
TEMPORAL_COMPOSE = OCAML_IMAGE=$(OCAML_IMAGE) HOST_UID=$(HOST_UID) HOST_GID=$(HOST_GID) SMOKE_DRIVER_TIMEOUT_SECONDS=$(TEMPORAL_DRIVER_TIMEOUT_SECONDS) SMOKE_CACHE_EVICTION_TIMEOUT_SECONDS=$(SMOKE_CACHE_EVICTION_TIMEOUT_SECONDS) SMOKE_WORKER_GENERATION=$(SMOKE_WORKER_GENERATION) SMOKE_WORKER_MAX_CACHED_WORKFLOWS=$(SMOKE_WORKER_MAX_CACHED_WORKFLOWS) SMOKE_WORKER_CACHE_EVICTION_FILE=$(SMOKE_WORKER_CACHE_EVICTION_FILE) SMOKE_WORKER_CACHE_EVICTION_READY_FILE=$(SMOKE_WORKER_CACHE_EVICTION_READY_FILE) SMOKE_WORKER_CACHE_EVICTION_SECOND_READY_FILE=$(SMOKE_WORKER_CACHE_EVICTION_SECOND_READY_FILE) SMOKE_CACHE_EVICTION_SECOND_WORKFLOW_ID=$(SMOKE_CACHE_EVICTION_SECOND_WORKFLOW_ID) SMOKE_REPLAY_WORKFLOW_ID=$(SMOKE_REPLAY_WORKFLOW_ID) $(COMPOSE) --profile temporal
# Keep the one-shot acceptance driver bounded while allowing a temporarily
# stalled CI PostgreSQL checkpoint to finish. This is a process-level guard,
# not a workflow timeout; callers can still override it for slower machines.
TEMPORAL_DRIVER_TIMEOUT_SECONDS ?= 300
# The cache driver has an isolated Dune build directory because its executable
# must compile while the long-lived worker holds its own Dune lock. A cold Rust
# bridge build can consume the normal driver budget before the executable starts.
SMOKE_CACHE_EVICTION_TIMEOUT_SECONDS ?= 900
# Parent/child recovery also builds its client in an isolated Dune tree while
# the first worker remains alive. Give a cold bridge build plus the durable
# child timer a process-level budget independent of the ordinary smoke driver.
SMOKE_PARENT_CHILD_RESTART_TIMEOUT_SECONDS ?= 900
SMOKE_DRIVER_LOG_FILE := $(TEMPORAL_FIXTURE_DIR)/.smoke-driver.log
SMOKE_CANCELLATION_READY_FILE := $(TEMPORAL_FIXTURE_DIR)/.cancellation-ready
SMOKE_WORKER_STOPPED_FILE := $(TEMPORAL_FIXTURE_DIR)/.worker-stopped
SMOKE_REPLAY_DIAGNOSTICS_FILE := $(TEMPORAL_FIXTURE_DIR)/.restart-replay-diagnostics.json
SMOKE_RESTART_ACCEPTED_FILE := $(TEMPORAL_FIXTURE_DIR)/.restart-replay-accepted
SMOKE_RESTART_RESULT_FILE := $(TEMPORAL_FIXTURE_DIR)/.restart-replay-result
SMOKE_RESTART_DRIVER_LOG_FILE := $(TEMPORAL_FIXTURE_DIR)/.restart-replay-driver.log
SMOKE_RESTART_INITIAL_HISTORY := $(TEMPORAL_FIXTURE_DIR)/.restart-replay-history.initial.json
SMOKE_RESTART_TERMINAL_HISTORY := $(TEMPORAL_FIXTURE_DIR)/.restart-replay-history.terminal.json
SMOKE_RESTART_CONTROLLER_FILE := $(TEMPORAL_FIXTURE_DIR)/.restart-replay-controller.json
SMOKE_CACHE_EVICTION_FILE := $(TEMPORAL_FIXTURE_DIR)/.cache-eviction.json
SMOKE_CACHE_EVICTION_READY_FILE := $(TEMPORAL_FIXTURE_DIR)/.cache-eviction-ready
SMOKE_CACHE_EVICTION_SECOND_READY_FILE := $(TEMPORAL_FIXTURE_DIR)/.cache-eviction-second-ready
SMOKE_CACHE_EVICTION_CONTAINER_FILE := /workspace/test/integration/temporal/.cache-eviction.json
SMOKE_CACHE_EVICTION_READY_CONTAINER_FILE := /workspace/test/integration/temporal/.cache-eviction-ready
SMOKE_CACHE_EVICTION_SECOND_READY_CONTAINER_FILE := /workspace/test/integration/temporal/.cache-eviction-second-ready
SMOKE_CACHE_EVICTION_DRIVER_LOG_FILE := $(TEMPORAL_FIXTURE_DIR)/.cache-eviction-driver.log
SMOKE_DRIVER_CONTAINER := $(TEMPORAL_COMPOSE_PROJECT)-smoke-driver
SMOKE_RESTART_DRIVER_CONTAINER := $(TEMPORAL_COMPOSE_PROJECT)-smoke-restart-driver
SMOKE_WORKER_GENERATION ?= 1
SMOKE_WORKER_MAX_CACHED_WORKFLOWS ?=
SMOKE_WORKER_CACHE_EVICTION_FILE ?=
SMOKE_WORKER_CACHE_EVICTION_READY_FILE ?=
SMOKE_WORKER_CACHE_EVICTION_SECOND_READY_FILE ?=
SMOKE_CACHE_EVICTION_SECOND_WORKFLOW_ID ?=
SMOKE_REPLAY_WORKFLOW_ID ?= two-binary-worker-restart-replay
SERVICE ?= dev
OCAML_VERSION ?= 5.2
OCAML_IMAGE ?= ocaml/opam:debian-12-ocaml-$(OCAML_VERSION)
HOST_UID ?= $(shell id -u)
HOST_GID ?= $(shell id -g)
# Leave Dune's worker count unchanged by default. A constrained Docker VM can
# set `DUNE_JOBS=1` (or another small value) to avoid concurrent native linkers
# exhausting its memory without changing the normal CI/default behavior.
DUNE_JOBS ?=
DUNE_BUILD_ARGS := $(if $(strip $(DUNE_JOBS)),-j $(DUNE_JOBS),)
# Rust integration tests each link the Temporal Core graph into a separate test
# executable. Serializing those links and disabling unused incremental state
# keeps the peak disk/memory footprint bounded on hosted runners without
# changing the test set or the production build profile.
CARGO_BUILD_JOBS ?= 1
CARGO_TEST_ENV := CARGO_BUILD_JOBS=$(CARGO_BUILD_JOBS) CARGO_INCREMENTAL=0
COMPOSE_RUN := OCAML_IMAGE=$(OCAML_IMAGE) $(COMPOSE) --progress quiet run --rm --build --user $(HOST_UID):$(HOST_GID) $(SERVICE)
RUN := $(COMPOSE_RUN) opam exec --
CARGO := $(COMPOSE_RUN) cargo
CARGO_MANIFEST := rust/Cargo.toml
NATIVE_RUN := opam exec --
NATIVE_CARGO_TARGET_DIR ?= $(CARGO_TARGET_DIR)
ifeq ($(strip $(NATIVE_CARGO_TARGET_DIR)),)
NATIVE_CARGO_TARGET_DIR := $(CURDIR)/_build/rust
endif
NATIVE_OCAML_VERSION ?= 5.5
NATIVE_RUST_VERSION ?= 1.94.1
NATIVE_ARCH ?=
NATIVE_RUST_HOST ?=
NATIVE_ENV := CARGO_TARGET_DIR="$(NATIVE_CARGO_TARGET_DIR)"
QUALITY_CARGO_DENY_VERSION ?= 0.20.2
QUALITY_CARGO_MACHETE_VERSION ?= 0.9.2
QUALITY_TYPOS_VERSION ?= 1.48.0

.PHONY: version-check build build-examples cargo-metadata test test-unit test-runtime test-rust test-bridge test-install test-api release-preflight release-tag-check test-quality-contract test-temporal-config test-temporal-worker-readiness-contract test-temporal-worker-stop-contract test-temporal-worker-crash-recovery-contract test-temporal-worker-cache-eviction-contract test-core-lifecycle-integration temporal-start temporal-start-worker temporal-run-driver temporal-inspect-smoke temporal-stop-worker test-temporal-two-binary test-temporal-integration test-temporal-worker-restart test-temporal-worker-restart-contract test-temporal-worker-restart-live test-temporal-worker-crash-recovery test-temporal-worker-cache-eviction test-temporal-worker-cache-eviction-live test-temporal-workflow-patching test-temporal-workflow-patching-contract test-temporal-workflow-patching-live test-temporal-parent-child-restart test-temporal-parent-child-restart-contract test-temporal-parent-child-restart-live test-temporal-parent-child-failure-replay test-temporal-parent-child-failure-replay-contract test-temporal-parent-child-failure-replay-live temporal-health temporal-status temporal-logs temporal-stop temporal-clean lint lint-rust fmt quality quality-tool-version-check quality-rust quality-spelling license-check audit clean verify check native-version-check native-build native-test native-test-rust native-test-install native-lint native-lint-rust native-verify
version-check:
	@OCAML_IMAGE=$(OCAML_IMAGE) $(COMPOSE) --progress plain build $(SERVICE) >&2
	@actual="$$( $(RUN) ocamlc -version | tail -n 1 )"; \
	case "$$actual" in \
		$(OCAML_VERSION).*) ;; \
		*) echo "expected OCaml $(OCAML_VERSION).x, got $$actual" >&2; exit 1 ;; \
	esac

build:
	$(RUN) dune build $(DUNE_BUILD_ARGS)
	$(MAKE) build-examples
	$(CARGO) build --manifest-path $(CARGO_MANIFEST) --locked

# Keep the examples as explicit compile targets rather than relying on Dune's
# default alias. Every Docker and native build therefore proves that all three
# executable applications compile against the public installed-library name.
build-examples:
	$(RUN) dune build $(DUNE_BUILD_ARGS) examples/workflow_worker/workflow_worker.exe examples/activity_worker/activity_worker.exe examples/client/client.exe

# Emits only the locked Cargo metadata document on stdout, allowing CI to pipe
# it into the isolated license scanner without knowing the Compose fixture path.
cargo-metadata:
	@$(CARGO) metadata --manifest-path $(CARGO_MANIFEST) --locked --format-version 1

test:
	$(MAKE) test-temporal-config
	$(MAKE) test-temporal-worker-readiness-contract
	$(MAKE) test-temporal-worker-stop-contract
	$(RUN) dune runtest
	$(MAKE) test-rust
	$(MAKE) test-bridge
	$(MAKE) test-install
	$(MAKE) test-quality-contract

test-rust:
	$(COMPOSE_RUN) sh test/smoke/test_rust_toolchain.sh
	$(COMPOSE_RUN) env $(CARGO_TEST_ENV) cargo test --manifest-path $(CARGO_MANIFEST) --locked

test-bridge:
	$(COMPOSE_RUN) sh test/bridge/test_abi.sh

test-install:
	$(COMPOSE_RUN) sh test/bridge/test_install.sh

# The API witness is part of the installed-consumer regression. Keep a named
# target so release preparation and downstream CI can request the public
# compatibility gate without knowing the package-layout test's implementation.
test-api: test-install

# Release preparation must run from a clean checkout. Keep its stale-owner
# rejection fixture out of the ordinary quality target so contributors can run
# that target while iterating with local changes, while the release-preflight
# workflow exercises both gates on every clean CI checkout.
release-preflight:
	sh scripts/check-release-preflight.sh .
	sh test/smoke/test_release_preflight_contract.sh .

# Validates a concrete vMAJOR.MINOR.PATCH tag against the package manifests.
# This is deliberately host-only so a release cannot spend time building an
# artifact whose version metadata is inconsistent.
release-tag-check:
	@test -n "$(RELEASE_TAG)" || { echo "set RELEASE_TAG=vMAJOR.MINOR.PATCH" >&2; exit 2; }
	sh scripts/check-release-tag.sh . "$(RELEASE_TAG)"

test-quality-contract:
	sh test/smoke/test_quality_contract.sh .
	sh test/smoke/test_release_tag_contract.sh .

test-temporal-config:
	sh test/smoke/test_temporal_compose_config.sh

# Verifies that an interrupted worker cannot leave a readiness marker that a
# later Compose health check would mistake for this run's successful startup.
test-temporal-worker-readiness-contract:
	sh test/smoke/test_worker_readiness_contract.sh

# Verifies the worker-stop evidence independently of Docker. The live teardown
# uses this same checker after Compose stop, so a stale aggregate log cannot
# turn a failed shutdown into a false-positive acceptance result.
test-temporal-worker-stop-contract:
	sh test/smoke/test_worker_stop_contract.sh

# This command runs inside the OCaml development container but connects to the
# real Temporal service on the shared Compose network.
test-core-lifecycle-integration:
	$(COMPOSE_RUN) env TEMPORAL_ADDRESS=http://temporal:7233 TEMPORAL_NAMESPACE=temporal-sdk-test opam exec -- dune exec test/integration/test_core_lifecycle.exe

# The live stack is intentionally separate from unit verification. Native
# Windows and macOS jobs build the SDK directly and never start Linux services.
temporal-start:
	$(TEMPORAL_COMPOSE) up --detach --wait postgresql temporal
	$(MAKE) temporal-health

# Starts a fresh long-lived OCaml worker only after the database and Temporal
# frontend are healthy. Force-recreating the container is part of the lifecycle
# contract: readiness lives in the container's /tmp, so reusing a stopped
# container could expose its previous marker before this process starts. The
# worker also removes the marker before Worker.create as a second fail-closed
# boundary. Its health check is backed by an atomic marker published after
# public Worker.create succeeds, not merely by process liveness.
temporal-start-worker:
	@rm -f "$(SMOKE_WORKER_STOPPED_FILE)"
	$(TEMPORAL_COMPOSE) up --force-recreate --detach --build --wait smoke-worker

# Runs the independent OCaml driver against the already-running worker. Using
# `run --no-deps` avoids accidentally creating a second worker process and makes
# the driver's exit status the acceptance test's authoritative result. The
# bounded timeout turns a lost native request into a normal nonzero child exit
# instead of allowing CI to wait for the job's global timeout. Capturing the
# one-off container output in a host file lets the failure trap print driver
# phases even though `--rm` removes the container before Compose logs can see it.
temporal-run-driver:
	@set -eu; \
	rm -f "$(SMOKE_DRIVER_LOG_FILE)"; \
	rm -f "$(SMOKE_CANCELLATION_READY_FILE)"; \
	docker rm -f "$(SMOKE_DRIVER_CONTAINER)" >/dev/null 2>&1 || true; \
	status=0; \
	$(TEMPORAL_COMPOSE) run --build --rm --name "$(SMOKE_DRIVER_CONTAINER)" --no-deps smoke-driver >"$(SMOKE_DRIVER_LOG_FILE)" 2>&1 || status=$$?; \
	docker rm -f "$(SMOKE_DRIVER_CONTAINER)" >/dev/null 2>&1 || true; \
	cat "$(SMOKE_DRIVER_LOG_FILE)"; \
	rm -f "$(SMOKE_CANCELLATION_READY_FILE)"; \
	if [ "$$status" -eq 0 ] && ! grep -F "two-binary phase=client_shutdown status=ok" "$(SMOKE_DRIVER_LOG_FILE)" >/dev/null; then \
		echo "smoke-driver did not report graceful client shutdown" >&2; \
		status=1; \
	fi; \
	if [ "$$status" -ne 0 ]; then $(MAKE) temporal-inspect-smoke || true; $(MAKE) temporal-logs || true; fi; \
	exit "$$status"

# Performs a failure-only metadata check for the known workflow IDs: the
# driver's eighteen top-level executions and the deterministic children created
# by its parent workflows. The heartbeat-retry, timeout-retry,
# heartbeat-timeout-retry, activity-non-retryable, child-retry, and child-start-
# failure IDs are also listed so those failure boundaries are visible in
# best-effort diagnostics.
# The cancellation ID is intentionally listed explicitly so a
# missing cancellation terminal event can be distinguished from a driver that
# never started that execution. The admin-tools output contains execution
# status/run identity but no history or payloads, which distinguishes a start,
# worker-dispatch, and terminal-wait failure without expanding the acceptance
# test's privacy surface. A missing workflow is expected when the driver
# stalled before its first start, so every query is best effort and cannot mask
# the original exit status.
temporal-inspect-smoke:
	@for workflow_id in two-binary-fan-out two-binary-timer-then-activity two-binary-continue-as-new two-binary-activity-retry two-binary-activity-long-backoff-retry two-binary-activity-heartbeat-retry two-binary-async-activity-completion two-binary-activity-timeout-retry two-binary-activity-heartbeat-timeout-retry two-binary-activity-non-retryable-failure two-binary-parent-retries-child two-binary-parent-awaits-child two-binary-parent-awaits-failed-child two-binary-parent-cancels-child two-binary-non-retryable-failure two-binary-long-running-cancellation two-binary-parent-observes-child-start-failure two-binary-signal-condition; do \
		echo "--- Temporal metadata for $$workflow_id ---"; \
		$(TEMPORAL_COMPOSE) run --rm --no-deps temporal-admin-tools \
			temporal workflow describe --workflow-id "$$workflow_id" \
			--namespace temporal-sdk-test || true; \
	done

# Stops the long-lived worker only after the one-shot driver has completed.
# The worker's shutdown marker is emitted after its signal watcher has joined
# and the public [Worker.shutdown] result has been checked, so this target
# proves Compose teardown did not merely kill a healthy-looking process.
temporal-stop-worker:
	@set -eu; \
	rm -f "$(SMOKE_WORKER_STOPPED_FILE)"; \
	$(TEMPORAL_COMPOSE) stop --timeout 30 smoke-worker; \
	if ! sh test/integration/temporal/scripts/check-worker-stop-marker.sh "$(SMOKE_WORKER_STOPPED_FILE)"; then \
		echo "smoke-worker did not report graceful shutdown" >&2; \
		$(MAKE) temporal-logs || true; \
		exit 1; \
	fi

temporal-health:
	$(TEMPORAL_COMPOSE) exec -T postgresql pg_isready -U temporal -d postgres
	$(TEMPORAL_COMPOSE) exec -T postgresql psql -v ON_ERROR_STOP=1 -U temporal -d temporal -tAc 'SELECT 1 FROM schema_version LIMIT 1'
	$(TEMPORAL_COMPOSE) exec -T postgresql psql -v ON_ERROR_STOP=1 -U temporal -d temporal_visibility -tAc 'SELECT 1 FROM schema_version LIMIT 1'
	$(TEMPORAL_COMPOSE) run --rm --no-deps --entrypoint /bin/sh temporal-admin-tools /scripts/check-temporal-stack.sh

temporal-status:
	$(TEMPORAL_COMPOSE) ps

temporal-logs:
	$(TEMPORAL_COMPOSE) logs --no-color --tail 200 postgresql temporal-schema temporal smoke-worker
	@if [ -f "$(SMOKE_DRIVER_LOG_FILE)" ]; then \
		echo '--- smoke-driver one-off output ---'; \
		tail -n 200 "$(SMOKE_DRIVER_LOG_FILE)"; \
	fi

temporal-stop:
	$(TEMPORAL_COMPOSE) down --remove-orphans
	@rm -f "$(SMOKE_CANCELLATION_READY_FILE)" "$(SMOKE_WORKER_STOPPED_FILE)" \
		"$(SMOKE_REPLAY_DIAGNOSTICS_FILE)" "$(SMOKE_RESTART_ACCEPTED_FILE)" \
		"$(SMOKE_RESTART_RESULT_FILE)" "$(SMOKE_RESTART_DRIVER_LOG_FILE)" \
		"$(SMOKE_RESTART_INITIAL_HISTORY)" "$(SMOKE_RESTART_TERMINAL_HISTORY)" \
		"$(SMOKE_RESTART_TERMINAL_HISTORY).raw" \
		"$(SMOKE_RESTART_TERMINAL_HISTORY).describe.json" \
		"$(SMOKE_RESTART_CONTROLLER_FILE)" "$(SMOKE_CACHE_EVICTION_FILE)" \
		"$(SMOKE_CACHE_EVICTION_READY_FILE)" "$(SMOKE_CACHE_EVICTION_SECOND_READY_FILE)" \
		"$(SMOKE_CACHE_EVICTION_DRIVER_LOG_FILE)"

temporal-clean:
	$(TEMPORAL_COMPOSE) down --volumes --remove-orphans
	@rm -f "$(SMOKE_DRIVER_LOG_FILE)" "$(SMOKE_CANCELLATION_READY_FILE)" \
		"$(SMOKE_WORKER_STOPPED_FILE)" "$(SMOKE_REPLAY_DIAGNOSTICS_FILE)" \
		"$(SMOKE_RESTART_ACCEPTED_FILE)" "$(SMOKE_RESTART_RESULT_FILE)" \
		"$(SMOKE_RESTART_DRIVER_LOG_FILE)" "$(SMOKE_RESTART_INITIAL_HISTORY)" \
		"$(SMOKE_RESTART_TERMINAL_HISTORY)" \
		"$(SMOKE_RESTART_TERMINAL_HISTORY).raw" \
		"$(SMOKE_RESTART_TERMINAL_HISTORY).describe.json" \
		"$(SMOKE_RESTART_CONTROLLER_FILE)" "$(SMOKE_CACHE_EVICTION_FILE)" \
		"$(SMOKE_CACHE_EVICTION_READY_FILE)" "$(SMOKE_CACHE_EVICTION_SECOND_READY_FILE)" \
		"$(SMOKE_CACHE_EVICTION_DRIVER_LOG_FILE)"

test-temporal-integration: test-temporal-config
	@set -eu; \
	cleanup() { \
		status=$$?; \
		trap - EXIT HUP INT TERM; \
		if [ "$$status" -ne 0 ]; then \
			$(MAKE) temporal-logs || true; \
		fi; \
		$(MAKE) temporal-clean || true; \
		exit "$$status"; \
	}; \
	$(MAKE) temporal-clean; \
	trap cleanup EXIT; \
	trap 'exit 129' HUP; \
	trap 'exit 130' INT; \
	trap 'exit 143' TERM; \
	$(MAKE) temporal-start; \
	$(MAKE) temporal-health; \
	$(MAKE) test-core-lifecycle-integration; \
	$(MAKE) temporal-start-worker; \
	$(MAKE) temporal-run-driver; \
	$(MAKE) temporal-stop-worker

# Explicit name for callers that want to discover the two-process acceptance
# without reading the broader integration target. It shares the same isolated
# lifecycle and therefore cannot leave a second Temporal stack behind.
test-temporal-two-binary: test-temporal-integration

# Runs the Docker-free contract first, then proves a real two-generation
# Temporal execution. Generation one starts the fixed workflow and reaches a
# pending timer; the controller gracefully stops or forcibly kills that worker,
# removes it, starts generation two on the same queue, requires an OCaml replay
# marker, and only then waits for the exact terminal result. Every intermediate
# document is normalized and validated before the final controller record is
# accepted.
test-temporal-worker-restart:
	$(MAKE) test-temporal-worker-restart-contract
	$(MAKE) test-temporal-worker-restart-live

test-temporal-worker-restart-contract:
	sh test/integration/temporal/scripts/test-restart-replay-contract.sh

# Runs the same exact-run replay acceptance after forcibly killing generation
# one. The crash path deliberately skips the worker's graceful shutdown marker;
# the controller records the container exit code and requires generation two to
# replay and complete before the replacement is accepted.
test-temporal-worker-crash-recovery:
	$(MAKE) test-temporal-worker-crash-recovery-contract
	TEMPORAL_WORKER_RESTART_MODE=crash $(MAKE) test-temporal-worker-restart-live

test-temporal-worker-crash-recovery-contract:
	sh test/smoke/test_temporal_worker_crash_recovery_contract.sh

# Runs a one-slot Core cache against two independent parked executions. The
# driver waits for the first task acknowledgement, admits the second run, and
# then waits for the worker's exact RemoveFromCache marker. It does not require
# a second-run completion callback because that callback can be withheld while
# Core evicts the first run; waiting for it would deadlock the eviction proof.
# Both exact runs are then cancelled and checked for typed terminal outcomes.
test-temporal-worker-cache-eviction:
	$(MAKE) test-temporal-worker-cache-eviction-contract
	SMOKE_WORKER_MAX_CACHED_WORKFLOWS=1 \
	SMOKE_WORKER_CACHE_EVICTION_FILE=$(SMOKE_CACHE_EVICTION_CONTAINER_FILE) \
	SMOKE_WORKER_CACHE_EVICTION_READY_FILE=$(SMOKE_CACHE_EVICTION_READY_CONTAINER_FILE) \
	SMOKE_WORKER_CACHE_EVICTION_SECOND_READY_FILE=$(SMOKE_CACHE_EVICTION_SECOND_READY_CONTAINER_FILE) \
	SMOKE_CACHE_EVICTION_SECOND_WORKFLOW_ID=two-binary-cache-eviction-b \
	SMOKE_REPLAY_WORKFLOW_ID=two-binary-cache-eviction-a \
	$(MAKE) test-temporal-worker-cache-eviction-live

test-temporal-worker-cache-eviction-contract:
	sh test/smoke/test_temporal_worker_cache_eviction_contract.sh

# Proves the patch lifecycle with three genuine server histories: marker-free
# legacy replay under active code, active-marker replay under deprecation-only
# code, and deprecated-marker replay after all patch calls are removed.
test-temporal-workflow-patching:
	$(MAKE) test-temporal-workflow-patching-contract
	$(MAKE) test-temporal-workflow-patching-live

test-temporal-workflow-patching-contract:
	sh test/integration/temporal/scripts/test-patch-replay-contract.sh

test-temporal-workflow-patching-live: test-temporal-config
	OCAML_IMAGE="$(OCAML_IMAGE)" HOST_UID="$(HOST_UID)" HOST_GID="$(HOST_GID)" \
		TEMPORAL_COMPOSE_PROJECT="$(TEMPORAL_COMPOSE_PROJECT)" \
		SMOKE_DRIVER_TIMEOUT_SECONDS="$(TEMPORAL_DRIVER_TIMEOUT_SECONDS)" \
		sh test/integration/temporal/scripts/run-patch-replay-live.sh

# Exercises a parent and its long-running child across a complete worker
# replacement. The contract target proves the fail-closed evidence format
# without Docker; the live target binds and validates both exact server runs.
test-temporal-parent-child-restart:
	$(MAKE) test-temporal-parent-child-restart-contract
	$(MAKE) test-temporal-parent-child-restart-live

test-temporal-parent-child-restart-contract:
	sh test/integration/temporal/scripts/test-parent-child-restart-replay-contract.sh

test-temporal-parent-child-restart-live: test-temporal-config
	OCAML_IMAGE="$(OCAML_IMAGE)" HOST_UID="$(HOST_UID)" HOST_GID="$(HOST_GID)" \
		TEMPORAL_COMPOSE_PROJECT="$(TEMPORAL_COMPOSE_PROJECT)" \
		SMOKE_PARENT_CHILD_RESTART_TIMEOUT_SECONDS="$(SMOKE_PARENT_CHILD_RESTART_TIMEOUT_SECONDS)" \
		sh test/integration/temporal/scripts/run-parent-child-restart-replay-live.sh

# Exercises the same two-generation parent/child replay lifecycle with a
# deterministic non-retryable child failure. The source-only contract runs on
# every host; the live target is reserved for the Compose Temporal gate.
test-temporal-parent-child-failure-replay:
	$(MAKE) test-temporal-parent-child-failure-replay-contract
	$(MAKE) test-temporal-parent-child-failure-replay-live

test-temporal-parent-child-failure-replay-contract:
	sh test/integration/temporal/scripts/test-child-failure-replay-contract.sh

test-temporal-parent-child-failure-replay-live: test-temporal-config
	OCAML_IMAGE="$(OCAML_IMAGE)" HOST_UID="$(HOST_UID)" HOST_GID="$(HOST_GID)" \
		TEMPORAL_COMPOSE_PROJECT="$(TEMPORAL_COMPOSE_PROJECT)" \
		SMOKE_PARENT_CHILD_RESTART_TIMEOUT_SECONDS="$(SMOKE_PARENT_CHILD_RESTART_TIMEOUT_SECONDS)" \
		sh test/integration/temporal/scripts/run-child-failure-replay-live.sh

test-temporal-worker-cache-eviction-live: test-temporal-config
	@set -eu; \
	driver_container="$(TEMPORAL_COMPOSE_PROJECT)-smoke-cache-eviction-driver"; \
	cleanup_driver() { \
		docker rm -f "$$driver_container" >/dev/null 2>&1 || true; \
		$(TEMPORAL_COMPOSE) rm --stop --force smoke-cache-eviction-driver >/dev/null 2>&1 || true; \
	}; \
	cleanup_cache_worker() { \
		$(TEMPORAL_COMPOSE) rm --stop --force smoke-cache-eviction-worker >/dev/null 2>&1 || true; \
	}; \
	cleanup() { \
		status=$$?; \
		trap - EXIT HUP INT TERM; \
		if [ "$$status" -ne 0 ]; then cat "$(SMOKE_CACHE_EVICTION_DRIVER_LOG_FILE)" 2>/dev/null || true; $(TEMPORAL_COMPOSE) logs --no-color --tail 200 smoke-cache-eviction-worker 2>/dev/null || true; $(MAKE) temporal-logs || true; fi; \
		cleanup_driver; \
		cleanup_cache_worker; \
		$(MAKE) temporal-clean || true; \
		exit "$$status"; \
	}; \
	trap cleanup EXIT; \
	trap 'exit 129' HUP; \
	trap 'exit 130' INT; \
	trap 'exit 143' TERM; \
	$(MAKE) temporal-clean; \
	rm -f "$(SMOKE_CACHE_EVICTION_FILE)" "$(SMOKE_CACHE_EVICTION_READY_FILE)" "$(SMOKE_CACHE_EVICTION_SECOND_READY_FILE)" "$(SMOKE_CACHE_EVICTION_DRIVER_LOG_FILE)"; \
	$(MAKE) temporal-start; \
	$(TEMPORAL_COMPOSE) up --force-recreate --detach --build --wait smoke-cache-eviction-worker; \
	docker rm -f "$$driver_container" >/dev/null 2>&1 || true; \
	status=0; \
	$(TEMPORAL_COMPOSE) run --build --rm --name "$$driver_container" --no-deps smoke-cache-eviction-driver >"$(SMOKE_CACHE_EVICTION_DRIVER_LOG_FILE)" 2>&1 || status=$$?; \
	cat "$(SMOKE_CACHE_EVICTION_DRIVER_LOG_FILE)"; \
	if [ "$$status" -ne 0 ]; then exit "$$status"; fi; \
	command -v jq >/dev/null 2>&1; \
	jq -e --arg workflow_id two-binary-cache-eviction-a \
		--arg reason cache_full \
		'type == "object" and (keys | sort) == ["reason", "run_id", "workflow_id"] and .workflow_id == $$workflow_id and .run_id != "" and .reason == $$reason' \
		"$(SMOKE_CACHE_EVICTION_FILE)" >/dev/null

test-temporal-worker-restart-live: test-temporal-config
	@set -eu; \
	replacement_mode=$${TEMPORAL_WORKER_RESTART_MODE:-graceful}; \
	case "$$replacement_mode" in graceful|crash) ;; *) echo "unsupported TEMPORAL_WORKER_RESTART_MODE: $$replacement_mode" >&2; exit 2 ;; esac; \
	workflow_id=two-binary-worker-restart-replay; \
	accepted_file="$(SMOKE_RESTART_ACCEPTED_FILE)"; \
	result_file="$(SMOKE_RESTART_RESULT_FILE)"; \
	diagnostics_file="$(SMOKE_REPLAY_DIAGNOSTICS_FILE)"; \
	initial_history="$(SMOKE_RESTART_INITIAL_HISTORY)"; \
	terminal_history="$(SMOKE_RESTART_TERMINAL_HISTORY)"; \
	raw_history="$(SMOKE_RESTART_TERMINAL_HISTORY).raw"; \
	describe_file="$(SMOKE_RESTART_TERMINAL_HISTORY).describe.json"; \
	controller_file="$(SMOKE_RESTART_CONTROLLER_FILE)"; \
	driver_log="$(SMOKE_RESTART_DRIVER_LOG_FILE)"; \
	validator="test/integration/temporal/scripts/validate-restart-replay.sh"; \
	normalizer="test/integration/temporal/scripts/normalize-history.sh"; \
	identity_validator="test/integration/temporal/scripts/validate-restart-replay-identity.sh"; \
	controller_validator="test/integration/temporal/scripts/validate-restart-replay-controller.sh"; \
	driver_pid=''; \
	driver_container="$(SMOKE_RESTART_DRIVER_CONTAINER)"; \
	cleanup_driver() { \
		if [ -n "$$driver_pid" ] && kill -0 "$$driver_pid" 2>/dev/null; then kill -TERM "$$driver_pid" 2>/dev/null || true; fi; \
		docker rm -f "$$driver_container" >/dev/null 2>&1 || true; \
		$(TEMPORAL_COMPOSE) rm --stop --force temporal-admin-tools smoke-restart-driver >/dev/null 2>&1 || true; \
	}; \
	cleanup() { \
		status=$$?; \
		trap - EXIT HUP INT TERM; \
		cleanup_driver; \
		if [ "$$status" -ne 0 ]; then cat "$$driver_log" 2>/dev/null || true; $(MAKE) temporal-logs || true; fi; \
		$(MAKE) temporal-clean || true; \
		exit "$$status"; \
	}; \
	trap cleanup EXIT; \
	trap 'exit 129' HUP; \
	trap 'exit 130' INT; \
	trap 'exit 143' TERM; \
	stale_project_volumes_before_cleanup=$$(docker volume ls -q --filter label=com.docker.compose.project=$(TEMPORAL_COMPOSE_PROJECT) | wc -l | tr -d ' '); \
	$(MAKE) temporal-clean; \
	project_volumes_before=$$(docker volume ls -q --filter label=com.docker.compose.project=$(TEMPORAL_COMPOSE_PROJECT) | wc -l | tr -d ' '); \
	rm -f "$$accepted_file" "$$result_file" "$$diagnostics_file" "$$initial_history" "$$terminal_history" "$$raw_history" "$$describe_file" "$$controller_file" "$$driver_log"; \
	$(MAKE) temporal-start; \
	$(MAKE) temporal-start-worker; \
	docker rm -f "$$driver_container" >/dev/null 2>&1 || true; \
	$(TEMPORAL_COMPOSE) run --build --rm --name "$$driver_container" --no-deps smoke-restart-driver >"$$driver_log" 2>&1 & driver_pid=$$!; \
	query_history() { \
		stage=$$1; destination=$$2; \
		if ! $(TEMPORAL_COMPOSE) run --rm --no-deps temporal-admin-tools \
			temporal workflow show --workflow-id "$$workflow_id" --run-id "$$run_id" \
			--namespace temporal-sdk-test --output json >"$$raw_history" 2>/dev/null; then return 1; fi; \
		if ! sh "$$normalizer" --workflow-id "$$workflow_id" --run-id "$$run_id" \
			--output "$$destination" <"$$raw_history"; then return 1; fi; \
		if [ "$$stage" = initial ]; then \
			sh "$$validator" --history "$$destination" --workflow-id "$$workflow_id" \
				--run-id "$$run_id" --stage initial >/dev/null 2>&1; \
		else \
			sh "$$validator" --history "$$destination" --initial-history "$$initial_history" \
				--diagnostics "$$diagnostics_file" --workflow-id "$$workflow_id" --run-id "$$run_id" \
				--stage terminal --require-replay >/dev/null 2>&1; \
		fi; \
	}; \
	for attempt in $$(seq 1 120); do \
		if [ -s "$$accepted_file" ]; then break; fi; \
		if ! kill -0 "$$driver_pid" 2>/dev/null; then cat "$$driver_log"; exit 1; fi; \
		sleep 1; \
	done; \
	[ -s "$$accepted_file" ] || { cat "$$driver_log"; echo "restart driver did not publish its run identity" >&2; exit 1; }; \
	run_id=$$(sed -n 's/^run_id=//p' "$$accepted_file"); \
	[ -n "$$run_id" ] || { echo "restart driver marker has no run ID" >&2; exit 1; }; \
	if ! $(TEMPORAL_COMPOSE) run --rm --no-deps temporal-admin-tools \
		temporal workflow describe --workflow-id "$$workflow_id" --run-id "$$run_id" \
		--namespace temporal-sdk-test --output json >"$$describe_file" 2>/dev/null; then \
		echo "restart workflow identity lookup failed" >&2; exit 1; \
	fi; \
	sh "$$identity_validator" --input "$$describe_file" --workflow-id "$$workflow_id" --run-id "$$run_id" >/dev/null; \
	initial_count=''; \
	for attempt in $$(seq 1 120); do \
		if query_history initial "$$initial_history"; then \
			initial_count=$$(jq -r '.events | length' "$$initial_history"); break; \
		fi; \
		sleep 1; \
	done; \
	[ -n "$$initial_count" ] || { echo "restart workflow never reached its pending timer history" >&2; exit 1; }; \
	generation_one_container=$$($(TEMPORAL_COMPOSE) ps -q smoke-worker); \
	[ -n "$$generation_one_container" ] || { echo "generation one container ID is missing" >&2; exit 1; }; \
	if [ "$$replacement_mode" = crash ]; then \
		docker kill --signal KILL "$$generation_one_container" >/dev/null; \
		for attempt in $$(seq 1 30); do \
			generation_one_state=$$(docker inspect --format '{{.State.Status}}' "$$generation_one_container" 2>/dev/null || true); \
			[ "$$generation_one_state" = exited ] && break; \
			sleep 1; \
		done; \
		[ "$$generation_one_state" = exited ] || { echo "crashed generation one did not exit" >&2; exit 1; }; \
		generation_one_exit_code=$$(docker inspect --format '{{.State.ExitCode}}' "$$generation_one_container"); \
		if [ -e "$(SMOKE_WORKER_STOPPED_FILE)" ]; then \
			echo "crashed generation one left a graceful shutdown marker" >&2; \
			exit 1; \
		fi; \
		generation_one_shutdown_marker=false; \
	else \
		$(MAKE) temporal-stop-worker; \
		generation_one_exit_code=0; \
		generation_one_shutdown_marker=true; \
	fi; \
	$(TEMPORAL_COMPOSE) rm --force smoke-worker >/dev/null; \
	remaining_workers=$$(docker ps -aq --filter label=com.docker.compose.project=$(TEMPORAL_COMPOSE_PROJECT) --filter label=com.docker.compose.service=smoke-worker | wc -l | tr -d ' '); \
	[ "$$remaining_workers" -eq 0 ] || { echo "generation one worker container was not removed" >&2; exit 1; }; \
	SMOKE_WORKER_GENERATION=2 $(MAKE) temporal-start-worker; \
	generation_two_container=$$($(TEMPORAL_COMPOSE) ps -q smoke-worker); \
	[ -n "$$generation_two_container" ] || { echo "generation two container ID is missing" >&2; exit 1; }; \
	[ "$$generation_two_container" != "$$generation_one_container" ] || { echo "worker container was reused" >&2; exit 1; }; \
	for attempt in $$(seq 1 120); do \
		if ! kill -0 "$$driver_pid" 2>/dev/null && [ ! -s "$$result_file" ]; then cat "$$driver_log"; exit 1; fi; \
		if jq -e --arg run_id "$$run_id" \
			'.run_id == $$run_id and ([.records[] | .phase] == ["initial", "replay"]) and .records[1].is_replaying == true and .records[1].history_length != "0"' \
			"$$diagnostics_file" >/dev/null 2>&1; then break; fi; \
		sleep 1; \
	done; \
	jq -e --arg run_id "$$run_id" \
		'.run_id == $$run_id and ([.records[] | .phase] == ["initial", "replay"]) and .records[1].is_replaying == true and .records[1].history_length != "0"' \
		"$$diagnostics_file" >/dev/null; \
	for attempt in $$(seq 1 120); do \
		if [ -s "$$result_file" ]; then break; fi; \
		if ! kill -0 "$$driver_pid" 2>/dev/null; then cat "$$driver_log"; exit 1; fi; \
		sleep 1; \
	done; \
	wait "$$driver_pid"; driver_pid=''; \
	grep -F 'completed' "$$result_file" >/dev/null; \
	for attempt in $$(seq 1 120); do \
		if query_history terminal "$$terminal_history"; then break; fi; \
		sleep 1; \
	done; \
	sh "$$validator" --history "$$terminal_history" --initial-history "$$initial_history" \
		--diagnostics "$$diagnostics_file" --workflow-id "$$workflow_id" --run-id "$$run_id" \
		--stage terminal --require-replay >/dev/null; \
	replay_history=$$(jq -r '.records[1].history_length' "$$diagnostics_file"); \
	$(MAKE) temporal-stop-worker; \
	$(TEMPORAL_COMPOSE) rm --force smoke-worker >/dev/null; \
	generation_two_remaining_workers=$$(docker ps -aq --filter label=com.docker.compose.project=$(TEMPORAL_COMPOSE_PROJECT) --filter label=com.docker.compose.service=smoke-worker | wc -l | tr -d ' '); \
	[ "$$generation_two_remaining_workers" -eq 0 ] || { echo "generation two worker container was not removed" >&2; exit 1; }; \
	$(TEMPORAL_COMPOSE) down --volumes --remove-orphans >/dev/null; \
	remaining_volumes=$$(docker volume ls -q --filter label=com.docker.compose.project=$(TEMPORAL_COMPOSE_PROJECT) | wc -l | tr -d ' '); \
	jq -n --arg workflow_id "$$workflow_id" --arg run_id "$$run_id" \
		--arg generation_one_container "$$generation_one_container" \
		--arg generation_two_container "$$generation_two_container" \
		--arg history_length "$$replay_history" \
		--arg replacement_mode "$$replacement_mode" --argjson generation_one_exit_code "$$generation_one_exit_code" \
		--argjson generation_one_shutdown_marker "$$generation_one_shutdown_marker" \
		--argjson initial_count "$$initial_count" --argjson terminal_count "$$(jq -r '.events | length' "$$terminal_history")" \
		--argjson stale_project_volumes_before_cleanup "$$stale_project_volumes_before_cleanup" \
		--argjson project_volumes_before "$$project_volumes_before" --argjson remaining_volumes "$$remaining_volumes" \
		--argjson generation_two_remaining_workers "$$generation_two_remaining_workers" \
		'{workflow_id:$$workflow_id,run_id:$$run_id,events:[{step:"stack_ready",status:"ok",stale_project_volumes_before_cleanup:$$stale_project_volumes_before_cleanup,remaining_project_volumes_before_start:$$project_volumes_before,temporal_healthy:true},{step:"driver_accepted",status:"ok",workflow_id:$$workflow_id,run_id:$$run_id},{step:"history_checked",status:"ok",stage:"initial",event_count:$$initial_count},{step:"driver_waiting",status:"ok"},{step:"generation_one_replaced",status:"ok",generation:1,container_id:$$generation_one_container,exit_code:$$generation_one_exit_code,replacement_mode:$$replacement_mode,shutdown_marker:$$generation_one_shutdown_marker},{step:"generation_one_removed",status:"ok",generation:1,container_id:$$generation_one_container,remaining_worker_containers:0},{step:"generation_two_ready",status:"ok",generation:2,container_id:$$generation_two_container,readiness_generation:2,fresh_container:true},{step:"replay_observed",status:"ok",generation:2,is_replaying:true,history_length:$$history_length},{step:"history_checked",status:"ok",stage:"terminal",event_count:$$terminal_count},{step:"driver_completed",status:"ok",outcome:"completed"},{step:"generation_two_stopped",status:"ok",generation:2,container_id:$$generation_two_container,exit_code:0,shutdown_marker:true},{step:"generation_two_removed",status:"ok",generation:2,container_id:$$generation_two_container,remaining_worker_containers:$$generation_two_remaining_workers},{step:"postgres_volume_removed",status:"ok",remaining_project_volumes:$$remaining_volumes}]}' \
		>"$$controller_file"; \
	sh "$$controller_validator" --controller "$$controller_file" --workflow-id "$$workflow_id" --run-id "$$run_id" --replacement-mode "$$replacement_mode"; \
	trap - EXIT HUP INT TERM; \
	$(MAKE) temporal-clean

test-unit:
	$(RUN) dune runtest test/unit test/smoke

test-runtime:
	$(RUN) dune runtest test/runtime

lint:
	$(RUN) dune build $(DUNE_BUILD_ARGS)
	$(MAKE) build-examples
	$(COMPOSE_RUN) sh scripts/check-format.sh
	$(MAKE) lint-rust

lint-rust:
	$(CARGO) fmt --manifest-path $(CARGO_MANIFEST) --all -- --check
	$(CARGO) clippy --manifest-path $(CARGO_MANIFEST) --locked --all-targets -- -D warnings

fmt:
	$(COMPOSE_RUN) sh scripts/check-format.sh

# These release binaries are intentionally separate from the development
# image. CI installs their pinned, checksum-verified artifacts once, while a
# contributor can install the same versions and invoke the identical Make gate.
quality: quality-rust quality-spelling

quality-tool-version-check:
	@actual="$$(cargo deny --version | awk '{ print $$2 }')"; \
	if [ "$$actual" != "$(QUALITY_CARGO_DENY_VERSION)" ]; then \
		echo "expected cargo-deny $(QUALITY_CARGO_DENY_VERSION), got $$actual" >&2; exit 1; \
	fi
	@actual="$$(cargo machete --version | awk '{ print $$1 }')"; \
	if [ "$$actual" != "$(QUALITY_CARGO_MACHETE_VERSION)" ]; then \
		echo "expected cargo-machete $(QUALITY_CARGO_MACHETE_VERSION), got $$actual" >&2; exit 1; \
	fi
	@actual="$$(typos --version | awk '{ print $$2 }')"; \
	if [ "$$actual" != "$(QUALITY_TYPOS_VERSION)" ]; then \
		echo "expected typos $(QUALITY_TYPOS_VERSION), got $$actual" >&2; exit 1; \
	fi

quality-rust: quality-tool-version-check
	cargo deny --manifest-path $(CARGO_MANIFEST) --locked --all-features check advisories sources
	cargo machete --with-metadata rust

quality-spelling: quality-tool-version-check
	typos

license-check:
	$(COMPOSE_RUN) sh scripts/check-licenses.sh

audit: license-check

clean:
	$(TEMPORAL_COMPOSE) down --remove-orphans
	rm -rf _build

verify: version-check lint test

check: verify license-check

# Native targets are used by the non-Linux compatibility jobs. The default
# developer path remains Docker Compose so no host OCaml toolchain is required.
native-version-check:
	@actual_ocaml="$$( $(NATIVE_RUN) ocamlc -version )"; \
	case "$$actual_ocaml" in \
		$(NATIVE_OCAML_VERSION).*) ;; \
		*) echo "expected OCaml $(NATIVE_OCAML_VERSION).x, got $$actual_ocaml" >&2; exit 1 ;; \
	esac
	@actual_rust="$$(rustc --version | awk '{ print $$2 }')"; \
	if [ "$$actual_rust" != "$(NATIVE_RUST_VERSION)" ]; then \
		echo "expected rustc $(NATIVE_RUST_VERSION), got $$actual_rust" >&2; exit 1; \
	fi
	@if [ -n "$(NATIVE_ARCH)" ]; then \
		actual_arch="$$( $(NATIVE_RUN) ocamlc -config-var architecture )"; \
		if [ "$$actual_arch" != "$(NATIVE_ARCH)" ]; then \
			echo "expected OCaml architecture $(NATIVE_ARCH), got $$actual_arch" >&2; exit 1; \
		fi; \
	fi
	@if [ -n "$(NATIVE_RUST_HOST)" ]; then \
		actual_host="$$(rustc -vV | sed -n 's/^host: //p')"; \
		if [ "$$actual_host" != "$(NATIVE_RUST_HOST)" ]; then \
			echo "expected Rust host $(NATIVE_RUST_HOST), got $$actual_host" >&2; exit 1; \
		fi; \
	fi

native-build:
	$(NATIVE_ENV) $(NATIVE_RUN) dune build @install $(DUNE_BUILD_ARGS)
	$(NATIVE_ENV) $(NATIVE_RUN) dune build $(DUNE_BUILD_ARGS) examples/workflow_worker/workflow_worker.exe examples/activity_worker/activity_worker.exe examples/client/client.exe
	$(NATIVE_ENV) cargo build --manifest-path $(CARGO_MANIFEST) --locked

native-test: native-test-rust native-test-install test-quality-contract
	$(NATIVE_ENV) $(NATIVE_RUN) dune runtest

native-test-rust:
	$(NATIVE_ENV) $(CARGO_TEST_ENV) cargo test --manifest-path $(CARGO_MANIFEST) --locked

native-test-install:
	$(NATIVE_ENV) sh test/bridge/test_install.sh

native-lint: native-lint-rust
	$(NATIVE_ENV) $(NATIVE_RUN) dune build $(DUNE_BUILD_ARGS)
	$(NATIVE_ENV) $(NATIVE_RUN) dune build $(DUNE_BUILD_ARGS) examples/workflow_worker/workflow_worker.exe examples/activity_worker/activity_worker.exe examples/client/client.exe
	sh scripts/check-format.sh

native-lint-rust:
	$(NATIVE_ENV) cargo fmt --manifest-path $(CARGO_MANIFEST) --all -- --check
	$(NATIVE_ENV) cargo clippy --manifest-path $(CARGO_MANIFEST) --locked --all-targets -- -D warnings

native-verify: native-version-check native-build native-lint native-test
