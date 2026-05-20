#!/bin/sh
# Runner-side ACTIONS_RUNNER_HOOK_JOB_STARTED script.
#
# Fires immediately before the runner begins executing a job. We POST a
# minimal payload back to the orchestrator's :9103 endpoint so it can
# transition the row Assigned -> Running without depending on the
# scaleset listener's JobStarted callback (which occasionally drops or
# delivers garbled events).
#
# Failures are non-fatal — if the orchestrator is unreachable or the
# secret has rotated, the gh.Reconciler polling loop will catch us up
# within ~15s. We intentionally do NOT block the job on this.
#
# Env vars required:
#   SCALESET_HOOK_URL       - e.g. http://192.168.0.20:9103
#   SCALESET_HOOK_SECRET    - bearer token
#   RUNNER_NAME             - set by the runner; matches our naming
# Optional (best-effort, may be empty):
#   GITHUB_JOB              - job ID as a string (numeric or human name)

set -u

# Bail silently if the hook integration isn't configured. This makes the
# image safe to use against orchestrators with runner_hook disabled.
if [ -z "${SCALESET_HOOK_URL:-}" ] || [ -z "${SCALESET_HOOK_SECRET:-}" ]; then
    exit 0
fi

# The runner's `.runner` file (written when the runner registers) carries
# the numeric runner ID and the registered name. Reading it is more
# reliable than guessing from env vars, which vary across runner
# versions.
RUNNER_ID=""
NAME="${RUNNER_NAME:-}"
if [ -r /opt/actions-runner/.runner ]; then
    # The file is JSON: {"agentId":NN,"agentName":"…",…}
    RUNNER_ID=$(jq -r '.agentId // empty' /opt/actions-runner/.runner 2>/dev/null || true)
    if [ -z "${NAME}" ]; then
        NAME=$(jq -r '.agentName // empty' /opt/actions-runner/.runner 2>/dev/null || true)
    fi
fi

# GITHUB_JOB is documented as the job ID but is the YAML job key, not
# the numeric ID. We pass it through anyway; the orchestrator treats it
# as informational.
JOB_RAW="${GITHUB_JOB:-}"
JOB_NUM=$(printf '%s' "${JOB_RAW}" | grep -E '^[0-9]+$' || true)

PAYLOAD=$(jq -nc \
    --arg phase "started" \
    --arg name "${NAME}" \
    --arg ts "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg rid "${RUNNER_ID}" \
    --arg jid "${JOB_NUM}" \
    '{phase:$phase, runner_name:$name, timestamp:$ts}
     + (if $rid != "" then {runner_id: ($rid|tonumber)} else {} end)
     + (if $jid != "" then {job_id: ($jid|tonumber)} else {} end)')

# 5s connect + total timeout. The orchestrator is on the LAN; if it
# takes longer than that, something's wrong and we don't want to block
# the job.
curl --silent --show-error --fail \
     --max-time 5 \
     --connect-timeout 2 \
     -H "Authorization: Bearer ${SCALESET_HOOK_SECRET}" \
     -H "Content-Type: application/json" \
     --data "${PAYLOAD}" \
     "${SCALESET_HOOK_URL}/runner-event" \
     >/dev/null 2>&1 || true

exit 0
