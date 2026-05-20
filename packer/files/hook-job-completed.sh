#!/bin/sh
# Runner-side ACTIONS_RUNNER_HOOK_JOB_COMPLETED script.
#
# Fires immediately after the runner finishes a job (success, failure,
# or cancelled). We notify the orchestrator so it can mark the VM
# completed and start destroy WITHOUT waiting for the systemd
# ExecStopPost=poweroff -> Proxmox status poll loop, shaving 5-10s off
# the cleanup tail.
#
# Failures are non-fatal — if the orchestrator is unreachable, the
# gh.Reconciler will catch the runner-went-idle state within ~15s and
# destroy the VM via the same code path.
#
# Env vars required:
#   SCALESET_HOOK_URL       - e.g. http://192.168.0.20:9103
#   SCALESET_HOOK_SECRET    - bearer token
#   RUNNER_NAME             - set by the runner

set -u

if [ -z "${SCALESET_HOOK_URL:-}" ] || [ -z "${SCALESET_HOOK_SECRET:-}" ]; then
    exit 0
fi

NAME="${RUNNER_NAME:-}"
if [ -z "${NAME}" ] && [ -r /opt/actions-runner/.runner ]; then
    NAME=$(jq -r '.agentName // empty' /opt/actions-runner/.runner 2>/dev/null || true)
fi

# The runner sets job status env vars at job-completed time. Different
# runner versions use different variable names; we probe both.
RESULT="${ACTIONS_RUNNER_JOB_RESULT:-${JOB_STATUS:-unknown}}"

PAYLOAD=$(jq -nc \
    --arg phase "completed" \
    --arg name "${NAME}" \
    --arg result "${RESULT}" \
    --arg ts "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    '{phase:$phase, runner_name:$name, result:$result, timestamp:$ts}')

curl --silent --show-error --fail \
     --max-time 5 \
     --connect-timeout 2 \
     -H "Authorization: Bearer ${SCALESET_HOOK_SECRET}" \
     -H "Content-Type: application/json" \
     --data "${PAYLOAD}" \
     "${SCALESET_HOOK_URL}/runner-event" \
     >/dev/null 2>&1 || true

exit 0
