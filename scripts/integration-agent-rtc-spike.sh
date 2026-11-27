#!/usr/bin/env bash
# superdial-agent in a call on a LiveKit dev stack, with a fake hub and a headless caller.
#
# Who is who:
#   - caller: $USER_A (superdial-testcaller), password in .local/pw_$USER_A;
#   - agent:  $USER_B stands in for a bridge agent. A second device, logged in by password and
#     saved in .local/agent_device.json, stands in for the hub-minted SUPERDIAL call device;
#   - hub:    scripts/fake_hub.py (PUT, reply or DELETE, and the cancel route);
#   - speech: TRANSCRIBE_URL / SPEECH_URL with the agent's own key in .local/speech_key
#     (default: local copies of agentpod's transcriber and speech servers, .local/speech-standin);
#   - token issuer: the dev stack's lk-jwt-service through the dev-only LkJwtDevIssuer.
#
# Three runs in one room:
#   1. reply:  the caller asks a spoken question; the fake hub replies; the caller records it;
#   2. hangup: the hub never replies; the caller hangs up; the agent must cancel the turn;
#   3. delete: the hub DELETEs the call while the turn waits; the agent must cancel the turn.
#
# Set HS, SERVER, USER_A and USER_B (see integration-rtc-spike.sh).
# Build first: cargo build -p superdial-testcaller -p superdial-agent --features superdial-agent/dev-lk-jwt
set -euo pipefail

HS=${HS:?set HS to the dev stack homeserver URL, e.g. https://matrix.example}
SERVER=${SERVER:?set SERVER to its server name, e.g. matrix.example}
USER_A=${USER_A:?set USER_A to the first test user localpart}
USER_B=${USER_B:?set USER_B to the second test user localpart}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
TARGET=${CARGO_TARGET_DIR:-$ROOT/target}/debug
LOCAL=$ROOT/.local
OUT=${OUT:-$LOCAL/run-agent-$(date +%Y%m%dT%H%M%S)}
TRANSCRIBE_URL=${TRANSCRIBE_URL:-http://127.0.0.1:18840}
SPEECH_URL=${SPEECH_URL:-http://127.0.0.1:18841}
SPEECH_KEY_FILE=${SPEECH_KEY_FILE:-$LOCAL/speech_key}
AGENT_PORT=${AGENT_PORT:-18850}
HUB_PORT=${HUB_PORT:-18851}
QUESTION=${QUESTION:-Hello agent. What is the capital of France?}
REPLY=${REPLY:-The capital of France is Paris.}
mkdir -p "$OUT"
echo "output: $OUT"

tc() {
  "$TARGET/superdial-testcaller" --homeserver "$HS" --user "$USER_A" --password-file "$LOCAL/pw_$USER_A" \
    --data-dir "$LOCAL/data_a" "$@"
}

# The agent's call device: log in once, keep the session (mode 600), never print the token.
if [ ! -s "$LOCAL/agent_device.json" ]; then
  python3 - "$HS" "$USER_B" "$LOCAL/pw_$USER_B" "$LOCAL/agent_device.json" <<'PY'
import json, os, sys, urllib.request
hs, user, pw_file, out = sys.argv[1:]
body = {"type": "m.login.password", "identifier": {"type": "m.id.user", "user": user},
        "password": open(pw_file).read().strip(), "initial_device_display_name": "superdial-agent stand-in"}
req = urllib.request.Request(f"{hs}/_matrix/client/v3/login", data=json.dumps(body).encode(),
                             headers={"content-type": "application/json", "user-agent": "superdial-integration"})
r = json.load(urllib.request.urlopen(req, timeout=30))
fd = os.open(out, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
os.write(fd, json.dumps({"user_id": r["user_id"], "device_id": r["device_id"], "access_token": r["access_token"]}).encode())
os.close(fd)
print(f"agent device: {r['user_id']} {r['device_id']}")
PY
fi
AGENT_USER=$(jq -r .user_id "$LOCAL/agent_device.json")

# Matrix calls as the agent device, for joining the room and discovering the transport.
as_agent() {
  python3 - "$HS" "$LOCAL/agent_device.json" "$@" <<'PY'
import json, sys, urllib.parse, urllib.request
hs, dev, method, path = sys.argv[1:5]
tok = json.load(open(dev))["access_token"]
data = sys.argv[5].encode() if len(sys.argv) > 5 else None
req = urllib.request.Request(hs + path, data=data, method=method,
                             headers={"authorization": f"Bearer {tok}", "content-type": "application/json",
                                      "user-agent": "superdial-integration"})
print(urllib.request.urlopen(req, timeout=30).read().decode())
PY
}
LK_SERVICE=$(as_agent GET /_matrix/client/unstable/org.matrix.msc4143/rtc/transports |
  jq -r '[.rtc_transports[] | select(.type=="livekit")][0].livekit_service_url')
echo "livekit service: $LK_SERVICE"

# The question, spoken by Kokoro (the same speech service the agent uses).
python3 - "$SPEECH_URL" "$SPEECH_KEY_FILE" "$QUESTION" "$OUT/question.wav" <<'PY'
import json, sys, urllib.request
url, key_file, text, out = sys.argv[1:]
req = urllib.request.Request(url.rstrip("/") + "/v1/audio/speech",
    data=json.dumps({"model": "tts-1", "input": text, "voice": "am_michael", "response_format": "wav"}).encode(),
    headers={"authorization": "Bearer " + open(key_file).read().strip(), "content-type": "application/json"})
open(out, "wb").write(urllib.request.urlopen(req, timeout=120).read())
PY

ROOM=${ROOM:-$(tc create-room --invite "$AGENT_USER" 2>"$OUT/create.log" | jq -r .room_id)}
echo "room: $ROOM"
as_agent POST "/_matrix/client/v3/rooms/$(jq -rn --arg r "$ROOM" '$r|@uri')/join" '{}' >/dev/null
echo "agent joined the room"

# superdial-agent, its own credentials only.
(umask 077; [ -s "$LOCAL/agent_token" ] || python3 -c 'import secrets; print(secrets.token_hex(32))' >"$LOCAL/agent_token")
SUPERDIAL_AGENT_BIND=127.0.0.1:$AGENT_PORT \
SUPERDIAL_AGENT_TOKEN_FILE=$LOCAL/agent_token \
SUPERDIAL_HUB_URL=http://127.0.0.1:$HUB_PORT \
SUPERDIAL_STATE_DIR=$LOCAL/agent-state \
TRANSCRIBE_URL=$TRANSCRIBE_URL TRANSCRIBE_API_KEY_FILE=$SPEECH_KEY_FILE \
SPEECH_URL=$SPEECH_URL SPEECH_API_KEY_FILE=$SPEECH_KEY_FILE \
SUPERDIAL_AGENT_DEV_LK_JWT_URL=$LK_SERVICE \
SUPERDIAL_AGENT_LOG_TEXT=${LOG_TEXT:-1} \
RUST_LOG=${RUST_LOG:-info,superdial_agent::session=debug,matrix_sdk=warn,matrix_sdk_base=warn,matrix_sdk_crypto=warn,matrix_sdk_crypto::backups=error,livekit=warn,libwebrtc=error} \
  "$TARGET/superdial-agent" >"$OUT/agent.log" 2>&1 &
AGENT=$!
trap 'kill $AGENT 2>/dev/null || true' EXIT
for _ in $(seq 1 50); do
  curl -sf -m 1 -H "authorization: Bearer $(cat "$LOCAL/agent_token")" "http://127.0.0.1:$AGENT_PORT/v1/health" >/dev/null && break
  sleep 0.2
done
echo "superdial-agent up"

run() { # name mode seconds
  local name=$1 mode=$2 secs=$3
  python3 "$ROOT/scripts/fake_hub.py" --agent-url "http://127.0.0.1:$AGENT_PORT" --token-file "$LOCAL/agent_token" \
    --port "$HUB_PORT" --room "$ROOM" --homeserver "$HS" --device-file "$LOCAL/agent_device.json" \
    --caller-jsonl "$OUT/$name.jsonl" --mode "$mode" --reply "$REPLY" --seconds $((secs + 30)) >"$OUT/$name-hub.jsonl" 2>"$OUT/$name-hub.log" &
  local hub=$!
  set +e
  tc call --room "$ROOM" --issuer lk-jwt-dev --play "$OUT/question.wav" --play-once --play-delay 2 \
    --record "$OUT/$name-heard.wav" --seconds "$secs" --transcripts-from "$AGENT_USER" \
    >"$OUT/$name.jsonl" 2>"$OUT/$name.log"
  echo "$name: caller exit $?"
  wait $hub
  set -e
  sleep 3
}

fail=0
check() { if eval "$2"; then echo "PASS $1"; else echo "FAIL $1"; fail=1; fi; }
members() { tc members --room "$ROOM" 2>/dev/null | jq -c .members; }
transcribe() {
  curl -s -m 300 -H "authorization: Bearer $(cat "$SPEECH_KEY_FILE")" -F "file=@$1;type=audio/wav" \
    -F model=large-v3-turbo -F language=en "$TRANSCRIBE_URL/v1/audio/transcriptions" | jq -r .text
}

# 1. reply
run reply reply "${REPLY_SECONDS:-90}"
HEARD_Q=$(jq -r 'select(.event=="transcript_received") | .text' "$OUT/reply.jsonl" | head -1)
HEARD_R=$(transcribe "$OUT/reply-heard.wav")
STATES=$(jq -r 'select(.event=="attributes") | .attributes["superdial.state"] // empty' "$OUT/reply.jsonl" | uniq | paste -sd, -)
SET_STATES=$(jq -r 'select(.fields.message=="state") | .fields.state' "$OUT/agent.log" | paste -sd, -)
M1=$(members)
echo "transcript: $HEARD_Q"
echo "caller heard: $HEARD_R"
echo "states seen by caller: $STATES"
echo "states the agent set: $SET_STATES"
echo "members after: $M1"
check "transcript is the question" "grep -qi 'capital of France' <<<'$HEARD_Q'"
check "C3 posted by the caller" "grep -q spoken_turn_posted '$OUT/reply.jsonl'"
check "reply sent by the hub" "grep -q '\"event\": \"reply\".*\"status\": 204' '$OUT/reply-hub.jsonl'"
check "caller heard the reply" "grep -qi 'paris' <<<'$HEARD_R'"
check "agent set connecting,listening,thinking,speaking,listening" "grep -q '^connecting,listening,thinking,speaking,listening' <<<'$SET_STATES'"
if grep -q 'does not have permission to update own metadata' "$OUT/agent.log"; then
  echo "SKIP caller sees superdial.state: this issuer's token does not grant canUpdateOwnMetadata (lk-jwt-service); SuperExchange's does"
else
  check "caller saw listening,thinking,speaking,listening" "grep -q 'listening,thinking,speaking,listening' <<<'$STATES'"
fi
check "agent E2EE Ok at the caller" "jq -e 'select(.summary) | .summary.e2ee | to_entries | map(select(.key|startswith(\"$AGENT_USER\"))) | .[0].value == \"Ok\"' '$OUT/reply.jsonl' >/dev/null"
check "both memberships cleared" "[ '$M1' = '[]' ]"
check "no cancel after an answered turn" "! grep -q '\"cancel\"' '$OUT/reply-hub.jsonl'"
check "second PUT is idempotent (202)" "grep -q '\"put_again\", \"status\": 202' '$OUT/reply-hub.jsonl'"

# 2. hangup while the turn waits
run hangup none "${HANGUP_SECONDS:-45}"
T2=$(jq -r 'select(.event=="no_reply") | .turn_id' "$OUT/hangup-hub.jsonl" | head -1)
check "hang-up cancels the waiting turn" "[ -n '$T2' ] && grep -q '\"cancel\", \"turn_id\": \"$T2\", \"room_id\": \"$ROOM\", \"auth_ok\": true' '$OUT/hangup-hub.jsonl'"
check "memberships cleared after hang-up" "[ '$(members)' = '[]' ]"

# 3. DELETE while the turn waits
run delete delete "${DELETE_SECONDS:-45}"
T3=$(jq -r 'select(.event=="delete") | .turn_id' "$OUT/delete-hub.jsonl" | head -1)
check "DELETE answered 204" "grep -q '\"delete\".*\"status\": 204' '$OUT/delete-hub.jsonl'"
check "DELETE cancels the waiting turn" "[ -n '$T3' ] && grep -q '\"cancel\", \"turn_id\": \"$T3\", \"room_id\": \"$ROOM\", \"auth_ok\": true' '$OUT/delete-hub.jsonl'"
check "memberships cleared after DELETE" "[ '$(members)' = '[]' ]"
check "health back to 0 calls" "curl -s -H \"authorization: Bearer \$(cat '$LOCAL/agent_token')\" http://127.0.0.1:$AGENT_PORT/v1/health | grep -q '\"calls\":0'"

check "agent log has no access token" "! grep -qF \"\$(jq -r .access_token '$LOCAL/agent_device.json')\" '$OUT/agent.log'"
SYNCS=$(jq -c 'select(.fields.message=="synced") | .fields.timeline_events' "$OUT/agent.log" 2>/dev/null | sort | uniq -c | tr '\n' ' ')
echo "agent syncs by timeline event count: $SYNCS"
TYPES=$(jq -r 'select(.fields.message=="synced") | .fields.timeline_types' "$OUT/agent.log" | tr -d '[]" ' | tr ',' '\n' | grep -v '^$' | sort | uniq -c | tr '\n' ' ')
echo "timeline event types the agent was sent: ${TYPES:-none}"
check "agent was sent no room messages, only call and member state" "[ -n '$SYNCS' ] && ! jq -r 'select(.fields.message==\"synced\") | .fields.timeline_types' '$OUT/agent.log' | tr -d '[]\" ' | tr ',' '\n' | grep -v '^\$' | grep -qvE '^(org.matrix.msc3401.call.member|m.room.member)\$'"
exit $fail
