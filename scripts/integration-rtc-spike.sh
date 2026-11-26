#!/usr/bin/env bash
# Two superdial test callers in one encrypted room's call on the rtc-spike stack.
#
# Prerequisites:
#   - test users sdtest_a and sdtest_b on the rtc-spike homeserver, passwords in
#     .local/pw_sdtest_a and .local/pw_sdtest_b (gitignored);
#   - a route to LiveKit's media (the workbench tailnet address, UDP 50100-50200 or TCP 7881);
#   - cargo build -p superdial-testcaller (BIN overrides the binary).
#
# Token issuer: the rtc-spike's lk-jwt-service, through the dev-only LkJwtDevIssuer.
#
# Checks: both sides report the peer's E2EE state Ok; each recording carries the other's tone
# (A plays 440 Hz, B plays 660 Hz); B's C2 transcript reaches A and A posts it as C3; both
# memberships are cleared on hang-up. With KILL_TEST=1 it also kills a caller mid-call and
# times how long the server takes to fire its delayed leave.
set -euo pipefail

HS=${HS:-https://staging-rtc.superjackfruit.com}
SERVER=${SERVER:-staging-rtc.superjackfruit.com}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
BIN=${BIN:-${CARGO_TARGET_DIR:-$ROOT/target}/debug/superdial-testcaller}
LOCAL=$ROOT/.local
OUT=${OUT:-$LOCAL/run-$(date +%Y%m%dT%H%M%S)}
SECONDS_IN_CALL=${SECONDS_IN_CALL:-30}
mkdir -p "$OUT"
echo "output: $OUT"

tc() {
  local who=$1
  shift
  "$BIN" --homeserver "$HS" --user "sdtest_$who" --password-file "$LOCAL/pw_sdtest_$who" \
    --data-dir "$LOCAL/data_$who" "$@"
}

python3 - "$OUT" <<'PY'
import math, struct, sys, wave
for name, hz in (("a.wav", 440.0), ("b.wav", 660.0)):
    with wave.open(f"{sys.argv[1]}/{name}", "wb") as w:
        w.setnchannels(1); w.setsampwidth(2); w.setframerate(48000)
        w.writeframes(b"".join(struct.pack("<h", int(8000 * math.sin(2 * math.pi * hz * i / 48000))) for i in range(48000 * 2)))
PY

ROOM=${ROOM:-$(tc a create-room --invite "@sdtest_b:$SERVER" 2>"$OUT/create.log" | jq -r .room_id)}
echo "room: $ROOM"

tc b call --room "$ROOM" --issuer lk-jwt-dev --accept-invites --play "$OUT/b.wav" --record "$OUT/b-heard.wav" \
  --seconds "$SECONDS_IN_CALL" --send-transcript "hello from b" >"$OUT/b.jsonl" 2>"$OUT/b.log" &
B=$!
sleep 4
tc a call --room "$ROOM" --issuer lk-jwt-dev --play "$OUT/a.wav" --record "$OUT/a-heard.wav" \
  --seconds "$SECONDS_IN_CALL" --transcripts-from "@sdtest_b:$SERVER" >"$OUT/a.jsonl" 2>"$OUT/a.log" &
A=$!
set +e
wait $B; B_EXIT=$?
wait $A; A_EXIT=$?
set -e

A_TONE=$(tc a analyze --wav "$OUT/a-heard.wav" --tone 660)
B_TONE=$(tc b analyze --wav "$OUT/b-heard.wav" --tone 440)
MEMBERS=$(tc a members --room "$ROOM" 2>/dev/null | jq -c .members)
echo "A summary: $(jq -c 'select(.summary)' "$OUT/a.jsonl")"
echo "B summary: $(jq -c 'select(.summary)' "$OUT/b.jsonl")"
echo "A heard (660 Hz expected): $A_TONE"
echo "B heard (440 Hz expected): $B_TONE"
echo "members after: $MEMBERS"

fail=0
check() { if eval "$2"; then echo "PASS $1"; else echo "FAIL $1"; fail=1; fi; }
check "A exit 0 (peer E2EE Ok, membership cleared, transcript posted)" "[ $A_EXIT -eq 0 ]"
check "B exit 0 (peer E2EE Ok, membership cleared)" "[ $B_EXIT -eq 0 ]"
check "A hears B's 660 Hz tone" "[ \$(jq '.tone_ratio > 0.5' <<<'$A_TONE') = true ]"
check "B hears A's 440 Hz tone" "[ \$(jq '.tone_ratio > 0.5' <<<'$B_TONE') = true ]"
check "B's transcript posted by A as C3" "grep -q spoken_turn_posted '$OUT/a.jsonl'"
check "no memberships left" "[ '$MEMBERS' = '[]' ]"

if [ "${KILL_TEST:-0}" = 1 ]; then
  tc a call --room "$ROOM" --issuer lk-jwt-dev --seconds 120 >"$OUT/kill.jsonl" 2>"$OUT/kill.log" &
  K=$!
  until grep -q '"event":"joined"' "$OUT/kill.jsonl" 2>/dev/null; do sleep 0.5; done
  sleep 6
  kill -9 $K
  T0=$(date +%s)
  for _ in $(seq 1 30); do
    M=$(tc b members --room "$ROOM" 2>/dev/null | jq -c .members)
    [ "$M" = "[]" ] && break
    sleep 1
  done
  T=$(($(date +%s) - T0))
  echo "after kill -9: members $M, within ${T}s (each check includes a login and sync)"
  check "killed caller's membership gone within 20 s" "[ '$M' = '[]' ] && [ $T -lt 20 ]"
fi
exit $fail
