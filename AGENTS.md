# superdial: agent guide

Rust Cargo workspace. `crates/superdial` is the library; `crates/superdial-testcaller` is a
headless caller used for integration tests; `crates/superdial-agent` is the agent's side of a
call, served to the AgentPod hub over C6 and shipped as the image in `Dockerfile`.

## Rules

- The wire formats are fixed contracts (C1, C2, C3, C5 of the superdial master plan, published
  as treaty contracts). Do not change a message type, field or route here; change the contract
  first.
- No Element or AGPL code. The rtc-spike's lk-jwt-service is reached over HTTP only, through
  `LkJwtDevIssuer` behind the `dev-lk-jwt` feature, for development and tests. It is never a
  default.
- The library has no UI and no platform audio code. Audio crosses its boundary as 48 kHz mono
  i16 PCM.
- Binaries must install a rustls provider: `rustls::crypto::ring::default_provider().install_default()`.
- superdial-agent holds only call-device tokens, its C6 secret and its own speech key, never
  the hub's appservice token. It never asks for room timelines and deletes room keys it is sent.
  Its turn logic lives in `conversation.rs` behind `CallIo`, tested with fakes; keep it there.
- Never log tokens. Transcript and reply text only behind `SUPERDIAL_AGENT_LOG_TEXT`.
- Keep wire decisions in the pure modules (`contract`, `keys`, `membership`, `state`) with unit
  tests; keep I/O behind `Homeserver`, `TokenIssuer`, `DelayedEvents` and `Media`.

## Build and test

```sh
export CARGO_TARGET_DIR=$PWD/target   # keep the (large) target dir in the repo, not /tmp
cargo fmt --all --check
cargo clippy --all-targets -- -D warnings
cargo test
```

The `livekit` crate's `webrtc-sys` downloads a prebuilt libwebrtc and compiles C++ glue. It
builds here with the system clang/gcc; if it does not, set `CC`/`CXX` to clang 21 or later. On
a machine short of memory, add `-j 6`.

## Integration test

`scripts/integration-rtc-spike.sh` runs two test callers against a LiveKit dev stack (tuwunel
2.0.0, LiveKit, and lk-jwt-service as the dev token issuer). Set `HS`, `SERVER`, `USER_A` and
`USER_B`; the two test users' passwords go in `.local/pw_<user>` (gitignored). The machine needs
a route to that LiveKit's media ports. See the script header.

```sh
cargo build -p superdial-testcaller
./scripts/integration-rtc-spike.sh             # fresh room; ROOM=!id:server reuses one
KILL_TEST=1 ./scripts/integration-rtc-spike.sh # also kill -9 a caller and time the delayed leave
```

`scripts/integration-agent-rtc-spike.sh` runs superdial-agent (built with
`--features superdial-agent/dev-lk-jwt`) against a fake hub (`scripts/fake_hub.py`) and a test
caller: a spoken question, the reply heard back, and the cancel on hang-up and on DELETE. It
uses `USER_B` as the stand-in agent with a second password-login device
(`.local/agent_device.json`), and speech services keyed with `.local/speech_key`; by default
local copies of agentpod's transcriber and speech servers on ports 18840 and 18841
(`.local/speech-standin/run.sh`). The dev issuer's tokens cannot set participant attributes, so
the caller does not see `superdial.state` there; the agent's log shows the states it set.

Do not build the image on a machine short of memory or disk. `docker build --check .` lints the Dockerfile.

Runs write their logs and recordings to `.local/run-*` (gitignored).
