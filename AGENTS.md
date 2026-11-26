# superdial: agent guide

Rust Cargo workspace. `crates/superdial` is the library; `crates/superdial-testcaller` is a
headless caller used for integration tests.

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

`scripts/integration-rtc-spike.sh` runs two test callers against the rtc-spike stack (tuwunel
2.0.0 at `https://staging-rtc.superjackfruit.com`, LiveKit, lk-jwt-service as the dev token
issuer). It needs two test users whose passwords are in `.local/` (gitignored), and a route to
LiveKit's media ports (UDP 50100–50200 on the workbench tailnet address). See the script header.

```sh
cargo build -p superdial-testcaller
./scripts/integration-rtc-spike.sh             # fresh room; ROOM=!id:server reuses one
KILL_TEST=1 ./scripts/integration-rtc-spike.sh # also kill -9 a caller and time the delayed leave
```

Do not invite `@agent_echo` to test rooms: the spike bot still runs there and answers calls.
Runs write their logs and recordings to `.local/run-*` (gitignored).
