# superdial

Voice calls to agents over MatrixRTC and LiveKit, end-to-end encrypted, in Rust.

superdial is the call code shared by SuperMessage's core (the person calling) and
`superdial-agent` (the agent answering). It covers the call protocol only:

- transport discovery (MSC4143 `rtc/transports`);
- the OpenID-to-LiveKit-token exchange, through a pluggable token issuer (SuperExchange's
  `POST {base}/v1/token` by default);
- call membership (`org.matrix.msc3401.call.member`) with a delayed leave (MSC4140) armed first
  and restarted every 5 s, and the delay handed to the token service;
- media keys: generated per call, sent Olm-encrypted to each peer's call device
  (`dev.superdial.call.encryption_keys`), and registered under the peer's LiveKit identity;
- transcripts (`dev.superdial.call.transcript`) and the "spoken in a call" message marker;
- the LiveKit connection with end-to-end encryption on, audio in and out as 48 kHz mono PCM.

It has no UI and no platform audio code.

## Crates

| Crate | What |
| --- | --- |
| `crates/superdial` | the library |
| `crates/superdial-testcaller` | a headless caller for tests: plays a WAV as its microphone, records what it hears, prints the encryption state |

## Status

Pre-release. Built against tuwunel 2.0.0 and LiveKit server 1.13. The wire formats are the
superdial contracts (C1, C2, C3, C5), to be published as treaty contracts.

## Licence

Apache-2.0. Copyright 2026 Super Jackfruit Labs (OPC) Private Limited. No Element or AGPL code
is used or linked.
