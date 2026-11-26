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
| `crates/superdial-agent` | the agent's side of a call, run as a container beside the AgentPod hub (below) |

## Using it

The application owns the matrix-sdk `Client` and its sync loop.

```rust
let (call, mut events) = superdial::Call::new(&client, &room_id, superdial::CallOptions::default())?;
// Make `call` reachable from the sync loop first: after each sync,
//   call.handle_to_device(&ev).await for every to-device event, then
//   call.refresh_members().await
call.join(None).await?; // None = SuperExchange's token service at the discovered base
call.capture(&pcm_48k_mono).await?;     // microphone
// events: CallEvent::Media(MediaEvent::Audio { identity, samples }), E2ee states, members,
// keys, transcripts
call.leave().await?;
```

## superdial-agent

`superdial-agent` joins calls as a bridge-mode AgentPod agent. The hub asks it to over C6, the
call API (treaty `agent-calls.v1`), on loopback or the docker network:

| Route | What |
| --- | --- |
| `PUT /v1/calls/{room_id}` | join as the agent's call device `{agent_user_id, homeserver_url, device: {device_id, access_token}, voice, stt_language}`, or stay joined → `202` |
| `DELETE /v1/calls/{room_id}` | leave, cancelling any turn waiting → `204` |
| `POST /v1/calls/{room_id}/turns/{turn_id}/reply` | `{seq, text, final, outcome}`: speak the text when `final` and `ok` → `204` |
| `GET /v1/health` | `{"calls": n}` |

All of them need `Authorization: Bearer $SUPERDIAL_AGENT_TOKEN`. When it leaves with a turn
waiting, it calls the hub: `POST $SUPERDIAL_HUB_URL/internal/superdial/turns/{turn_id}/cancel`
with `{"room_id"}` and the same bearer.

In a call it:
- finds where each thing the caller says ends (an RMS gate, 800 ms of quiet);
- has it transcribed by foundry's speech-to-text, then sends the transcript (C2) to the
  caller's call device, which posts it to the room as the caller's message (C3);
- speaks the hub's reply with foundry's Kokoro, after stripping markdown and capping it at
  about 1200 characters;
- shows its state in the LiveKit attribute `superdial.state`: `connecting`, `listening`,
  `thinking`, `speaking`;
- leaves when the last person leaves or LiveKit drops, and stays up for other calls.

It holds one sqlite crypto store per call device under `SUPERDIAL_STATE_DIR`. It syncs only
the call rooms' membership state and to-device events, never room timelines, and deletes the
room keys the call device is sent.

### Configuration

| Variable | |
| --- | --- |
| `SUPERDIAL_AGENT_BIND` | listen address, default `127.0.0.1:8850` (`0.0.0.0:8850` in the image) |
| `SUPERDIAL_AGENT_TOKEN` | the C6 bearer, both directions (secret) |
| `SUPERDIAL_HUB_URL` | the hub, for the cancel route |
| `SUPERDIAL_STATE_DIR` | crypto stores, default `/var/lib/superdial-agent` |
| `TRANSCRIBE_URL`, `TRANSCRIBE_API_KEY`, `TRANSCRIBE_MODEL` | speech-to-text (`large-v3-turbo` by default); the key is superdial-agent's own (secret) |
| `SPEECH_URL`, `SPEECH_API_KEY` | text-to-speech; superdial-agent's own key (secret) |
| `SUPERDIAL_AGENT_STORE_PASSPHRASE` | optional: encrypts the stores at rest (secret) |
| `SUPERDIAL_AGENT_LOG_TEXT` | development only: `1` logs transcripts and replies |
| `SUPERDIAL_AGENT_DEV_LK_JWT_URL` | development only, in a `dev-lk-jwt` build: use lk-jwt-service as token issuer |
| `RUST_LOG` | log filter; logs are JSON lines |

Every secret can instead be given as a file: `SUPERDIAL_AGENT_TOKEN_FILE=/run/secrets/token`.
Setting both forms is an error. Tokens are never logged. Transcript and reply text is logged
only with `SUPERDIAL_AGENT_LOG_TEXT=1`.

```sh
docker build -t superdial-agent .
docker run -d --name superdial-agent -p 127.0.0.1:8850:8850 -v superdial-agent:/var/lib/superdial-agent \
  -e SUPERDIAL_AGENT_TOKEN_FILE=/run/secrets/agent_token -e SUPERDIAL_HUB_URL=http://hub:3000 \
  -e TRANSCRIBE_URL=http://foundry:8840 -e TRANSCRIBE_API_KEY_FILE=/run/secrets/stt_key \
  -e SPEECH_URL=http://foundry:8841 -e SPEECH_API_KEY_FILE=/run/secrets/speech_key \
  -v /path/to/secrets:/run/secrets:ro superdial-agent
```

A call device's store must stay with its device. If the hub deletes and re-mints the device,
delete its store directory (`devices/<user>~<device>`) too, or the new device never uploads
its keys.

## Status

Pre-release. Built against tuwunel 2.0.0 and LiveKit server 1.13. The wire formats are the
superdial contracts (C1, C2, C3, C5), to be published as treaty contracts.

## Licence

Apache-2.0. Copyright 2026 Super Jackfruit Labs (OPC) Private Limited. No Element or AGPL code
is used or linked.
