# Changelog

## Unreleased

## v0.0.1 — 2026-11-27

- First release. The `superdial` library joins a Matrix room's MatrixRTC call over LiveKit with
  end-to-end encrypted media, through SuperExchange's token service.
- `superdial-agent`, the agent side of calls with AgentPod bridge-mode agents: it transcribes
  what the caller says, hands the text to the hub, and speaks the agent's reply.
- `superdial-testcaller`, a headless caller for integration tests.
- The release carries the `superdial-agent` image as `superdial-agent.tar.zst`
  (`docker load < superdial-agent.tar.zst`), with its image id and `SHA256SUMS`.
