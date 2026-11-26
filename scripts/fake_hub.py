#!/usr/bin/env python3
"""Plays the AgentPod hub's side of C6 against a running superdial-agent, for integration tests.

- Serves POST /internal/superdial/turns/{turn_id}/cancel (bearer SUPERDIAL_AGENT_TOKEN), logging
  each cancel as a JSON line.
- PUTs /v1/calls/{room_id} once the caller has joined, with the stand-in agent's call device.
- Watches the test caller's JSON lines: when the caller has posted a spoken turn (C3), it
  replies with fixed text (--mode reply), DELETEs the call (--mode delete), or does nothing
  (--mode none, so the caller's hang-up has to cancel the turn).

Standard library only. Secrets are read from files and never printed.
"""

import argparse
import http.server
import json
import sys
import threading
import time
import urllib.parse
import urllib.request

OUT_LOCK = threading.Lock()


def out(**event):
    event["t"] = round(time.time(), 3)
    with OUT_LOCK:
        print(json.dumps(event), flush=True)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--agent-url", required=True)
    p.add_argument("--token-file", required=True)
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--room", required=True)
    p.add_argument("--homeserver", required=True)
    p.add_argument("--device-file", required=True, help="JSON {user_id, device_id, access_token}")
    p.add_argument("--caller-jsonl", required=True)
    p.add_argument("--mode", choices=["reply", "delete", "none"], default="reply")
    p.add_argument("--reply", default="The capital of France is Paris.")
    p.add_argument("--voice", default="af_heart")
    p.add_argument("--stt-language", default="en")
    p.add_argument("--seconds", type=float, default=120)
    a = p.parse_args()
    token = open(a.token_file).read().strip()
    device = json.load(open(a.device_file))
    room_q = urllib.parse.quote(a.room, safe="")

    class Hub(http.server.BaseHTTPRequestHandler):
        def do_POST(self):
            parts = self.path.split("/")
            body = self.rfile.read(int(self.headers.get("content-length") or 0))
            ok_auth = self.headers.get("authorization") == f"Bearer {token}"
            if len(parts) == 6 and parts[1:4] == ["internal", "superdial", "turns"] and parts[5] == "cancel":
                try:
                    room_id = json.loads(body).get("room_id")
                except ValueError:
                    room_id = None
                out(event="cancel", turn_id=parts[4], room_id=room_id, auth_ok=ok_auth)
                self.send_response(204 if ok_auth else 401)
            else:
                self.send_response(404)
            self.end_headers()

        def log_message(self, *args):
            pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", a.port), Hub)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    out(event="hub_listening", port=a.port)

    def agent(method, path, body=None):
        req = urllib.request.Request(
            a.agent_url.rstrip("/") + path,
            method=method,
            data=None if body is None else json.dumps(body).encode(),
            headers={"authorization": f"Bearer {token}", "content-type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=40) as r:
                return r.status
        except urllib.error.HTTPError as e:
            return e.code

    def caller_lines():
        """Yields the caller's JSON events as they are written."""
        pos = 0
        deadline = time.time() + a.seconds
        while time.time() < deadline:
            try:
                with open(a.caller_jsonl) as f:
                    f.seek(pos)
                    chunk = f.read()
                    pos = f.tell()
            except FileNotFoundError:
                chunk = ""
            for line in chunk.splitlines():
                try:
                    yield json.loads(line)
                except ValueError:
                    pass
            time.sleep(0.2)

    put_done = False
    last_turn = None
    for ev in caller_lines():
        kind = ev.get("event")
        if kind == "joined" and not put_done:
            status = agent(
                "PUT",
                f"/v1/calls/{room_q}",
                {
                    "agent_user_id": device["user_id"],
                    "homeserver_url": a.homeserver,
                    "device": {"device_id": device["device_id"], "access_token": device["access_token"]},
                    "voice": a.voice,
                    "stt_language": a.stt_language or None,
                },
            )
            put_done = True
            out(event="put", status=status)
            # Idempotent: a second PUT keeps the same call.
            out(event="put_again", status=agent("PUT", f"/v1/calls/{room_q}", {
                "agent_user_id": device["user_id"],
                "homeserver_url": a.homeserver,
                "device": {"device_id": device["device_id"], "access_token": device["access_token"]},
                "voice": a.voice,
                "stt_language": a.stt_language or None,
            }))
        elif kind == "transcript_received":
            last_turn = ev.get("turn_id")
        elif kind == "spoken_turn_posted" and last_turn:
            if a.mode == "reply":
                status = agent(
                    "POST",
                    f"/v1/calls/{room_q}/turns/{last_turn}/reply",
                    {"seq": 0, "text": a.reply, "final": True, "outcome": "ok"},
                )
                out(event="reply", turn_id=last_turn, status=status)
            elif a.mode == "delete":
                time.sleep(1)
                out(event="delete", turn_id=last_turn, status=agent("DELETE", f"/v1/calls/{room_q}"))
            else:
                out(event="no_reply", turn_id=last_turn)
            last_turn = None
        elif "summary" in ev:
            break
    # Give a hang-up's cancel time to arrive.
    time.sleep(12)
    out(event="health", status=agent("GET", "/v1/health"))
    server.shutdown()


if __name__ == "__main__":
    sys.exit(main())
