#!/usr/bin/env python3
"""Global merit (wooden fish) counter API for henryli.duckdns.org.

Stdlib-only. Listens on 127.0.0.1:8377 behind nginx (/api/).
Endpoints:
  GET  /api/merit       -> {"count": N}
  POST /api/merit/tap   -> body {"n": 1..20} -> {"count": N}
Persistence: count kept in memory, flushed to $MUYU_STATE/count.txt
every 2s when dirty (atomic tmp+rename). Anti-abuse: per-IP minimum
interval + per-IP hourly increment cap. Real client IP is taken from
the X-Real-IP header set by nginx (the service only listens on
loopback, so only nginx can reach it).
"""
import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

STATE_DIR = os.environ.get("MUYU_STATE", "/var/lib/muyu")
STATE_FILE = os.path.join(STATE_DIR, "count.txt")
PORT = 8377
MIN_INTERVAL = 0.15        # seconds between tap requests per IP
HOURLY_CAP = 1000          # max increments per IP per hour

lock = threading.Lock()
count = 0
dirty = False
last_seen = {}             # ip -> last request ts
hourly = {}                # ip -> [window_start_ts, increments]


def load():
    global count
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            count = int((f.read() or "0").strip() or 0)
    except Exception:
        count = 0


def save():
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(str(count))
        f.flush()
        os.fsync(f.fileno())
    os.rename(tmp, STATE_FILE)


def flusher():
    global dirty
    while True:
        time.sleep(2)
        with lock:
            if dirty:
                try:
                    save()
                except Exception:
                    pass
                dirty = False


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def _json(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _ip(self):
        return self.headers.get("X-Real-IP") or self.client_address[0]

    def do_GET(self):
        if self.path == "/api/merit":
            with lock:
                c = count
            self._json(200, {"count": c})
        else:
            self._json(404, {"error": "not found"})

    def do_POST(self):
        global count, dirty
        if self.path != "/api/merit/tap":
            return self._json(404, {"error": "not found"})
        ip = self._ip()
        now = time.time()
        with lock:
            if now - last_seen.get(ip, 0.0) < MIN_INTERVAL:
                return self._json(429, {"error": "slow down", "count": count})
            last_seen[ip] = now
            win = hourly.get(ip)
            if not win or now - win[0] >= 3600:
                hourly[ip] = [now, 0]
                win = hourly[ip]
            if win[1] >= HOURLY_CAP:
                return self._json(429, {"error": "hourly cap", "count": count})
        try:
            ln = int(self.headers.get("Content-Length", "0"))
            if ln <= 0 or ln > 1024:
                raise ValueError("length")
            data = json.loads(self.rfile.read(ln) or b"{}")
            n = int(data.get("n", 1))
        except Exception:
            return self._json(400, {"error": "bad request"})
        n = max(1, min(n, 20))
        with lock:
            count += n
            dirty = True
            hourly[ip][1] += n
            c = count
        self._json(200, {"count": c})


if __name__ == "__main__":
    os.makedirs(STATE_DIR, exist_ok=True)
    load()
    threading.Thread(target=flusher, daemon=True).start()
    ThreadingHTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
