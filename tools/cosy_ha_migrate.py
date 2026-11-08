#!/usr/bin/env python3
"""
Cosy 6 — Home Assistant entity_id migration for the 2026-11-08 register map.

Python 3.9+ standard library only. Run it on any host that can read the HA
config directory (grep) or reach the HA WebSocket API (rename, verify).

    # 1. Find every reference to an old entity_id. Run BEFORE the rename.
    python3 cosy_ha_migrate.py grep /config /addon_configs

    # 2. Show the rename plan (dry run, no change).
    export HA_TOKEN=<long-lived access token>
    python3 cosy_ha_migrate.py rename --url ws://homeassistant.local:8123/api/websocket

    # 3. Do the rename.
    python3 cosy_ha_migrate.py rename --url ws://... --apply

    # 4. Check registry state and recorder history (acceptance D3, D4, D7).
    python3 cosy_ha_migrate.py verify --url ws://...

    # 5. After the rename: 0 hits expected, except qsh.yaml (report only).
    python3 cosy_ha_migrate.py grep --after-rename /config /addon_configs

The rename uses the entity registry (config/entity_registry/update). The
unique_id (qsh_modbus_reg_N) does not change. The HA recorder moves the
state history and the long-term statistics to the new entity_id.

Order: reg 24 leaves "suction_pressure" BEFORE reg 50 takes it. The tool
waits between the two so that the recorder finishes the first move. If the
recorder still has the slug in use, it does not move the reg 50 history
(HA log: "Cannot migrate history ... already in use").

This tool never edits a file. qsh.yaml hits are reported only: the QSH
mapping is a separate change.
"""

import argparse
import base64
import json
import os
import re
import socket
import ssl
import struct
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse

DEFAULT_PREFIX = "sensor.hub_cosy_hp_cosy_"

# (register, old slug, new slug). The list order is the rename order.
# Reg 24 must come before reg 50 (slug collision on "suction_pressure").
RENAMES = [
    (24, "suction_pressure", "unknown_24"),
    (19, "compressor_frequency", "unknown_19"),
    (20, "runtime_counter", "electrical_power_input"),
    (25, "heat_output_suspect", "dc_link_voltage"),
    (28, "heat_output_2", "unknown_28"),
    (29, "condenser_inlet_temp", "compressor_area_temp_1"),
    (30, "condenser_outlet_temp", "compressor_area_temp_2"),
    (32, "v1_heating_valve", "main_eev_position"),
    (34, "reversing_valve", "defrost_valve_position"),
    (35, "v4_inverter", "unknown_35"),
    (37, "indoor_ambient", "unknown_temp_37"),
    (39, "outdoor_coil_air_temp", "unknown_temp_39"),
    (41, "evaporator_inlet_temp", "sump_temp"),
    (45, "dhw_cylinder_temp", "discharge_gas_temp"),
    (50, "reported_cop", "suction_pressure"),
    (51, "modbus_reg_51", "circulation_pump_speed"),
    (52, "modbus_reg_52", "water_flow_target"),
    (53, "dhw_tank_temp", "compressor_demand"),
    (54, "outdoor_ambient_raw", "unknown_temp_54"),
    (55, "evaporator_temp", "suction_superheat"),
    (56, "discharge_temp", "saturated_condensing_temp"),
    (57, "defrost_accumulator", "saturated_evaporating_temp"),
    (58, "modbus_reg_58", "defrost_te_threshold"),
    (60, "rated_elec_input", "unknown_60"),
    (62, "eev_opening", "fan_speed_command"),
    (63, "energy_counter", "compressor_speed"),
    (66, "modbus_reg_66", "hp_state_substep"),
    (67, "runtime_counter_provisional", "substep_timer"),
    (77, "config_param_77", "standby_pump_run_flag"),
]

# The recorder exclude must not hold these after the change.
RECORDER_UNEXCLUDE = {51, 52, 58, 66}

# Slug that is old (reg 24) before the rename and new (reg 50) after it.
COLLISION_SLUG = "suction_pressure"

COLLISION_WAIT_S = 10


# =============================================================================
# grep
# =============================================================================

SKIP_DIRS = {".git", "__pycache__", "deps", "tts", "node_modules", ".cache", "backups"}
SKIP_FILES = {
    # Every entity is in these by definition. They are not configuration.
    "core.entity_registry", "core.restore_state", "core.device_registry",
    "trace.saved_traces", "core.config_entries",
}
SKIP_SUFFIXES = {".db", ".db-wal", ".db-shm", ".log", ".tar", ".gz", ".zip",
                 ".png", ".jpg", ".jpeg", ".gif", ".svg", ".ico", ".pyc", ".csv"}
MAX_FILE_BYTES = 20 * 1024 * 1024


def iter_files(roots):
    for root in roots:
        root = Path(root)
        if root.is_file():
            yield root
            continue
        for dirpath, dirnames, filenames in os.walk(root):
            dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
            for name in filenames:
                p = Path(dirpath) / name
                if name in SKIP_FILES or p.suffix.lower() in SKIP_SUFFIXES:
                    continue
                if name.startswith("home-assistant") and ".log" in name:
                    continue
                try:
                    if p.stat().st_size > MAX_FILE_BYTES:
                        continue
                except OSError:
                    continue
                yield p


def cmd_grep(args):
    prefix = args.prefix
    domain, _, obj_prefix = prefix.partition(".")
    old = {}
    for reg, old_slug, _new in RENAMES:
        if args.after_rename and old_slug == COLLISION_SLUG:
            continue  # now the reg 50 entity_id: a hit is correct
        old[old_slug] = reg
    slugs = sorted(old, key=len, reverse=True)
    # Old slug, then not a slug character: "runtime_counter" does not match
    # "runtime_counter_provisional"; "heat_output_2" does not match "heat_output".
    pattern = re.compile(
        r"(?<![a-z0-9_])(?:" + re.escape(domain) + r"\.)?" + re.escape(obj_prefix)
        + r"(" + "|".join(re.escape(s) for s in slugs) + r")(?![a-z0-9_])"
    )
    glob_pattern = re.compile(re.escape(obj_prefix) + r"[a-z0-9_]*\*")

    hits = {s: [] for s in slugs}
    globs = []
    files_read = 0
    for path in iter_files(args.paths):
        try:
            text = path.read_text(encoding="utf-8", errors="strict")
        except (UnicodeDecodeError, OSError):
            continue
        files_read += 1
        for lineno, line in enumerate(text.splitlines(), 1):
            for m in pattern.finditer(line):
                hits[m.group(1)].append((path, lineno, line.strip()))
            if glob_pattern.search(line):
                globs.append((path, lineno, line.strip()))

    def tag(path):
        return "  [qsh.yaml: REPORT ONLY, DO NOT CHANGE]" if path.name == "qsh.yaml" else ""

    total = 0
    qsh = 0
    print(f"Searched {files_read} text files under: {', '.join(map(str, args.paths))}")
    print(f"Entity prefix: {prefix}")
    print()
    for reg, old_slug, new_slug in RENAMES:
        if old_slug not in hits:
            continue
        h = hits[old_slug]
        total += len(h)
        qsh += sum(1 for p, _, _ in h if p.name == "qsh.yaml")
        note = ""
        if reg in RECORDER_UNEXCLUDE:
            note = "  (remove from recorder exclude)"
        if old_slug == COLLISION_SLUG:
            note += "  (AMBIGUOUS after rename: same id is reg 50 then. Review each hit NOW)"
        print(f"R{reg:<3} {prefix}{old_slug} -> {prefix}{new_slug}: {len(h)} hit(s){note}")
        for p, n, line in h:
            print(f"      {p}:{n}: {line[:160]}{tag(p)}")
    if globs:
        print()
        print("Glob patterns with the entity prefix (check by hand, e.g. recorder exclude):")
        for p, n, line in globs:
            print(f"      {p}:{n}: {line[:160]}{tag(p)}")
    print()
    print(f"Total: {total} hit(s) of old entity_ids, {qsh} in qsh.yaml, {len(globs)} glob line(s).")
    return 0


# =============================================================================
# Minimal WebSocket client (RFC 6455, client side, text frames only)
# =============================================================================

class WebSocket:
    def __init__(self, url, timeout=30, insecure=False):
        u = urlparse(url)
        if u.scheme not in ("ws", "wss"):
            raise ValueError("URL must start with ws:// or wss://")
        port = u.port or (443 if u.scheme == "wss" else 80)
        sock = socket.create_connection((u.hostname, port), timeout=timeout)
        if u.scheme == "wss":
            ctx = ssl.create_default_context()
            if insecure:
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE
            sock = ctx.wrap_socket(sock, server_hostname=u.hostname)
        self.sock = sock
        self.buf = b""
        key = base64.b64encode(os.urandom(16)).decode()
        host = u.hostname if u.port is None else f"{u.hostname}:{u.port}"
        req = (f"GET {u.path or '/'} HTTP/1.1\r\nHost: {host}\r\nUpgrade: websocket\r\n"
               f"Connection: Upgrade\r\nSec-WebSocket-Key: {key}\r\n"
               f"Sec-WebSocket-Version: 13\r\n\r\n")
        sock.sendall(req.encode())
        while b"\r\n\r\n" not in self.buf:
            chunk = sock.recv(4096)
            if not chunk:
                raise ConnectionError("connection closed during handshake")
            self.buf += chunk
        head, self.buf = self.buf.split(b"\r\n\r\n", 1)
        status = head.split(b"\r\n", 1)[0]
        if b" 101 " not in status + b" ":
            raise ConnectionError(f"handshake failed: {status.decode(errors='replace')}")

    def _read(self, n):
        while len(self.buf) < n:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise ConnectionError("connection closed")
            self.buf += chunk
        out, self.buf = self.buf[:n], self.buf[n:]
        return out

    def _send_frame(self, opcode, payload):
        header = bytes([0x80 | opcode])
        n = len(payload)
        if n < 126:
            header += bytes([0x80 | n])
        elif n < 65536:
            header += bytes([0x80 | 126]) + struct.pack(">H", n)
        else:
            header += bytes([0x80 | 127]) + struct.pack(">Q", n)
        mask = os.urandom(4)
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        self.sock.sendall(header + mask + masked)

    def send_json(self, obj):
        self._send_frame(0x1, json.dumps(obj).encode())

    def recv_json(self):
        message = b""
        while True:
            b1, b2 = self._read(2)
            fin, opcode = b1 & 0x80, b1 & 0x0F
            n = b2 & 0x7F
            if n == 126:
                n = struct.unpack(">H", self._read(2))[0]
            elif n == 127:
                n = struct.unpack(">Q", self._read(8))[0]
            mask = self._read(4) if b2 & 0x80 else None
            data = self._read(n)
            if mask:
                data = bytes(b ^ mask[i % 4] for i, b in enumerate(data))
            if opcode == 0x9:        # ping
                self._send_frame(0xA, data)
                continue
            if opcode == 0xA:        # pong
                continue
            if opcode == 0x8:
                raise ConnectionError("server closed the connection")
            message += data
            if fin:
                return json.loads(message.decode())

    def close(self):
        try:
            self._send_frame(0x8, b"")
        except OSError:
            pass
        self.sock.close()


class HA:
    def __init__(self, url, token, insecure=False):
        self.ws = WebSocket(url, insecure=insecure)
        self.next_id = 1
        msg = self.ws.recv_json()
        if msg.get("type") != "auth_required":
            raise ConnectionError(f"unexpected first message: {msg}")
        self.ws.send_json({"type": "auth", "access_token": token})
        msg = self.ws.recv_json()
        if msg.get("type") != "auth_ok":
            raise PermissionError(f"authentication failed: {msg.get('message', msg)}")
        self.version = msg.get("ha_version", "?")

    def call(self, msg_type, **kwargs):
        mid = self.next_id
        self.next_id += 1
        self.ws.send_json({"id": mid, "type": msg_type, **kwargs})
        while True:
            msg = self.ws.recv_json()
            if msg.get("id") == mid and msg.get("type") == "result":
                if not msg.get("success"):
                    err = msg.get("error", {})
                    raise RuntimeError(f"{msg_type}: {err.get('code')}: {err.get('message')}")
                return msg.get("result")

    def close(self):
        self.ws.close()


def connect(args):
    token = os.environ.get("HA_TOKEN")
    if not token:
        sys.exit("Set HA_TOKEN to a long-lived access token (HA profile > Security).")
    ha = HA(args.url, token, insecure=args.insecure)
    print(f"Connected to Home Assistant {ha.version}")
    return ha


def registry_by_unique_id(ha):
    entries = ha.call("config/entity_registry/list")
    by_uid = {}
    taken = {}
    for e in entries:
        taken[e["entity_id"]] = e
        if e.get("platform") == "mqtt" and e.get("unique_id", "").startswith("qsh_modbus_reg_"):
            by_uid[e["unique_id"]] = e
    return by_uid, taken


# =============================================================================
# rename
# =============================================================================

def cmd_rename(args):
    prefix = args.prefix
    ha = connect(args)
    try:
        by_uid, taken = registry_by_unique_id(ha)
        owner = {eid: e.get("unique_id") for eid, e in taken.items()}
        plan, problems = [], []
        for reg, old_slug, new_slug in RENAMES:
            uid = f"qsh_modbus_reg_{reg}"
            old_id, new_id = prefix + old_slug, prefix + new_slug
            e = by_uid.get(uid)
            if e is None:
                problems.append(f"R{reg}: no registry entry with unique_id {uid}")
                continue
            cur = e["entity_id"]
            if cur == new_id:
                print(f"R{reg:<3} already {new_id} (skip)")
                continue
            if cur != old_id:
                problems.append(f"R{reg}: entity_id is {cur}, expected {old_id} (not changed)")
                continue
            holder = owner.get(new_id)
            if holder is not None and holder != uid:
                problems.append(f"R{reg}: {new_id} is in use by unique_id {holder} (not changed)")
                continue
            # Simulate the move so later steps see the freed / taken slug.
            owner.pop(old_id, None)
            owner[new_id] = uid
            plan.append((reg, old_id, new_id))

        print()
        for reg, old_id, new_id in plan:
            print(f"R{reg:<3} {old_id} -> {new_id}")
        for p in problems:
            print(f"PROBLEM {p}")
        if not plan:
            print("Nothing to rename.")
            return 1 if problems else 0
        if not args.apply:
            print(f"\nDry run: {len(plan)} rename(s) planned. Add --apply to do them.")
            return 1 if problems else 0

        print()
        for reg, old_id, new_id in plan:
            ha.call("config/entity_registry/update", entity_id=old_id, new_entity_id=new_id)
            print(f"done R{reg:<3} {new_id}")
            if reg == 24:
                print(f"     waiting {COLLISION_WAIT_S} s for the recorder to free "
                      f"'{COLLISION_SLUG}' before R50 takes it")
                time.sleep(COLLISION_WAIT_S)
        print(f"\n{len(plan)} rename(s) done. Check the HA log for "
              "'Cannot migrate history'. Then run: verify")
        return 1 if problems else 0
    finally:
        ha.close()


# =============================================================================
# verify
# =============================================================================

def _num(state):
    try:
        return float(state)
    except (TypeError, ValueError):
        return None


def cmd_verify(args):
    prefix = args.prefix
    ha = connect(args)
    ok = True
    try:
        by_uid, _ = registry_by_unique_id(ha)
        print("\nRegistry:")
        for reg, old_slug, new_slug in RENAMES:
            e = by_uid.get(f"qsh_modbus_reg_{reg}")
            cur = e["entity_id"] if e else "(missing)"
            good = cur == prefix + new_slug
            ok &= good
            print(f"  {'OK ' if good else 'BAD'} R{reg:<3} {cur}")

        states = {s["entity_id"]: s for s in ha.call("get_states")}

        def val(slug):
            s = states.get(prefix + slug)
            return _num(s["state"]) if s else None

        print("\nCurrent values:")
        ps, te, th = val("suction_pressure"), val("saturated_evaporating_temp"), val("defrost_te_threshold")
        st = val("hp_controller_state")
        print(f"  R65 hp_controller_state       = {st}")
        print(f"  R50 suction_pressure          = {ps} bar g (D3: 2-4 running, ~4-6 OFF)")
        print(f"  R57 saturated_evaporating_temp= {te} °C")
        print(f"  R58 defrost_te_threshold      = {th} °C (D4: expect -12.0)")
        if th is not None and abs(th + 12.0) > 0.05:
            ok = False
            print("  BAD defrost_te_threshold is not -12.0")
        for slug in ("defrost_valve_position", "unknown_35"):
            v = val(slug)
            if v is not None and v > 1000:
                ok = False
                print(f"  BAD {slug} = {v} (> 1000: decoder is not int16)")
        margin = states.get(prefix + "defrost_margin")
        if margin and te is not None:
            m = _num(margin["state"])
            exp = round(te + 12.0, 1)
            good = m is not None and abs(m - exp) <= 0.15
            ok &= good
            print(f"  {'OK ' if good else 'BAD'} defrost_margin = {m} (D4: Te + 12.0 = {exp})")
        dfa = states.get(prefix.replace("sensor.", "binary_sensor.", 1) + "defrost_active")
        if dfa and st is not None:
            exp = "on" if int(st) in (6, 7, 8) else "off"
            good = dfa["state"] == exp
            ok &= good
            print(f"  {'OK ' if good else 'BAD'} defrost_active = {dfa['state']} (D5: expect {exp})")

        print(f"\nRecorder history, last {args.days} d (D7):")
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=args.days)
        for reg, slug in ((20, "electrical_power_input"), (50, "suction_pressure"),
                          (57, "saturated_evaporating_temp")):
            eid = prefix + slug
            res = ha.call("history/history_during_period", start_time=start.isoformat(),
                          end_time=end.isoformat(), entity_ids=[eid],
                          minimal_response=True, no_attributes=True,
                          significant_changes_only=False)
            rows = res.get(eid, [])
            first = None
            if rows:
                t = rows[0].get("lu") or rows[0].get("last_updated")
                if isinstance(t, (int, float)):
                    first = datetime.fromtimestamp(t, tz=timezone.utc)
                elif isinstance(t, str):
                    first = datetime.fromisoformat(t.replace("Z", "+00:00"))
            good = (len(rows) > 1 and first is not None
                    and first <= end - timedelta(hours=args.min_hours))
            ok &= good
            shown = first.isoformat(timespec="minutes") if first else None
            print(f"  {'OK ' if good else 'BAD'} R{reg:<3} {eid}: {len(rows)} states, first {shown}")
        print("\nRESULT:", "PASS" if ok else "FAIL (see BAD lines)")
        return 0 if ok else 1
    finally:
        ha.close()


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--prefix", default=DEFAULT_PREFIX, help=f"entity_id prefix (default {DEFAULT_PREFIX})")
    sub = ap.add_subparsers(dest="cmd", required=True)

    g = sub.add_parser("grep", help="find references to old entity_ids (no change)")
    g.add_argument("paths", nargs="+", help="HA config directory, add-on config directories")
    g.add_argument("--after-rename", action="store_true",
                   help=f"do not count '{COLLISION_SLUG}' (it is the reg 50 id after the rename)")
    g.set_defaults(func=cmd_grep)

    for name, func, helptext in (("rename", cmd_rename, "rename entity_ids (dry run unless --apply)"),
                                 ("verify", cmd_verify, "check registry, values and history")):
        p = sub.add_parser(name, help=helptext)
        p.add_argument("--url", default="ws://homeassistant.local:8123/api/websocket")
        p.add_argument("--insecure", action="store_true", help="wss: do not verify the certificate")
        p.set_defaults(func=func)
        if name == "rename":
            p.add_argument("--apply", action="store_true", help="do the renames")
        else:
            p.add_argument("--days", type=int, default=7, help="history window (default 7)")
            p.add_argument("--min-hours", type=float, default=24,
                           help="first state must be at least this old (default 24)")

    args = ap.parse_args()
    sys.exit(args.func(args))


if __name__ == "__main__":
    main()
