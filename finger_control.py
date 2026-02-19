#!/usr/bin/env python3
# Python 3.8 compatible
# pip install pyserial loguru pyyaml
#
# Usage:
#   python finger_control.py fingers.yaml
#
# YAML example:
# outputs_root: outputs
# baud: 115200
# cfg_timeout_s: 6.0
# boot_delay_s: 2.0
# pose_timeout_s: 0.8
# fingers:
#   - index: 1
#     port: /dev/ttyUSB0
#     name: finger_1

import os
import sys
import time
import json
import csv
import queue
import re
import threading
import multiprocessing as mp
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Any

import serial
import yaml
from loguru import logger


# -----------------------------
# Helpers
# -----------------------------


def now_datestr() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def now_timestr() -> str:
    return datetime.now().strftime("%H-%M-%S")


def now_stamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def write_raw_line(path: str, direction: str, payload: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    with open(path, "a", encoding="utf-8") as f:
        f.write("{} | {} | {}\n".format(ts, direction, payload.rstrip()))


def parse_kv_line(line: str) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
    """
    Parses:
      CFG,k=v,k2=v2
      STAT,ms=...,finger=1,...
      POSE,finger=1,D=...,S1=...,S2=...
      ACK,S1
      ERR,...
    Returns (tag, dict)
    """
    line = line.strip()
    if not line:
        return None, None
    parts = line.split(",")
    tag = parts[0].strip()
    kv: Dict[str, Any] = {}
    for p in parts[1:]:
        p = p.strip()
        if not p:
            continue
        if "=" in p:
            k, v = p.split("=", 1)
            kv[k.strip()] = v.strip()
        else:
            kv.setdefault("_", []).append(p)
    return tag, kv


def sanitize_name(s: str) -> str:
    s = s.strip()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^A-Za-z0-9_\-]+", "", s)
    return s[:80] if s else "unnamed_pose"


# -----------------------------
# Process-side worker
# -----------------------------


@dataclass
class FingerProcConfig:
    port: str
    baud: int
    finger_index: int
    cfg_timeout_s: float  # total handshake budget
    boot_delay_s: float  # time to wait after reset/open
    pose_timeout_s: float  # how long to wait for POSE after SAVE_POSE
    outputs_root: str
    folder_name: str  # e.g., "finger_1"


def finger_worker(cfg: FingerProcConfig, cmd_q: mp.Queue, evt_q: mp.Queue) -> None:
    """
    One process per finger.

    - Opens serial (may reset Arduino)
    - Performs warmup handshake:
        retries WARMUP every 0.5s until CFG arrives (within cfg_timeout_s)
        then if still no CFG, tries CFG? for 1s
    - Supports:
        TEACH_ALL / TEACH_FINGER
        SAVE_POSE -> expects POSE tag; fallback uses latest STAT if needed
    - Logs:
        serial_raw.log (RX/TX)
        events.log (python-side)
        cfg.json
        stat.csv (STAT rows)
    """
    date_dir = now_datestr()
    time_dir = now_timestr()
    out_dir = os.path.join(cfg.outputs_root, date_dir, time_dir, cfg.folder_name)
    ensure_dir(out_dir)

    serial_log_path = os.path.join(out_dir, "serial_raw.log")
    events_log_path = os.path.join(out_dir, "events.log")
    cfg_json_path = os.path.join(out_dir, "cfg.json")
    stat_csv_path = os.path.join(out_dir, "stat.csv")

    logger.remove()
    logger.add(
        events_log_path, level="INFO", enqueue=False, backtrace=False, diagnose=False
    )

    def rawlog(direction: str, s: str) -> None:
        write_raw_line(serial_log_path, direction, s)

    def info_evt(msg: str) -> None:
        evt_q.put(("INFO", "{}:{}".format(cfg.finger_index, msg)))

    def error_evt(msg: str) -> None:
        evt_q.put(("ERROR", "{}:{}".format(cfg.finger_index, msg)))

    started = False
    cfg_dict: Optional[Dict[str, Any]] = None

    last_stat_kv: Optional[Dict[str, Any]] = None
    last_pose_kv: Optional[Dict[str, Any]] = None

    # Open serial
    try:
        ser = serial.Serial(cfg.port, cfg.baud, timeout=0.02)
    except Exception as e:
        logger.exception("Failed to open {}: {}".format(cfg.port, e))
        evt_q.put(("FATAL", "{}:open_failed:{}".format(cfg.finger_index, e)))
        return

    logger.info("Opened {} @ {}, out_dir={}".format(cfg.port, cfg.baud, out_dir))
    info_evt("opened:{}".format(cfg.port))

    # CSV setup
    csv_file = open(stat_csv_path, "a", newline="", encoding="utf-8")
    csv_writer = None  # type: Optional[csv.DictWriter]
    csv_fieldnames = None  # type: Optional[List[str]]

    def write_csv_row(stat_kv: Dict[str, Any]) -> None:
        nonlocal csv_writer, csv_fieldnames
        row: Dict[str, Any] = {
            "host_ts": datetime.now().isoformat(timespec="milliseconds")
        }
        row.update(stat_kv)

        if csv_writer is None:
            keys = sorted(row.keys())
            if "host_ts" in keys:
                keys.remove("host_ts")
            csv_fieldnames = ["host_ts"] + keys
            csv_writer = csv.DictWriter(csv_file, fieldnames=csv_fieldnames)
            csv_writer.writeheader()

        assert csv_fieldnames is not None
        full_row = {k: row.get(k, "") for k in csv_fieldnames}
        csv_writer.writerow(full_row)
        csv_file.flush()

    def read_line_nonblock() -> str:
        try:
            return ser.readline().decode("utf-8", errors="replace").strip()
        except Exception:
            return ""

    def send_line(s: str) -> None:
        try:
            ser.write(s.encode("utf-8"))
            ser.flush()
            rawlog("TX", s.strip())
        except Exception as e:
            logger.error("TX failed: {}".format(e))
            error_evt("tx_failed:{}".format(e))

    # ---- DTR / reset handling ----
    try:
        ser.dtr = False
        time.sleep(0.2)
        ser.reset_input_buffer()
        ser.reset_output_buffer()
        ser.dtr = True
    except Exception:
        pass

    time.sleep(cfg.boot_delay_s)

    try:
        ser.reset_input_buffer()
        ser.reset_output_buffer()
    except Exception:
        pass

    # Drain startup chatter
    t0 = time.time()
    while time.time() - t0 < 1.0:
        line = read_line_nonblock()
        if line:
            rawlog("RX", line)
            evt_q.put(("LINE", (cfg.finger_index, line)))

    # ---- Warmup handshake ----
    send_line("WARMUP\n")
    last_send = time.time()
    overall_deadline = time.time() + cfg.cfg_timeout_s

    while time.time() < overall_deadline:
        now = time.time()
        if now - last_send >= 0.5:
            send_line("WARMUP\n")
            last_send = now

        line = read_line_nonblock()
        if not line:
            continue

        rawlog("RX", line)
        tag, kv = parse_kv_line(line)

        if tag == "CFG" and kv is not None:
            cfg_dict = kv

            if "finger" in cfg_dict:
                try:
                    got_idx = int(cfg_dict["finger"])
                    if got_idx != cfg.finger_index:
                        logger.error(
                            "Finger index mismatch: expected {}, got {}".format(
                                cfg.finger_index, got_idx
                            )
                        )
                        error_evt("finger_mismatch:{}".format(got_idx))
                except Exception:
                    pass

            try:
                with open(cfg_json_path, "w", encoding="utf-8") as f:
                    json.dump(cfg_dict, f, indent=2)
                logger.info("CFG saved")
            except Exception as e:
                logger.error("Failed to write cfg.json: {}".format(e))
                error_evt("cfg_json_write_failed:{}".format(e))

            evt_q.put(("CFG", (cfg.finger_index, cfg_dict)))
            break

        evt_q.put(("LINE", (cfg.finger_index, line)))

    # Last resort: CFG? for 1s
    if cfg_dict is None:
        send_line("CFG?\n")
        t1 = time.time()
        while time.time() - t1 < 1.0:
            line = read_line_nonblock()
            if not line:
                continue
            rawlog("RX", line)
            tag, kv = parse_kv_line(line)
            if tag == "CFG" and kv is not None:
                cfg_dict = kv
                try:
                    with open(cfg_json_path, "w", encoding="utf-8") as f:
                        json.dump(cfg_dict, f, indent=2)
                except Exception:
                    pass
                evt_q.put(("CFG", (cfg.finger_index, cfg_dict)))
                break
            evt_q.put(("LINE", (cfg.finger_index, line)))

    if cfg_dict is None:
        logger.error("CFG timeout after WARMUP/CFG?")
        error_evt("cfg_timeout")

    def handle_incoming(line: str) -> None:
        nonlocal last_stat_kv, last_pose_kv
        rawlog("RX", line)
        tag, kv = parse_kv_line(line)

        if tag == "STAT" and kv is not None:
            last_stat_kv = kv
            write_csv_row(kv)
            evt_q.put(("STAT", (cfg.finger_index, kv)))

        elif tag == "POSE" and kv is not None:
            last_pose_kv = kv
            evt_q.put(("POSE", (cfg.finger_index, kv)))

        elif tag == "ACK":
            ack = ""
            if kv is not None:
                ack = (kv.get("_", [""]) or [""])[0]
            evt_q.put(("ACK", (cfg.finger_index, ack)))

        elif tag == "ERR":
            evt_q.put(("ERR", (cfg.finger_index, line)))

        else:
            evt_q.put(("LINE", (cfg.finger_index, line)))

    def wait_for_pose_or_fallback(timeout_s: float) -> Dict[str, Any]:
        """
        Try to get POSE within timeout. If not, fallback using latest STAT fields:
          D <- dc_pos
          S1 <- s1_fb
          S2 <- s2_fb
        """
        nonlocal last_pose_kv, last_stat_kv
        last_pose_kv = None

        deadline = time.time() + timeout_s
        while time.time() < deadline:
            line = read_line_nonblock()
            if line:
                handle_incoming(line)
                if last_pose_kv is not None:
                    return dict(last_pose_kv)
            else:
                time.sleep(0.005)

        fb = {}
        if last_stat_kv is not None:
            fb = dict(last_stat_kv)

        pose: Dict[str, Any] = {
            "finger": str(cfg.finger_index),
            "D": fb.get("dc_pos", ""),
            "S1": fb.get("s1_fb", ""),
            "S2": fb.get("s2_fb", ""),
            "source": "fallback_stat",
        }
        return pose

    # ---- Main loop ----
    try:
        while True:
            # pump serial
            line = read_line_nonblock()
            if line:
                handle_incoming(line)

            # handle commands
            try:
                cmd = cmd_q.get_nowait()
            except queue.Empty:
                cmd = None

            if cmd is None:
                continue

            op = cmd.get("op")
            if op == "CLOSE":
                logger.info("CLOSE")
                info_evt("closing")
                break

            if op == "START":
                send_line("START\n")
                started = True
                info_evt("start_sent")
                continue

            if op == "STOP":
                send_line("STOP\n")
                started = False
                info_evt("stop_sent")
                continue

            if op == "CFG?":
                send_line("CFG?\n")
                continue

            if op == "MODE?":
                send_line("MODE?\n")
                continue

            if op == "TEACH_ALL":
                send_line("TEACH_ALL\n")
                started = False
                info_evt("teach_all_sent")
                continue

            if op == "TEACH_FINGER":
                send_line("TEACH_FINGER\n")
                started = False
                info_evt("teach_finger_sent")
                continue

            if op == "SAVE_POSE":
                send_line("SAVE_POSE\n")
                pose = wait_for_pose_or_fallback(cfg.pose_timeout_s)
                evt_q.put(("POSE_SNAPSHOT", (cfg.finger_index, pose)))
                continue

            if op == "SET_TARGETS":
                if not started:
                    msg = "not_started: call START first"
                    evt_q.put(("PYERR", (cfg.finger_index, msg)))
                    continue
                motor = cmd["motor_deg"]
                s1 = cmd["s1_deg"]
                s2 = cmd["s2_deg"]
                send_line("D {},S1 {},S2 {}\n".format(motor, s1, s2))
                continue

            error_evt("unknown_op:{}".format(op))

    except Exception as e:
        logger.exception("Worker crash: {}".format(e))
        evt_q.put(("FATAL", "{}:worker_crash:{}".format(cfg.finger_index, e)))
    finally:
        try:
            csv_file.close()
        except Exception:
            pass
        try:
            ser.close()
        except Exception:
            pass


# -----------------------------
# Parent-side manager
# -----------------------------


class FingerManager:
    def __init__(
        self,
        fingers_cfg: List[Dict[str, Any]],
        outputs_root: str,
        baud: int,
        cfg_timeout_s: float,
        boot_delay_s: float,
        pose_timeout_s: float,
    ):
        self.fingers_cfg = fingers_cfg
        self.outputs_root = outputs_root
        self.baud = baud
        self.cfg_timeout_s = cfg_timeout_s
        self.boot_delay_s = boot_delay_s
        self.pose_timeout_s = pose_timeout_s

        self.cmd_qs = {}  # type: Dict[int, mp.Queue]
        self.proc = {}  # type: Dict[int, mp.Process]
        self.evt_q = mp.Queue()  # type: mp.Queue

        self.cfg_dicts = {}  # type: Dict[int, Dict[str, Any]]
        self.latest_stat = {}  # type: Dict[int, Dict[str, Any]]
        self.latest_pose = {}  # type: Dict[int, Dict[str, Any]]
        self.started = set()  # type: set

        self._evt_stop = threading.Event()
        self._evt_thread = None  # type: Optional[threading.Thread]

        self.indices = []  # type: List[int]
        for f in fingers_cfg:
            idx = int(f["index"])
            self.indices.append(idx)
        self.indices.sort()

    def start_processes(self) -> None:
        for f in self.fingers_cfg:
            idx = int(f["index"])
            port = str(f["port"])
            folder_name = str(f.get("name", "finger_{}".format(idx)))

            cfg = FingerProcConfig(
                port=port,
                baud=self.baud,
                finger_index=idx,
                cfg_timeout_s=self.cfg_timeout_s,
                boot_delay_s=self.boot_delay_s,
                pose_timeout_s=self.pose_timeout_s,
                outputs_root=self.outputs_root,
                folder_name=folder_name,
            )

            q = mp.Queue()
            p = mp.Process(target=finger_worker, args=(cfg, q, self.evt_q), daemon=True)
            p.start()
            self.cmd_qs[idx] = q
            self.proc[idx] = p

        self._evt_stop.clear()
        self._evt_thread = threading.Thread(target=self._evt_loop, daemon=True)
        self._evt_thread.start()

    def _evt_loop(self) -> None:
        while not self._evt_stop.is_set():
            try:
                typ, payload = self.evt_q.get(timeout=0.1)
            except queue.Empty:
                continue

            if typ == "CFG":
                idx, d = payload
                self.cfg_dicts[idx] = d

            elif typ == "STAT":
                idx, d = payload
                self.latest_stat[idx] = d

            elif typ == "POSE":
                idx, d = payload
                self.latest_pose[idx] = d

            elif typ == "POSE_SNAPSHOT":
                idx, d = payload
                self.latest_pose[idx] = d

            elif typ == "PYERR":
                idx, msg = payload
                print("[PYERR finger {}] {}".format(idx, msg))

            elif typ == "ERROR":
                print("[ERROR] {}".format(payload))

            elif typ == "FATAL":
                print("[FATAL] {}".format(payload))

            # LINE ignored by default

    def wait_for_cfg(self, total_timeout_s: float) -> None:
        deadline = time.time() + total_timeout_s
        need = set(self.indices)
        while time.time() < deadline:
            got = set(self.cfg_dicts.keys())
            if got >= need:
                return
            time.sleep(0.05)

    def start_all(self) -> None:
        for idx in self.indices:
            self.cmd_qs[idx].put({"op": "START"})
            self.started.add(idx)

    def stop_all(self) -> None:
        for idx in self.indices:
            self.cmd_qs[idx].put({"op": "STOP"})
        self.started.clear()

    def teach_all(self) -> None:
        for idx in self.indices:
            self.cmd_qs[idx].put({"op": "TEACH_ALL"})
        self.started.clear()

    def teach_finger(self) -> None:
        for idx in self.indices:
            self.cmd_qs[idx].put({"op": "TEACH_FINGER"})
        self.started.clear()

    def set_targets_all(
        self, targets_by_idx: Dict[int, Tuple[float, float, float]]
    ) -> None:
        missing = [i for i in self.indices if i not in self.started]
        if missing:
            raise RuntimeError(
                "Not started fingers: {}. Call START first.".format(missing)
            )

        for idx in self.indices:
            motor, s1, s2 = targets_by_idx[idx]
            self.cmd_qs[idx].put(
                {"op": "SET_TARGETS", "motor_deg": motor, "s1_deg": s1, "s2_deg": s2}
            )

    # -----------------------
    # IMPORTANT FIX HERE
    # -----------------------
    def request_pose_all(self) -> None:
        """
        Broadcast SAVE_POSE to all fingers.
        IMPORTANT: clear cached pose values first so wait_for_pose waits for fresh POSE.
        """
        for idx in self.indices:
            if idx in self.latest_pose:
                del self.latest_pose[idx]

        for idx in self.indices:
            self.cmd_qs[idx].put({"op": "SAVE_POSE"})

    def wait_for_pose(self, timeout_s: float) -> Dict[int, Dict[str, Any]]:
        deadline = time.time() + timeout_s
        need = set(self.indices)
        while time.time() < deadline:
            got = set(self.latest_pose.keys())
            if got >= need:
                return {i: dict(self.latest_pose[i]) for i in self.indices}
            time.sleep(0.02)

        # partial best-effort
        return {
            i: dict(self.latest_pose[i]) for i in self.indices if i in self.latest_pose
        }

    def close(self) -> None:
        for idx in self.indices:
            try:
                self.cmd_qs[idx].put({"op": "CLOSE"})
            except Exception:
                pass

        self._evt_stop.set()
        if self._evt_thread is not None:
            self._evt_thread.join(timeout=1.0)

        for _, p in self.proc.items():
            p.join(timeout=1.0)


# -----------------------------
# Interactive helpers
# -----------------------------


def parse_finger_command_block(block: str) -> Tuple[int, float, float, float]:
    """
    Parses "finger N: D <m>,S1 <a>,S2 <b>"
    """
    block = block.strip()
    if not block:
        raise ValueError("empty block")
    if ":" not in block:
        raise ValueError("Missing ':' in block: {}".format(block))

    left, right = block.split(":", 1)
    left = left.strip()
    right = right.strip()

    parts = left.split()
    if len(parts) != 2 or parts[0].lower() != "finger":
        raise ValueError("Bad finger header (use 'finger N:'): {}".format(left))
    idx = int(parts[1])

    motor = None  # type: Optional[float]
    s1 = None  # type: Optional[float]
    s2 = None  # type: Optional[float]

    tokens = [t.strip() for t in right.split(",") if t.strip()]
    for t in tokens:
        tl = t.lower()
        if tl.startswith("d"):
            motor = float(t[1:].strip())
        elif tl.startswith("s1"):
            s1 = float(t[2:].strip())
        elif tl.startswith("s2"):
            s2 = float(t[2:].strip())
        else:
            raise ValueError("Unrecognized token: '{}'".format(t))

    if motor is None or s1 is None or s2 is None:
        raise ValueError(
            "Command must include motor + S1 + S2. Got motor={}, s1={}, s2={}".format(
                motor, s1, s2
            )
        )

    return idx, motor, s1, s2


def parse_multi_finger_line(line: str) -> Dict[int, Tuple[float, float, float]]:
    blocks = [b.strip() for b in line.split(";") if b.strip()]
    out = {}  # type: Dict[int, Tuple[float, float, float]]
    for b in blocks:
        idx, motor, s1, s2 = parse_finger_command_block(b)
        if idx in out:
            raise ValueError("Duplicate finger {} in input.".format(idx))
        out[idx] = (motor, s1, s2)
    return out


def pose_dict_to_targets(
    poses: Dict[int, Dict[str, Any]],
) -> Dict[int, Tuple[float, float, float]]:
    """
    poses[i] contains keys: D, S1, S2 (strings typically)
    """
    out: Dict[int, Tuple[float, float, float]] = {}
    for idx, d in poses.items():
        try:
            m = float(d.get("D", 0.0))
            s1 = float(d.get("S1", 0.0))
            s2 = float(d.get("S2", 0.0))
        except Exception as e:
            raise RuntimeError(
                "Bad pose values for finger {}: {} ({})".format(idx, d, e)
            )
        out[idx] = (m, s1, s2)
    return out


def poses_to_command_line(
    indices: List[int], targets_by_idx: Dict[int, Tuple[float, float, float]]
) -> str:
    parts = []
    for idx in indices:
        m, s1, s2 = targets_by_idx[idx]
        parts.append("finger {}: D {},S1 {},S2 {}".format(idx, m, s1, s2))
    return "; ".join(parts)


# -----------------------------
# YAML loading + main
# -----------------------------


def load_yaml(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python finger_control.py fingers.yaml")
        raise SystemExit(1)

    yaml_path = sys.argv[1]
    cfg = load_yaml(yaml_path)

    outputs_root = str(cfg.get("outputs_root", "outputs"))
    baud = int(cfg.get("baud", 115200))
    cfg_timeout_s = float(cfg.get("cfg_timeout_s", 6.0))
    boot_delay_s = float(cfg.get("boot_delay_s", 2.0))
    pose_timeout_s = float(cfg.get("pose_timeout_s", 0.8))

    fingers = cfg.get("fingers", [])
    if not fingers:
        raise RuntimeError("YAML has no 'fingers:' entries.")

    mgr = FingerManager(
        fingers_cfg=fingers,
        outputs_root=outputs_root,
        baud=baud,
        cfg_timeout_s=cfg_timeout_s,
        boot_delay_s=boot_delay_s,
        pose_timeout_s=pose_timeout_s,
    )
    mgr.start_processes()

    total_wait = max(8.0, boot_delay_s + cfg_timeout_s + 3.0)
    mgr.wait_for_cfg(total_timeout_s=total_wait)

    print(
        "warmup done (best-effort), CFG received for:",
        sorted(list(mgr.cfg_dicts.keys())),
    )
    print("Expected fingers:", mgr.indices)
    print("")
    print("Commands:")
    print("  START")
    print("  STOP")
    print("  TEACH_ALL")
    print("  TEACH_FINGER")
    print("  SAVE_POSE  (captures pose -> holds pose -> ask name or reteach)")
    print("  finger 1: D 0,S1 90,S2 90; finger 2: D 10,S1 80,S2 100")
    print("  quit")

    expected_set = set(mgr.indices)

    saved_pose_dir = os.path.join(outputs_root, "saved_pose")
    ensure_dir(saved_pose_dir)

    def do_reteach_prompt() -> None:
        while True:
            sel = input("Reteach mode? Enter 1=TEACH_ALL, 2=TEACH_FINGER: ").strip()
            if sel == "1":
                mgr.teach_all()
                print("Sent TEACH_ALL to all fingers.")
                return
            if sel == "2":
                mgr.teach_finger()
                print("Sent TEACH_FINGER to all fingers.")
                return
            print("Invalid input. Please enter 1 or 2.")

    try:
        while True:
            line = input("\n> ").strip()
            if not line:
                continue
            low = line.lower()

            if low in ("quit", "exit", "q"):
                break

            if low == "start":
                mgr.start_all()
                print("Sent START to all fingers.")
                continue

            if low == "stop":
                mgr.stop_all()
                print("Sent STOP to all fingers.")
                continue

            if low == "teach_all":
                mgr.teach_all()
                print("Sent TEACH_ALL to all fingers.")
                continue

            if low == "teach_finger":
                mgr.teach_finger()
                print("Sent TEACH_FINGER to all fingers.")
                continue

            if low == "save_pose":
                # 1) capture pose snapshots (fresh)
                mgr.request_pose_all()
                poses = mgr.wait_for_pose(timeout_s=max(1.5, pose_timeout_s + 0.7))

                got_set = set(poses.keys())
                if got_set != expected_set:
                    print("[ERROR] Did not receive POSE from all fingers.")
                    print("  Got:", sorted(list(got_set)))
                    print("  Expected:", sorted(list(expected_set)))
                    do_reteach_prompt()
                    continue

                # 2) build targets + command line
                targets = pose_dict_to_targets(poses)
                cmdline = poses_to_command_line(mgr.indices, targets)

                print("\nCaptured pose:")
                for idx in mgr.indices:
                    m, s1, s2 = targets[idx]
                    print("  finger {}: D {}, S1 {}, S2 {}".format(idx, m, s1, s2))
                print("\nPasteable command:")
                print("  " + cmdline)

                # 3) hold pose: START + SET_TARGETS
                mgr.start_all()
                time.sleep(0.15)
                mgr.set_targets_all(targets)
                print("\nHolding pose now (START + SET_TARGETS).")

                # 4) ask name or reteach
                ans = input(
                    "Enter pose name to SAVE, or type 'reteach' to discard: "
                ).strip()
                if ans.lower() == "reteach":
                    print("Discarded pose.")
                    do_reteach_prompt()
                    continue

                pose_name = sanitize_name(ans)
                ts = now_stamp()
                out_path = os.path.join(
                    saved_pose_dir, "{}_{}.json".format(ts, pose_name)
                )

                payload = {
                    "timestamp": datetime.now().isoformat(timespec="seconds"),
                    "pose_name": pose_name,
                    "command": cmdline,
                    "poses_by_finger": {
                        str(i): {
                            "D": targets[i][0],
                            "S1": targets[i][1],
                            "S2": targets[i][2],
                        }
                        for i in mgr.indices
                    },
                    "cfg_by_finger": {
                        str(i): mgr.cfg_dicts.get(i, {}) for i in mgr.indices
                    },
                    "run_config": {
                        "yaml_path": yaml_path,
                        "outputs_root": outputs_root,
                        "baud": baud,
                        "cfg_timeout_s": cfg_timeout_s,
                        "boot_delay_s": boot_delay_s,
                        "pose_timeout_s": pose_timeout_s,
                        "fingers": fingers,
                    },
                }

                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(payload, f, indent=2)

                print("Saved pose JSON to:", out_path)
                continue

            # Otherwise treat as a multi-finger target line
            try:
                targets = parse_multi_finger_line(line)
            except Exception as e:
                print("[ERROR] Parse failed:", e)
                continue

            got_set = set(targets.keys())
            if got_set != expected_set:
                missing = sorted(list(expected_set - got_set))
                extra = sorted(list(got_set - expected_set))
                print("[ERROR] Finger set mismatch.")
                if missing:
                    print("  Missing:", missing)
                if extra:
                    print("  Extra:", extra)
                print("  Expected exactly:", sorted(list(expected_set)))
                continue

            print("\nWill send these targets:")
            for idx in sorted(list(targets.keys())):
                m, s1, s2 = targets[idx]
                print("  finger {}: D {}, S1 {}, S2 {}".format(idx, m, s1, s2))

            confirm = (
                input("Press Enter to CONFIRM send (or type 'cancel' to abort): ")
                .strip()
                .lower()
            )
            if confirm == "cancel":
                print("Cancelled.")
                continue
            if confirm != "":
                print("Cancelled (did not press Enter).")
                continue

            try:
                mgr.set_targets_all(targets)
                print("Sent.")
            except Exception as e:
                print("[ERROR] Send failed:", e)

    finally:
        mgr.close()


if __name__ == "__main__":
    mp.set_start_method("spawn", force=True)
    main()
