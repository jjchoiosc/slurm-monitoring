#!/usr/bin/env python3
"""Slurm command monitor API and static file server."""

from __future__ import annotations

import json
import socket
import os
import re
import shlex
import subprocess
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse
from urllib import error as urlerror
from urllib import request as urlrequest
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parent
STATIC_DIR = ROOT / "static"
DEFAULT_PORT = int(os.environ.get("SLURM_MONITOR_PORT", "8120"))
EXEC_TIMEOUT = int(os.environ.get("SLURM_MONITOR_TIMEOUT", "30"))
CLUSTER_NAME = os.environ.get("SLURM_MONITOR_CLUSTER_NAME", "Slurm Cluster")
PRIMARY_IP = os.environ.get("SLURM_PRIMARY_IP", "")
BACKUP_IP = os.environ.get("SLURM_BACKUP_IP", "")
SLURM_CONF_PATH = Path(os.environ.get("SLURM_CONF_PATH", "/etc/slurm/slurm.conf"))
SLURM_CONF_MAX_BYTES = int(os.environ.get("SLURM_CONF_MAX_BYTES", str(512 * 1024)))
SLURM_RESTART_SERVICE = os.environ.get("SLURM_RESTART_SERVICE", "slurmctld")
PEER_HTTP_TIMEOUT = float(os.environ.get("SLURM_MONITOR_PEER_TIMEOUT", "8"))

_VERSION_CACHE: dict[str, Any] = {"value": None, "ts": 0.0}
_PEER_VERSION_CACHE: dict[str, dict[str, Any]] = {}
VERSION_CACHE_TTL = int(os.environ.get("SLURM_MONITOR_VERSION_TTL", "60"))

ARG_PATTERN = re.compile(r"^[A-Za-z0-9_./:=+\-,*@% ]+$")
BLOCKED_TOKENS = {";", "|", "&", "`", "$", "(", ")", "<", ">", "\n", "\r"}

WARN_STATE_TOKENS = ("drain", "maint", "power_down", "power_up")
CRIT_STATE_TOKENS = ("down", "fail", "unk", "error", "invalid")
WARN_REASON_TOKENS = ("low ", "kill", "boot", "power")
CRIT_REASON_TOKENS = ("not responding", "down")

SUPPORTED_LANGS = ("ko", "en")
DEFAULT_LANG = "ko"


def parse_lang(value: str | None) -> str:
    if value and value.lower().startswith("en"):
        return "en"
    return DEFAULT_LANG


def pick_text(value: Any, lang: str) -> str:
    if isinstance(value, dict):
        return str(value.get(lang) or value.get(DEFAULT_LANG) or value.get("en") or "")
    return str(value)


def localize_commands(lang: str) -> dict[str, dict[str, Any]]:
    lang = parse_lang(lang)
    localized: dict[str, dict[str, Any]] = {}
    for name, meta in SLURM_COMMANDS.items():
        item = {key: value for key, value in meta.items() if key not in {"summary", "presets"}}
        item["summary"] = pick_text(meta.get("summary", ""), lang)
        item["presets"] = []
        for preset in meta.get("presets", []):
            preset_item = {key: value for key, value in preset.items() if key != "label"}
            preset_item["label"] = pick_text(preset.get("label", ""), lang)
            if "note" in preset:
                preset_item["note"] = pick_text(preset["note"], lang)
            item["presets"].append(preset_item)
        localized[name] = item
    return localized


ALERT_TEXT = {
    "ko": {
        "needs_attention": "노드 {count}개에 점검이 필요합니다",
        "all_ok": "모든 노드가 정상입니다",
    },
    "en": {
        "needs_attention": "{count} node(s) need attention",
        "all_ok": "All nodes are healthy",
    },
}


SLURM_COMMANDS: dict[str, dict[str, Any]] = {
    "sinfo": {
        "title": "sinfo",
        "summary": {
            "ko": "파티션과 노드 상태를 한눈에 확인합니다.",
            "en": "View partition and node status at a glance.",
        },
        "category": "monitor",
        "presets": [
            {"label": {"ko": "요약", "en": "Summary"}, "args": ["-s"]},
            {"label": {"ko": "노드 상세", "en": "Node details"}, "args": ["-N", "-l"]},
            {"label": {"ko": "파티션", "en": "Partitions"}, "args": ["-o", "%P %a %l %D %t %N"]},
            {"label": {"ko": "장애 원인", "en": "Failure reasons"}, "args": ["-R"]},
        ],
    },
    "squeue": {
        "title": "squeue",
        "summary": {
            "ko": "현재 대기열의 작업을 조회합니다.",
            "en": "Inspect jobs in the current queue.",
        },
        "category": "monitor",
        "presets": [
            {"label": {"ko": "상세 형식", "en": "Long format"}, "args": ["-l"]},
            {"label": {"ko": "모든 파티션", "en": "All partitions"}, "args": ["-a"]},
            {"label": {"ko": "실행 중인 작업", "en": "Running jobs"}, "args": ["-t", "RUNNING"]},
            {"label": {"ko": "대기 중인 작업", "en": "Pending jobs"}, "args": ["-t", "PENDING"]},
        ],
    },
    "scontrol": {
        "title": "scontrol",
        "summary": {
            "ko": "클러스터 설정과 객체 상세 정보를 조회합니다.",
            "en": "Inspect cluster configuration and object details.",
        },
        "category": "monitor",
        "presets": [
            {"label": {"ko": "노드 조회", "en": "Show nodes"}, "args": ["show", "nodes"]},
            {"label": {"ko": "파티션 조회", "en": "Show partitions"}, "args": ["show", "partition"]},
            {"label": {"ko": "작업 조회", "en": "Show jobs"}, "args": ["show", "jobs"]},
            {"label": {"ko": "설정 조회", "en": "Show config"}, "args": ["show", "config"]},
            {"label": {"ko": "예약 조회", "en": "Show reservations"}, "args": ["show", "reservation"]},
        ],
    },
    "sdiag": {
        "title": "sdiag",
        "summary": {
            "ko": "Slurm 컨트롤러 진단과 RPC 통계를 조회합니다.",
            "en": "Inspect Slurm controller diagnostics and RPC statistics.",
        },
        "category": "monitor",
        "presets": [
            {"label": {"ko": "전체 통계", "en": "All statistics"}, "args": ["-a"]},
            {"label": {"ko": "형식 목록", "en": "Format list"}, "args": ["-f", "all"]},
        ],
    },
    "sprio": {
        "title": "sprio",
        "summary": {
            "ko": "작업 우선순위 구성 요소를 조회합니다.",
            "en": "Inspect job priority components.",
        },
        "category": "monitor",
        "presets": [
            {"label": {"ko": "상세 형식", "en": "Long format"}, "args": ["-l"]},
            {"label": {"ko": "가중치", "en": "Weights"}, "args": ["-w"]},
        ],
    },
    "sshare": {
        "title": "sshare",
        "summary": {
            "ko": "Fair-share 할당 정보를 조회합니다.",
            "en": "Inspect fair-share allocation data.",
        },
        "category": "monitor",
        "presets": [
            {"label": {"ko": "모든 계정", "en": "All accounts"}, "args": ["-a"]},
            {"label": {"ko": "상세 형식", "en": "Long format"}, "args": ["-l"]},
        ],
    },
    "sstat": {
        "title": "sstat",
        "summary": {
            "ko": "실행 중인 작업의 통계를 조회합니다. 작업 ID가 필요합니다.",
            "en": "Inspect statistics for running jobs. A job ID is required.",
        },
        "category": "monitor",
        "presets": [
            {
                "label": {"ko": "도움말", "en": "Help"},
                "args": ["--help"],
                "note": {
                    "ko": "사용자 인자 예: -j <jobid> --format=JobID,MaxRSS,AveCPU",
                    "en": "Example args: -j <jobid> --format=JobID,MaxRSS,AveCPU",
                },
            },
        ],
    },
    "strigger": {
        "title": "strigger",
        "summary": {
            "ko": "Slurm에 설정된 이벤트 트리거를 조회합니다.",
            "en": "Inspect event triggers configured in Slurm.",
        },
        "category": "monitor",
        "presets": [
            {"label": {"ko": "트리거 목록", "en": "List triggers"}, "args": ["--get"]},
            {"label": {"ko": "트리거 상세", "en": "Trigger details"}, "args": ["--get", "-v"]},
        ],
    },
    "sacct": {
        "title": "sacct",
        "summary": {
            "ko": "완료된 작업의 사용 기록을 조회합니다.",
            "en": "Inspect accounting records for completed jobs.",
        },
        "category": "accounting",
        "presets": [
            {"label": {"ko": "오늘 작업", "en": "Today's jobs"}, "args": ["-S", "today", "-n", "-P", "--format=JobID,JobName,State,Elapsed"]},
            {"label": {"ko": "최근 24시간", "en": "Last 24 hours"}, "args": ["-S", "now-1day", "-n", "-P", "--format=JobID,JobName,State,Elapsed"]},
            {"label": {"ko": "도움말", "en": "Help"}, "args": ["--help"]},
        ],
    },
    "sacctmgr": {
        "title": "sacctmgr",
        "summary": {
            "ko": "Slurm 계정 데이터베이스를 읽기 전용으로 조회합니다.",
            "en": "Read-only inspection of the Slurm accounting database.",
        },
        "category": "accounting",
        "read_only": True,
        "presets": [
            {"label": {"ko": "클러스터 조회", "en": "Show clusters"}, "args": ["show", "cluster"]},
            {"label": {"ko": "계정 조회", "en": "Show accounts"}, "args": ["show", "account"]},
            {"label": {"ko": "사용자 조회", "en": "Show users"}, "args": ["show", "user"]},
            {"label": {"ko": "QoS 조회", "en": "Show QoS"}, "args": ["show", "qos"]},
        ],
    },
    "sreport": {
        "title": "sreport",
        "summary": {
            "ko": "클러스터 가동률과 사용량 보고서를 조회합니다.",
            "en": "Generate cluster utilization and usage reports.",
        },
        "category": "accounting",
        "presets": [
            {"label": {"ko": "클러스터 가동률", "en": "Cluster utilization"}, "args": ["cluster", "utilization", "-t", "1"]},
            {"label": {"ko": "작업 크기", "en": "Job sizes"}, "args": ["job", "sizes", "-t", "1"]},
            {"label": {"ko": "도움말", "en": "Help"}, "args": ["--help"]},
        ],
    },
    "sbatch": {
        "title": "sbatch",
        "summary": {
            "ko": "배치 작업을 제출합니다.",
            "en": "Submit batch jobs.",
        },
        "category": "submit",
        "help_only": True,
        "presets": [{"label": {"ko": "도움말", "en": "Help"}, "args": ["--help"]}],
    },
    "srun": {
        "title": "srun",
        "summary": {
            "ko": "병렬 작업을 대화형으로 실행합니다.",
            "en": "Run parallel jobs interactively.",
        },
        "category": "submit",
        "help_only": True,
        "presets": [{"label": {"ko": "도움말", "en": "Help"}, "args": ["--help"]}],
    },
    "salloc": {
        "title": "salloc",
        "summary": {
            "ko": "대화형 셸에 필요한 자원을 할당합니다.",
            "en": "Allocate resources for an interactive shell.",
        },
        "category": "submit",
        "help_only": True,
        "presets": [{"label": {"ko": "도움말", "en": "Help"}, "args": ["--help"]}],
    },
    "scancel": {
        "title": "scancel",
        "summary": {
            "ko": "대기 또는 실행 중인 작업을 취소합니다.",
            "en": "Cancel pending or running jobs.",
        },
        "category": "submit",
        "help_only": True,
        "presets": [{"label": {"ko": "도움말", "en": "Help"}, "args": ["--help"]}],
    },
    "sattach": {
        "title": "sattach",
        "summary": {
            "ko": "실행 중인 작업 단계에 연결합니다.",
            "en": "Attach to a running job step.",
        },
        "category": "submit",
        "help_only": True,
        "presets": [{"label": {"ko": "도움말", "en": "Help"}, "args": ["--help"]}],
    },
    "sbcast": {
        "title": "sbcast",
        "summary": {
            "ko": "할당된 노드로 파일을 전송합니다.",
            "en": "Transfer files to allocated nodes.",
        },
        "category": "submit",
        "help_only": True,
        "presets": [{"label": {"ko": "도움말", "en": "Help"}, "args": ["--help"]}],
    },
    "scrontab": {
        "title": "scrontab",
        "summary": {
            "ko": "Slurm이 관리하는 cron 항목을 관리합니다.",
            "en": "Manage cron entries managed by Slurm.",
        },
        "category": "submit",
        "help_only": True,
        "presets": [{"label": {"ko": "도움말", "en": "Help"}, "args": ["--help"]}],
    },
    "scrun": {
        "title": "scrun",
        "summary": {
            "ko": "Slurm을 통해 컨테이너를 실행합니다.",
            "en": "Run containers through Slurm.",
        },
        "category": "submit",
        "help_only": True,
        "presets": [{"label": {"ko": "도움말", "en": "Help"}, "args": ["--help"]}],
    },
}


def validate_args(command: str, args: list[str]) -> None:
    meta = SLURM_COMMANDS[command]
    if meta.get("help_only"):
        if args != ["--help"]:
            raise ValueError(f"{command} is help-only in this dashboard")

    if meta.get("read_only"):
        if not args or args[0] != "show":
            raise ValueError("sacctmgr only allows read-only 'show' subcommands")

    for arg in args:
        if any(token in arg for token in BLOCKED_TOKENS):
            raise ValueError(f"Unsafe argument: {arg}")
        if not ARG_PATTERN.match(arg):
            raise ValueError(f"Invalid characters in argument: {arg}")


def run_slurm(command: str, args: list[str]) -> dict[str, Any]:
    if command not in SLURM_COMMANDS:
        raise ValueError(f"Unknown command: {command}")

    validate_args(command, args)
    started = time.time()
    proc = subprocess.run(
        [command, *args],
        capture_output=True,
        text=True,
        timeout=EXEC_TIMEOUT,
        check=False,
    )
    return {
        "command": command,
        "args": args,
        "argv": [command, *args],
        "exit_code": proc.returncode,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
        "duration_ms": int((time.time() - started) * 1000),
    }


def _classify_node_alert(node: str, state: str, reason: str) -> dict[str, Any] | None:
    state_l = state.lower().strip()
    reason_l = reason.lower().strip()

    severity = None
    if any(token in state_l for token in CRIT_STATE_TOKENS):
        severity = "critical"
    elif any(token in state_l for token in WARN_STATE_TOKENS):
        severity = "warning"
    elif reason_l and reason_l not in {"none", "n/a"}:
        if any(token in reason_l for token in CRIT_REASON_TOKENS):
            severity = "critical"
        elif any(token in reason_l for token in WARN_REASON_TOKENS):
            severity = "warning"

    if not severity:
        return None

    return {
        "node": node,
        "state": state,
        "reason": reason if reason and reason.lower() not in {"none", "n/a"} else "",
        "severity": severity,
    }


def node_alerts(lang: str = DEFAULT_LANG) -> dict[str, Any]:
    proc = subprocess.run(
        ["sinfo", "-h", "-N", "-o", "%N|%T|%E"],
        capture_output=True,
        text=True,
        timeout=EXEC_TIMEOUT,
        check=False,
    )
    alerts: list[dict[str, Any]] = []
    if proc.returncode == 0:
        for line in proc.stdout.splitlines():
            parts = [part.strip() for part in line.split("|", 2)]
            if len(parts) < 2:
                continue
            node, state = parts[0], parts[1]
            reason = parts[2] if len(parts) > 2 else ""
            alert = _classify_node_alert(node, state, reason)
            if alert:
                alerts.append(alert)

    # Supplement with scontrol for compound states like IDLE+DRAIN.
    ctrl = subprocess.run(
        ["scontrol", "show", "nodes"],
        capture_output=True,
        text=True,
        timeout=EXEC_TIMEOUT,
        check=False,
    )
    if ctrl.returncode == 0:
        current = ""
        for line in ctrl.stdout.splitlines():
            if line.startswith("NodeName="):
                current = line.split("=", 1)[1].split()[0]
            elif line.strip().startswith("State=") and current:
                state = line.split("=", 1)[1].strip().split()[0]
                existing = next((a for a in alerts if a["node"] == current), None)
                alert = _classify_node_alert(current, state, existing["reason"] if existing else "")
                if alert:
                    if existing:
                        existing["state"] = state
                        existing["severity"] = max(
                            existing["severity"],
                            alert["severity"],
                            key=lambda s: 0 if s == "warning" else 1,
                        )
                    else:
                        alerts.append(alert)
                current = ""

    alerts.sort(key=lambda item: (0 if item["severity"] == "critical" else 1, item["node"]))
    has_critical = any(item["severity"] == "critical" for item in alerts)
    lang = parse_lang(lang)
    texts = ALERT_TEXT[lang]
    return {
        "alerts": alerts,
        "has_alerts": bool(alerts),
        "has_critical": has_critical,
        "summary": (
            texts["needs_attention"].format(count=len(alerts))
            if alerts
            else texts["all_ok"]
        ),
        "generated_at": int(time.time()),
        "source_exit_code": proc.returncode,
        "lang": lang,
    }


def node_inventory() -> list[dict[str, Any]]:
    proc = subprocess.run(
        ["sinfo", "-h", "-N", "-o", "%N|%T|%E|%v"],
        capture_output=True,
        text=True,
        timeout=EXEC_TIMEOUT,
        check=False,
    )
    nodes: dict[str, dict[str, Any]] = {}
    if proc.returncode != 0:
        return []

    for line in proc.stdout.splitlines():
        parts = [part.strip() for part in line.split("|", 3)]
        if len(parts) < 2 or not parts[0]:
            continue
        name, state = parts[0], parts[1]
        reason = parts[2] if len(parts) > 2 else ""
        version = parts[3] if len(parts) > 3 else ""
        reason = "" if reason.lower() in {"none", "n/a", "(null)"} else reason
        version = "" if version.lower() in {"none", "n/a", "(null)", "unknown"} else version
        alert = _classify_node_alert(name, state, reason)
        nodes[name] = {
            "name": name,
            "state": state,
            "reason": reason,
            "version": version,
            "severity": alert["severity"] if alert else "healthy",
        }
    return sorted(nodes.values(), key=lambda item: item["name"])


def dashboard_snapshot(lang: str = DEFAULT_LANG) -> dict[str, Any]:
    sections = []
    for command, args in (
        ("sinfo", ["-s"]),
        ("squeue", ["-l"]),
        ("scontrol", ["show", "nodes"]),
    ):
        try:
            sections.append(run_slurm(command, args))
        except Exception as exc:  # noqa: BLE001 - surface to UI
            sections.append(
                {
                    "command": command,
                    "args": args,
                    "exit_code": 1,
                    "stdout": "",
                    "stderr": str(exc),
                    "duration_ms": 0,
                }
            )
    return {
        "sections": sections,
        "generated_at": int(time.time()),
        "alerts": node_alerts(lang),
        "nodes": node_inventory(),
        "controller": controller_status(),
        "lang": parse_lang(lang),
    }


def ping_controllers() -> dict[str, dict[str, Any]]:
    """Parse scontrol ping without enrichment (no peer calls)."""
    proc = subprocess.run(
        ["scontrol", "ping"],
        capture_output=True,
        text=True,
        timeout=EXEC_TIMEOUT,
        check=False,
    )
    controllers: dict[str, dict[str, Any]] = {}
    pattern = re.compile(
        r"Slurmctld\((primary|backup)\) at (\S+) is (UP|DOWN)",
        re.IGNORECASE,
    )
    for role, host, status in pattern.findall(f"{proc.stdout}\n{proc.stderr}"):
        role = role.lower()
        controllers[role] = {
            "host": host,
            "ip": PRIMARY_IP if role == "primary" else BACKUP_IP,
            "status": status.upper(),
        }
    return controllers


def controller_status() -> dict[str, Any]:
    controllers = ping_controllers()
    for role, info in controllers.items():
        info["version"] = role_slurm_version(role, info.get("ip"))

    primary = controllers.get("primary", {})
    backup = controllers.get("backup", {})
    active = primary if primary.get("status") == "UP" else backup
    return {
        "active": active or None,
        "primary": primary or None,
        "backup": backup or None,
        "reachable": bool(active and active.get("status") == "UP"),
    }


def health_status() -> tuple[dict[str, Any], int]:
    """Return healthy only when this host can query a Slurm controller."""
    try:
        result = run_slurm("sinfo", ["-h", "-o", "%P %a %D %t"])
        healthy = result["exit_code"] == 0
        detail = (result["stderr"] or result["stdout"]).strip()[:500]
    except Exception as exc:  # noqa: BLE001
        healthy = False
        detail = str(exc)
    return (
        {
            "status": "ok" if healthy else "error",
            "host": os.uname().nodename,
            "cluster": CLUSTER_NAME,
            "slurm": "reachable" if healthy else "unreachable",
            "version": local_slurm_version(),
            "detail": detail,
            "controller": controller_status(),
        },
        HTTPStatus.OK if healthy else HTTPStatus.SERVICE_UNAVAILABLE,
    )



def role_inventory() -> dict[str, dict[str, Any]]:
    """Map primary/backup roles to known controller IPs."""
    return {
        "primary": {
            "role": "primary",
            "label": "MASTER",
            "ip": PRIMARY_IP or None,
        },
        "backup": {
            "role": "backup",
            "label": "SLAVE",
            "ip": BACKUP_IP or None,
        },
    }


def local_controller_role() -> str | None:
    host = os.uname().nodename
    # Use lightweight ping parse only — never call controller_status() here
    # (it enriches versions and would recurse).
    for role, info in ping_controllers().items():
        if info.get("host") == host:
            return role
    local_addrs = set()
    try:
        hostname = socket.gethostname()
        local_addrs.add(socket.gethostbyname(hostname))
    except OSError:
        pass
    try:
        for info in socket.getaddrinfo(host, None):
            local_addrs.add(info[4][0])
    except OSError:
        pass
    if PRIMARY_IP and PRIMARY_IP in local_addrs:
        return "primary"
    if BACKUP_IP and BACKUP_IP in local_addrs:
        return "backup"
    return None


def read_local_slurm_conf() -> dict[str, Any]:
    path = SLURM_CONF_PATH
    if not path.is_file():
        raise FileNotFoundError(f"slurm.conf not found: {path}")
    data = path.read_bytes()
    if len(data) > SLURM_CONF_MAX_BYTES:
        raise ValueError(f"slurm.conf too large (>{SLURM_CONF_MAX_BYTES} bytes)")
    stat = path.stat()
    role = local_controller_role()
    inventory = role_inventory().get(role or "", {})
    return {
        "host": os.uname().nodename,
        "role": role,
        "label": inventory.get("label"),
        "ip": inventory.get("ip"),
        "path": str(path),
        "mtime": int(stat.st_mtime),
        "size": stat.st_size,
        "version": local_slurm_version(),
        "content": data.decode("utf-8"),
    }


def write_local_slurm_conf(content: str) -> dict[str, Any]:
    if not isinstance(content, str):
        raise ValueError("content must be a string")
    encoded = content.encode("utf-8")
    if not encoded.strip():
        raise ValueError("content must not be empty")
    if len(encoded) > SLURM_CONF_MAX_BYTES:
        raise ValueError(f"content too large (>{SLURM_CONF_MAX_BYTES} bytes)")
    if "\x00" in content:
        raise ValueError("content contains null bytes")

    path = SLURM_CONF_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    backup_path = None
    if path.is_file():
        backup_path = path.with_name(f"{path.name}.bak.{stamp}")
        backup_path.write_bytes(path.read_bytes())

    tmp_path = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    try:
        tmp_path.write_bytes(encoded)
        os.chmod(tmp_path, 0o644)
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)

    result = read_local_slurm_conf()
    result["backup"] = str(backup_path) if backup_path else None
    result["saved"] = True
    return result


def restart_local_slurm_service(service: str | None = None) -> dict[str, Any]:
    svc = (service or SLURM_RESTART_SERVICE).strip()
    if not re.fullmatch(r"[A-Za-z0-9_.@+-]+", svc or ""):
        raise ValueError("invalid service name")
    before = subprocess.run(
        ["systemctl", "is-active", svc],
        capture_output=True,
        text=True,
        timeout=EXEC_TIMEOUT,
        check=False,
    )
    proc = subprocess.run(
        ["systemctl", "restart", svc],
        capture_output=True,
        text=True,
        timeout=max(EXEC_TIMEOUT, 60),
        check=False,
    )
    after = subprocess.run(
        ["systemctl", "is-active", svc],
        capture_output=True,
        text=True,
        timeout=EXEC_TIMEOUT,
        check=False,
    )
    return {
        "host": os.uname().nodename,
        "role": local_controller_role(),
        "service": svc,
        "exit_code": proc.returncode,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
        "before": before.stdout.strip(),
        "after": after.stdout.strip(),
        "ok": proc.returncode == 0 and after.stdout.strip() == "active",
    }


def peer_url(ip: str, path: str) -> str:
    return f"http://{ip}:{DEFAULT_PORT}{path}"


def peer_request(ip: str, method: str, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    body = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json; charset=utf-8"
    req = urlrequest.Request(peer_url(ip, path), data=body, headers=headers, method=method)
    try:
        with urlrequest.urlopen(req, timeout=PEER_HTTP_TIMEOUT) as resp:
            raw = resp.read().decode("utf-8")
            data = json.loads(raw) if raw else {}
            if resp.status >= 400:
                raise RuntimeError(data.get("error") or f"peer HTTP {resp.status}")
            return data
    except urlerror.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        try:
            parsed = json.loads(detail)
            message = parsed.get("error") or detail
        except Exception:  # noqa: BLE001
            message = detail or str(exc)
        raise RuntimeError(f"{ip}: {message}") from exc
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"{ip}: {exc}") from exc



def local_slurm_version() -> str:
    now = time.time()
    cached = _VERSION_CACHE.get("value")
    if cached and now - float(_VERSION_CACHE.get("ts") or 0) < VERSION_CACHE_TTL:
        return str(cached)
    proc = subprocess.run(
        ["scontrol", "-V"],
        capture_output=True,
        text=True,
        timeout=EXEC_TIMEOUT,
        check=False,
    )
    text_out = f"{proc.stdout}\n{proc.stderr}".strip()
    match = re.search(r"(\d+\.\d+(?:\.\d+)?)", text_out)
    version = match.group(1) if match else (text_out.split()[-1] if text_out else "")
    _VERSION_CACHE["value"] = version
    _VERSION_CACHE["ts"] = now
    return version


def _local_ips() -> set[str]:
    addrs: set[str] = {"127.0.0.1", "::1"}
    host = os.uname().nodename
    try:
        addrs.add(socket.gethostbyname(host))
    except OSError:
        pass
    try:
        for info in socket.getaddrinfo(host, None):
            addrs.add(info[4][0])
    except OSError:
        pass
    return addrs


def role_slurm_version(role: str, ip: str | None) -> str:
    role = (role or "").lower()
    if ip and ip in _local_ips():
        return local_slurm_version()
    # Hostname-based local role without re-entering controller_status.
    if local_controller_role() == role:
        return local_slurm_version()
    if not ip:
        return ""
    now = time.time()
    cached = _PEER_VERSION_CACHE.get(role) or {}
    if cached.get("value") is not None and now - float(cached.get("ts") or 0) < VERSION_CACHE_TTL:
        return str(cached.get("value") or "")
    version = ""
    try:
        data = peer_request(ip, "GET", "/api/version")
        version = str(data.get("version") or "")
    except Exception:  # noqa: BLE001
        version = ""
    _PEER_VERSION_CACHE[role] = {"value": version, "ts": now}
    return version

def fetch_role_conf(role: str) -> dict[str, Any]:
    role = role.lower()
    inventory = role_inventory().get(role)
    if not inventory:
        raise ValueError("role must be primary or backup")
    ip = inventory.get("ip")
    if not ip:
        raise ValueError(f"{role} IP is not configured")

    local_role = local_controller_role()
    if local_role == role:
        data = read_local_slurm_conf()
    else:
        data = peer_request(ip, "GET", "/api/slurm-conf/local")

    data["role"] = role
    data["label"] = inventory["label"]
    data["ip"] = ip
    data["reachable"] = True
    return data


def save_role_conf(role: str, content: str) -> dict[str, Any]:
    role = role.lower()
    inventory = role_inventory().get(role)
    if not inventory:
        raise ValueError("role must be primary or backup")
    ip = inventory.get("ip")
    if not ip:
        raise ValueError(f"{role} IP is not configured")

    local_role = local_controller_role()
    if local_role == role:
        data = write_local_slurm_conf(content)
    else:
        data = peer_request(ip, "PUT", "/api/slurm-conf/local", {"content": content})

    data["role"] = role
    data["label"] = inventory["label"]
    data["ip"] = ip
    return data


def restart_role_service(role: str, service: str | None = None) -> dict[str, Any]:
    role = role.lower()
    inventory = role_inventory().get(role)
    if not inventory:
        raise ValueError("role must be primary or backup")
    ip = inventory.get("ip")
    if not ip:
        raise ValueError(f"{role} IP is not configured")

    local_role = local_controller_role()
    payload = {"service": service or SLURM_RESTART_SERVICE}
    if local_role == role:
        data = restart_local_slurm_service(payload["service"])
    else:
        data = peer_request(ip, "POST", "/api/slurm-conf/restart-local", payload)

    data["role"] = role
    data["label"] = inventory["label"]
    data["ip"] = ip
    return data


def conf_bundle() -> dict[str, Any]:
    roles: dict[str, Any] = {}
    for role in ("primary", "backup"):
        try:
            roles[role] = fetch_role_conf(role)
        except Exception as exc:  # noqa: BLE001
            inventory = role_inventory()[role]
            roles[role] = {
                "role": role,
                "label": inventory["label"],
                "ip": inventory.get("ip"),
                "reachable": False,
                "error": str(exc),
                "content": "",
            }
    return {
        "generated_at": int(time.time()),
        "path": str(SLURM_CONF_PATH),
        "service": SLURM_RESTART_SERVICE,
        "roles": roles,
        "local_role": local_controller_role(),
        "local_host": os.uname().nodename,
    }


class Handler(BaseHTTPRequestHandler):
    server_version = "SlurmMonitor/1.0"

    def log_message(self, fmt: str, *args: Any) -> None:
        print(f"{self.address_string()} - {fmt % args}")

    def _request_lang(self, parsed) -> str:
        query = parse_qs(parsed.query)
        header = self.headers.get("Accept-Language", "")
        return parse_lang((query.get("lang") or [header])[0])

    def _send_json(self, payload: dict[str, Any], status: int = HTTPStatus.OK) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_file(self, path: Path) -> None:
        if not path.is_file():
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")
            return
        data = path.read_bytes()
        content_type = "text/html; charset=utf-8"
        if path.suffix == ".css":
            content_type = "text/css; charset=utf-8"
        elif path.suffix == ".js":
            content_type = "application/javascript; charset=utf-8"
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        lang = self._request_lang(parsed)
        if parsed.path == "/api/commands":
            self._send_json({"commands": localize_commands(lang), "lang": lang})
            return
        if parsed.path == "/api/dashboard":
            self._send_json(dashboard_snapshot(lang))
            return
        if parsed.path == "/api/alerts":
            self._send_json(node_alerts(lang))
            return
        if parsed.path == "/api/nodes":
            self._send_json({"nodes": node_inventory(), "generated_at": int(time.time())})
            return
        if parsed.path == "/api/health":
            payload, status = health_status()
            self._send_json(payload, status=status)
            return
        if parsed.path == "/api/controller":
            self._send_json(controller_status())
            return
        if parsed.path == "/api/exec":
            query = parse_qs(parsed.query)
            command = (query.get("command") or [""])[0]
            arg_line = (query.get("args") or [""])[0]
            try:
                args = shlex.split(arg_line)
                result = run_slurm(command, args)
                self._send_json(result)
            except Exception as exc:  # noqa: BLE001
                self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return

        if parsed.path == "/api/version":
            self._send_json(
                {
                    "host": os.uname().nodename,
                    "role": local_controller_role(),
                    "version": local_slurm_version(),
                }
            )
            return
        if parsed.path == "/api/slurm-conf":
            self._send_json(conf_bundle())
            return
        if parsed.path == "/api/slurm-conf/local":
            try:
                self._send_json(read_local_slurm_conf())
            except Exception as exc:  # noqa: BLE001
                self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return

        if parsed.path in ("/", "/index.html"):
            self._send_file(STATIC_DIR / "index.html")
            return

        candidate = STATIC_DIR / parsed.path.lstrip("/")
        if candidate.exists():
            self._send_file(candidate)
            return

        self.send_error(HTTPStatus.NOT_FOUND, "Not found")

    def _read_json_body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b"{}"
        payload = json.loads(raw.decode("utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("JSON object required")
        return payload

    def do_PUT(self) -> None:
        parsed = urlparse(self.path)
        try:
            payload = self._read_json_body()
            if parsed.path == "/api/slurm-conf/local":
                self._send_json(write_local_slurm_conf(payload.get("content", "")))
                return
            if parsed.path == "/api/slurm-conf":
                role = str(payload.get("role", "")).lower()
                self._send_json(save_role_conf(role, payload.get("content", "")))
                return
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")
        except Exception as exc:  # noqa: BLE001
            self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        try:
            payload = self._read_json_body()
            if parsed.path == "/api/exec":
                command = payload.get("command", "")
                args = payload.get("args", [])
                if not isinstance(args, list):
                    raise ValueError("args must be a list")
                self._send_json(run_slurm(command, args))
                return
            if parsed.path == "/api/slurm-conf/restart-local":
                self._send_json(restart_local_slurm_service(payload.get("service")))
                return
            if parsed.path == "/api/slurm-conf/restart":
                role = str(payload.get("role", "")).lower()
                self._send_json(restart_role_service(role, payload.get("service")))
                return
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")
        except Exception as exc:  # noqa: BLE001
            self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)


def main() -> None:
    host = os.environ.get("SLURM_MONITOR_HOST", "0.0.0.0")
    server = ThreadingHTTPServer((host, DEFAULT_PORT), Handler)
    print(f"Slurm monitor listening on http://{host}:{DEFAULT_PORT}")
    server.serve_forever()


if __name__ == "__main__":
    main()
