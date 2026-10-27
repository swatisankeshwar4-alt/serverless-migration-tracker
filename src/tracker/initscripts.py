"""Translate a classic init script into serverless-friendly pieces (L3).

Serverless has no init scripts, so we pull out what can become environment
dependencies (pip installs) and flag everything else for manual review.
Only names are reported for env vars and commands, never values, because init
scripts often contain secrets.
"""
from __future__ import annotations

import re
import shlex
from typing import Any, Dict, List

PIP_RE = re.compile(r"^(?:sudo\s+)?(?:\S*python\d?\s+-m\s+)?(?:\S*/)?pip\d?\s+install\s+(.*)$")
APT_RE = re.compile(r"^(?:sudo\s+)?apt(?:-get)?\s+(?:-y\s+)?install\s+(.*)$")
EXPORT_RE = re.compile(r"^export\s+([A-Za-z_][A-Za-z0-9_]*)=")
# Lines that carry no installation intent.
IGNORED_COMMANDS = {"set", "echo", "mkdir", "cd", "cp", "mv", "ls", "cat", "touch", "true", "exit"}
# pip options that take a value (the value is not a package name).
PIP_VALUE_FLAGS = {"-i", "--index-url", "--extra-index-url", "-f", "--find-links", "-c", "--constraint",
                   "-r", "--requirement", "-e", "--editable", "--target", "-t"}


def _pip_packages(arg_string: str, other: List[str]) -> List[str]:
    try:
        tokens = shlex.split(arg_string)
    except ValueError:
        other.append("pip (unparsable arguments)")
        return []
    pkgs, skip_next = [], False
    for tok in tokens:
        if skip_next:
            skip_next = False
            continue
        if tok in PIP_VALUE_FLAGS:
            other.append(f"pip option {tok} needs manual review")
            skip_next = True
            continue
        if tok.startswith("-"):
            continue  # quiet/upgrade style flags
        if tok.endswith(".whl") or tok.startswith(("/", "dbfs:", "s3:", "file:")):
            other.append("local wheel/path install: move it to a Unity Catalog volume")
            continue
        pkgs.append(tok)
    return pkgs


def translate_init_script(text: str, client_version: str = "2") -> Dict[str, Any]:
    """client_version is written into the environment spec; confirm the field name and
    value against the current Jobs API environment documentation before use."""
    deps: List[str] = []
    os_packages: List[str] = []
    env_names: List[str] = []
    other: List[str] = []

    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("%pip"):
            line = "pip" + line[len("%pip"):]
        m = PIP_RE.match(line)
        if m:
            deps.extend(_pip_packages(m.group(1), other))
            continue
        m = APT_RE.match(line)
        if m:
            os_packages.extend(t for t in m.group(1).split() if not t.startswith("-"))
            continue
        m = EXPORT_RE.match(line)
        if m:
            env_names.append(m.group(1))
            continue
        cmd = line.split()[0]
        if cmd not in IGNORED_COMMANDS:
            other.append(cmd)

    deps = list(dict.fromkeys(deps))
    other = list(dict.fromkeys(other))
    return {
        "dependencies": deps,
        "os_packages": os_packages,
        "env_var_names": env_names,
        "other_commands": other,
        "needs_manual_review": bool(os_packages or env_names or other),
        "environment_spec": {"client": client_version, "dependencies": deps},
    }
