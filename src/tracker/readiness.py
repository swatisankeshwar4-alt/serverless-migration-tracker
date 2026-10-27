"""Readiness scan: turns job/cluster definitions into findings, scores and statuses.

Covers the config-level limitations:
  L2  jobs with more than 10 migratable tasks
  L3  init scripts in S3 / DBFS (and other locations)
  L4  custom images, ML runtime, DBR < 13, instance profiles
  L5  egg / jar / maven (and cran) libraries
  L11 cluster log delivery and SSH keys (modelled as "lost on serverless")

Code-level limitations (L6, L7, L13) need notebook source and come in step 4.
No Spark or SDK imports, so everything here is unit-testable.
"""
from __future__ import annotations

import json
import re
from typing import Any, Dict, Iterable, List, Optional, Tuple

from . import inventory, schedules, splitter

BLOCKER = "BLOCKER"
WARNING = "WARNING"
INFO = "INFO"

# Limit taken from the migration agent documentation referenced in the challenge.
MAX_MIGRATABLE_TASKS = inventory.MAX_MIGRATABLE_TASKS
MIN_DBR_MAJOR = 13
# Tasks that only orchestrate and do not run user code on a cluster.
NON_MIGRATABLE_TASK_KEYS = inventory.NON_MIGRATABLE_TASK_KEYS
# Init script locations the agent cannot read (L3).
UNREADABLE_INIT_LOCATIONS = ("s3", "dbfs")
# Spark conf keys Databricks sets itself on single-node / profile clusters: not user configuration.
AUTO_SPARK_CONF = ("spark.master", "spark.databricks.cluster.profile")

# Points taken off per distinct problem. Problems an automated rewrite can remove cost less.
PENALTY = {(BLOCKER, False): 20, (BLOCKER, True): 5, (WARNING, False): 5, (WARNING, True): 2}


def _finding(workload_id: str, limitation: str, severity: str, detail: str, fix: str) -> Dict[str, str]:
    return {"workload_id": workload_id, "limitation_id": limitation, "severity": severity,
            "detail": detail, "suggested_fix": fix, "auto_fixable": False}


# ---- cluster-level checks (L3, L4, L11) -----------------------------------

def dbr_major(spark_version: Optional[str]) -> Optional[int]:
    m = re.match(r"(\d+)\.", spark_version or "")
    return int(m.group(1)) if m else None


def _is_ml_runtime(spark_version: str) -> bool:
    return re.search(r"(^|[-.])ml([-.]|$)", spark_version.lower()) is not None


def check_cluster_spec(workload_id: str, spec: Dict[str, Any]) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []

    # An all-purpose cluster we could not resolve: its real config is unknown.
    if set(spec) == {"existing_cluster_id"}:
        out.append(_finding(workload_id, "L4", WARNING,
                            f"runs on existing cluster {spec['existing_cluster_id']} whose configuration was not found",
                            "include the cluster in discovery so its runtime, init scripts and libraries are scanned"))
        return out

    sv = spec.get("spark_version") or ""
    if sv.startswith("custom:"):
        out.append(_finding(workload_id, "L4", BLOCKER, f"custom runtime image ({sv})",
                            "rebuild the dependencies as a serverless environment spec (wheels / PyPI)"))
    elif sv:
        major = dbr_major(sv)
        if "apache-spark" in sv or (major is not None and major < MIN_DBR_MAJOR):
            out.append(_finding(workload_id, "L4", BLOCKER, f"Databricks Runtime {sv} is below {MIN_DBR_MAJOR}",
                                f"upgrade to DBR {MIN_DBR_MAJOR}+ on classic and re-test, then migrate"))
        if _is_ml_runtime(sv):
            out.append(_finding(workload_id, "L4", BLOCKER, f"ML runtime ({sv})",
                                "move ML libraries to the serverless environment spec, or keep on classic"))

    if spec.get("docker_image"):
        out.append(_finding(workload_id, "L4", BLOCKER, "custom Docker image",
                            "replace the image with a serverless environment spec, or keep on classic"))

    profile = (spec.get("aws_attributes") or {}).get("instance_profile_arn")
    if profile:
        out.append(_finding(workload_id, "L4", BLOCKER, "uses an instance profile",
                            "switch storage access to a Unity Catalog external location / storage credential"))

    for script in spec.get("init_scripts") or []:
        kind = next(iter(script), "unknown")
        dest = (script.get(kind) or {}).get("destination", "")
        if kind in UNREADABLE_INIT_LOCATIONS:
            out.append(_finding(workload_id, "L3", BLOCKER, f"init script in {kind.upper()}: {dest}",
                                "retrieve the script and translate it into an environment spec "
                                "(see tracker/initscripts.py)"))
        else:
            out.append(_finding(workload_id, "L3", WARNING, f"init script ({kind}): {dest}",
                                "init scripts are not supported on serverless: translate into an environment spec"))

    if spec.get("cluster_log_conf"):
        out.append(_finding(workload_id, "L11", WARNING, "cluster log delivery is configured",
                            "lost on serverless: use system tables / job run output for logs instead"))
    if spec.get("ssh_public_keys"):
        out.append(_finding(workload_id, "L11", WARNING, "SSH keys are configured",
                            "SSH access does not exist on serverless: remove the dependency"))
    if spec.get("spark_env_vars"):
        names = ", ".join(sorted(spec["spark_env_vars"]))
        out.append(_finding(workload_id, "L11", WARNING,
                            f"compute-scoped environment variables ({names})",
                            "not supported on serverless: use job parameters or widgets"))
    conf_keys = sorted(k for k in (spec.get("spark_conf") or {}) if k not in AUTO_SPARK_CONF)
    if conf_keys:
        out.append(_finding(workload_id, "L11", WARNING,
                            f"{len(conf_keys)} Spark config(s): {', '.join(conf_keys)}",
                            "most Spark configs are not supported on serverless: check each against the supported list"))
    return out


# ---- job-level checks (L2, L5) --------------------------------------------

def job_cluster_specs(job: Dict[str, Any], clusters_by_id: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Distinct cluster specs a job uses; existing_cluster_id is resolved when the cluster is known."""
    jc = inventory._job_clusters(job)
    specs, seen = [], set()
    for t in inventory._tasks(job):
        spec = inventory.task_cluster(t, jc)
        if not spec:
            continue
        if set(spec) == {"existing_cluster_id"}:
            spec = clusters_by_id.get(spec["existing_cluster_id"], spec)
        key = json.dumps(spec, sort_keys=True)
        if key not in seen:
            seen.add(key)
            specs.append(spec)
    return specs


def migratable_task_count(job: Dict[str, Any]) -> int:
    return sum(1 for t in inventory._tasks(job)
               if not any(k in t for k in NON_MIGRATABLE_TASK_KEYS))


def check_libraries(workload_id: str, job: Dict[str, Any]) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    for t in inventory._tasks(job):
        for lib in t.get("libraries") or []:
            if "egg" in lib:
                out.append(_finding(workload_id, "L5", BLOCKER, f"egg library {lib['egg']}",
                                    "rebuild as a wheel and reference it from a Unity Catalog volume"))
            elif "jar" in lib:
                out.append(_finding(workload_id, "L5", BLOCKER, f"JAR library {lib['jar']}",
                                    "upload to a UC volume and run it as a JAR task, or keep on classic"))
            elif "maven" in lib:
                coords = (lib["maven"] or {}).get("coordinates", "")
                out.append(_finding(workload_id, "L5", BLOCKER, f"Maven library {coords}",
                                    "find a PyPI equivalent, or package it into a JAR task; otherwise keep on classic"))
            elif "cran" in lib:
                out.append(_finding(workload_id, "L7", BLOCKER, "R (CRAN) library",
                                    "R does not run on serverless: rewrite in PySpark/SQL or keep on classic"))
            elif "whl" in lib and str(lib["whl"]).startswith("dbfs:"):
                out.append(_finding(workload_id, "L5", WARNING, f"wheel on DBFS {lib['whl']}",
                                    "move the wheel to a Unity Catalog volume"))
        if "notebook_task" in t and any("pypi" in lib for lib in t.get("libraries") or []):
            out.append(_finding(workload_id, "L5", WARNING,
                                f"task-level PyPI library on notebook task '{t.get('task_key')}'",
                                "task libraries are not supported for notebook tasks: use %pip in the notebook "
                                "or a job environment"))
        if "spark_jar_task" in t:
            out.append(_finding(workload_id, "L5", INFO, f"JAR task '{t.get('task_key')}'",
                                "check the JAR task runs on a serverless JAR environment"))
    return out


def scan_job(job: Dict[str, Any], clusters_by_id: Optional[Dict[str, Dict[str, Any]]] = None) -> List[Dict[str, str]]:
    wid = str(job["job_id"])
    findings: List[Dict[str, str]] = []

    n = migratable_task_count(job)
    if n > MAX_MIGRATABLE_TASKS:
        can_split = splitter.splittable(job)
        f = _finding(wid, "L2", BLOCKER,
                     f"{n} migratable tasks (limit {MAX_MIGRATABLE_TASKS})",
                     f"split into child jobs of at most {MAX_MIGRATABLE_TASKS} tasks"
                     + (" (split plan generated: see split_plans)" if can_split else " (cannot be split automatically)"))
        f["auto_fixable"] = can_split
        findings.append(f)

    for spec in job_cluster_specs(job, clusters_by_id or {}):
        findings.extend(check_cluster_spec(wid, spec))
    findings.extend(check_libraries(wid, job))
    return dedupe(findings)


def scan_cluster(cluster: Dict[str, Any]) -> List[Dict[str, str]]:
    return dedupe(check_cluster_spec(cluster["cluster_id"], cluster))


def dedupe(findings: Iterable[Dict[str, str]]) -> List[Dict[str, str]]:
    seen, out = set(), []
    for f in findings:
        key = (f["workload_id"], f["limitation_id"], f["detail"])
        if key not in seen:
            seen.add(key)
            out.append(f)
    return out


# ---- scoring and status ----------------------------------------------------

def rule_key(finding: Dict[str, Any]) -> Tuple[str, str, str, str]:
    """Identity of a problem: the same rule hit on several lines of one workload is one problem."""
    detail = re.sub(r"\s*\(line \d+\)$", "", finding["detail"])
    return (finding["workload_id"], finding["limitation_id"], finding["severity"], detail)


def score(findings: Iterable[Dict[str, Any]]) -> int:
    """100 minus a penalty per distinct problem, floored at 0: 20 for a blocker, 5 for a warning,
    and only 5 / 2 when an automated rewrite can remove it. A heuristic, not a measurement."""
    seen, total = set(), 0
    for f in findings:
        key = rule_key(f)
        if key in seen:
            continue
        seen.add(key)
        total += PENALTY.get((f["severity"], bool(f.get("auto_fixable"))), 0)
    return max(0, 100 - total)


def status_for(compute_kind: str, findings: Iterable[Dict[str, str]]) -> str:
    if compute_kind == inventory.SERVERLESS:
        return "DONE"  # already on serverless, nothing to migrate
    if compute_kind == inventory.CLASSIC:
        return "BLOCKED" if any(f["severity"] == BLOCKER for f in findings) else "READY"
    return "DISCOVERED"


def analyse_all(jobs: List[Dict[str, Any]], clusters: List[Dict[str, Any]],
                notebook_findings: Optional[Dict[str, List[Dict[str, str]]]] = None
                ) -> Tuple[List[Dict[str, str]], List[Dict[str, Any]]]:
    """Return (findings, status_rows).

    findings holds job and cluster configuration findings only. Notebook findings come from the
    code scan (step 4), are stored separately, and are passed in here so that they lower the score
    and status of the notebook and of every job that runs it.
    """
    nbf = notebook_findings or {}
    clusters_by_id = {c["cluster_id"]: c for c in clusters}
    findings: List[Dict[str, str]] = []
    status_rows: List[Dict[str, Any]] = []
    rank = {"BLOCKED": 3, "READY": 2, "DISCOVERED": 1, "DONE": 0}
    nb: Dict[str, Dict[str, Any]] = {}

    # L10: a clone that fires on the same schedule as its original doubles the work.
    dup_findings: Dict[str, List[Dict[str, Any]]] = {}
    for d in schedules.find_duplicates(jobs):
        f = _finding(d["extra"], "L10", BLOCKER,
                     f"fires on the same schedule ({d['schedule']}) as job {d['keep']} for the same workload",
                     f"pause this job's trigger until it is promoted (automated: see the schedule guard)")
        f["auto_fixable"] = True
        dup_findings.setdefault(d["extra"], []).append(f)

    def result(wid: str, kind: str, fs: List[Dict[str, str]]) -> Dict[str, Any]:
        return {"workload_id": wid,
                "readiness_score": 100 if kind == inventory.SERVERLESS else score(fs),
                "status": status_for(kind, fs)}

    for job in jobs:
        wid = str(job["job_id"])
        kind = inventory.classify_compute(job)
        config = scan_job(job, clusters_by_id) + dup_findings.get(wid, [])
        paths = [r["workload_id"] for r in inventory.notebook_rows(job, "x")]
        code = [f for p in paths for f in nbf.get(p, [])]
        findings.extend(config)
        status_rows.append(result(wid, kind, config + code))

        for p in paths:
            res = result(p, kind, config + nbf.get(p, []))
            cur = nb.get(p)
            if cur is None:
                nb[p] = res
            else:
                cur["readiness_score"] = min(cur["readiness_score"], res["readiness_score"])
                if rank[res["status"]] > rank[cur["status"]]:
                    cur["status"] = res["status"]

    for c in clusters:
        f = scan_cluster(c)
        findings.extend(f)
        status_rows.append(result(c["cluster_id"], inventory.CLASSIC, f))

    status_rows.extend(nb.values())
    return findings, status_rows
