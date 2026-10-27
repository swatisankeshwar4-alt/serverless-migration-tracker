"""Static code scan and automated rewrites for notebook source.

L6  hard API blockers: checkpoint(), catalog cache calls, CACHE/UNCACHE/REFRESH TABLE,
    global temp views, DBFS mount management, RDD APIs
L7  Scala and R (classify, and propose a rewrite or a keep-on-classic decision)
L13 streaming triggers (a supported trigger is required) and ANSI-mode risky SQL

Rules follow the serverless limitations page. Note that .cache(), .persist() and
.unpersist() ARE supported on serverless and are deliberately not flagged.

Findings never include source text (it may contain secrets): only a rule name and a line number.
"""
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

BLOCKER = "BLOCKER"
WARNING = "WARNING"
INFO = "INFO"

EXT_LANGUAGE = {".py": "PYTHON", ".sql": "SQL", ".scala": "SCALA", ".r": "R"}

HELPER = '''def _materialise_to_delta(df, name):
    """Serverless replacement for a Spark checkpoint: write to a scratch Delta table and read it back."""
    import uuid
    tbl = f"{spark.catalog.currentCatalog()}.{spark.catalog.currentDatabase()}.__mig_{name}_{uuid.uuid4().hex[:8]}"
    df.write.mode("overwrite").saveAsTable(tbl)
    return spark.table(tbl)
'''

MARK = "[serverless-migration]"


def language_from_extension(filename: str) -> Optional[str]:
    dot = filename.rfind(".")
    return EXT_LANGUAGE.get(filename[dot:].lower()) if dot != -1 else None


def _line_of(text: str, index: int) -> int:
    return text.count("\n", 0, index) + 1


def _f(workload_id: str, lim: str, sev: str, detail: str, fix: str) -> Dict[str, str]:
    return {"workload_id": workload_id, "limitation_id": lim, "severity": sev, "detail": detail,
            "suggested_fix": fix, "auto_fixable": False}


# ---- SQL fragment extraction ----------------------------------------------

SPARK_SQL_RE = re.compile(r"spark\.sql\(\s*[fFrR]?(?P<q>\"\"\"|'''|\"|')(?P<body>.*?)(?P=q)", re.DOTALL)
MAGIC_SQL_RE = re.compile(r"^#\s*MAGIC\s+%sql\b(.*)$")
MAGIC_LINE_RE = re.compile(r"^#\s*MAGIC ?(.*)$")


def _strip_sql_comments(sql: str) -> str:
    """Blank out comments but keep newlines so line numbers stay right."""
    sql = re.sub(r"/\*.*?\*/", lambda m: re.sub(r"[^\n]", " ", m.group(0)), sql, flags=re.DOTALL)
    return re.sub(r"--[^\n]*", lambda m: " " * len(m.group(0)), sql)


def sql_fragments(source: str, language: str) -> List[Tuple[str, int]]:
    """Return (sql_text, first_line_number) pieces found in a notebook."""
    if language == "SQL":
        return [(source, 1)]
    if language != "PYTHON":
        return []
    out: List[Tuple[str, int]] = []
    for m in SPARK_SQL_RE.finditer(source):
        out.append((m.group("body"), _line_of(source, m.start("body"))))
    lines = source.splitlines()
    i = 0
    while i < len(lines):
        m = MAGIC_SQL_RE.match(lines[i].strip())
        if m:
            start, buf = i + 1, [m.group(1).strip()]
            i += 1
            while i < len(lines) and MAGIC_LINE_RE.match(lines[i].strip()):
                buf.append(MAGIC_LINE_RE.match(lines[i].strip()).group(1))
                i += 1
            out.append(("\n".join(buf), start))
        else:
            i += 1
    return out


# ---- scanning ---------------------------------------------------------------

DIV_RE = re.compile(r"(?<![/*\-])/(?![/*])\s*(?P<den>[A-Za-z_`(][\w.`()]*)")
SQL_CAST_RE = re.compile(r"(?<![\w.])CAST\s*\(", re.IGNORECASE)
PY_CAST_RE = re.compile(
    r"\.cast\(\s*[\"'](?:int|integer|bigint|long|short|tinyint|double|float|decimal[^\"']*|date|timestamp|boolean)[\"']", re.I)


def _scan_python_rules(wid: str, src: str) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []

    def add(pattern: str, lim: str, sev: str, detail: str, fix: str, flags: int = 0):
        for m in re.finditer(pattern, src, flags):
            out.append(_f(wid, lim, sev, f"{detail} (line {_line_of(src, m.start())})", fix))

    add(r"\.checkpoint\(", "L6", BLOCKER, "df.checkpoint() is unsupported",
        "materialise to a Delta table instead (automated rewrite available)")
    add(r"spark\.catalog\.(?:cacheTable|uncacheTable|clearCache)\(", "L6", BLOCKER,
        "spark.catalog cache API is unsupported",
        "remove the call (performance-only); automated rewrite available")
    add(r"\.(?:createGlobalTempView|createOrReplaceGlobalTempView)\(", "L6", BLOCKER,
        "global temp view is unsupported",
        "use a session temp view, or materialise to Delta if other tasks read it (automated rewrite available)")
    add(r"\bglobal_temp\.", "L6", BLOCKER, "reference to global_temp",
        "drop the global_temp prefix after converting to a session temp view")
    add(r"dbutils\.fs\.(?:mount|unmount|updateMounts|refreshMounts|mounts)\(", "L6", BLOCKER,
        "DBFS mount management call",
        "replace with Unity Catalog external locations or volumes (manual)")
    add(r"\.rdd\b|\.parallelize\(|\.mapPartitions\(", "L6", BLOCKER,
        "RDD API (serverless supports Spark Connect APIs only)",
        "rewrite with the DataFrame API (manual)")
    add(r"\bsparkContext\b|\bsc\.", "L6", WARNING, "SparkContext access",
        "SparkContext is not available over Spark Connect: verify or rewrite")
    add(r"[\"'](?:/dbfs/|dbfs:/)", "L11", WARNING, "DBFS path",
        "DBFS access is limited on serverless: move data to a Unity Catalog volume")
    add(r"[\"']/mnt/", "L11", WARNING, "DBFS mount path (/mnt)",
        "mounts are not modelled on serverless: move to a volume or external location")
    for m in PY_CAST_RE.finditer(src):
        out.append(_f(wid, "L13", WARNING,
                      f"DataFrame cast fails on invalid input under ANSI mode (line {_line_of(src, m.start())})",
                      "use F.expr(\"try_cast(col AS type)\") to keep NULL-on-invalid behaviour"))

    # Streaming triggers: a supported trigger must be set.
    for m in re.finditer(r"\.trigger\(\s*(?:processingTime|continuous)\s*=", src):
        out.append(_f(wid, "L13", BLOCKER,
                      f"unsupported streaming trigger (line {_line_of(src, m.start())})",
                      "use .trigger(availableNow=True) (automated rewrite available; the stream then stops after "
                      "processing available data, so schedule the job)"))
    for start, end in _stream_chains(src):
        if ".trigger(" not in src[start:end]:
            out.append(_f(wid, "L13", BLOCKER,
                          f"writeStream without a trigger (line {_line_of(src, start)})",
                          "serverless requires a supported trigger: add .trigger(availableNow=True) "
                          "(automated rewrite available)"))
    return out


def _stream_chains(src: str) -> List[Tuple[int, int]]:
    """Spans from each .writeStream to its terminal .start( / .toTable( call, within one statement."""
    spans = []
    for m in re.finditer(r"\.writeStream\b", src):
        term = re.search(r"\.(?:start|toTable)\(", src[m.start():m.start() + 2000])
        if not term:
            continue
        end = m.start() + term.start()
        if re.search(r"\n\s*\n", src[m.start():end]):
            continue  # a blank line means the chain was not continued
        spans.append((m.start(), end))
    return spans


def _scan_sql_rules(wid: str, text: str, first_line: int) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    clean = _strip_sql_comments(text)

    def at(m) -> int:
        return first_line + clean.count("\n", 0, m.start())

    for m in re.finditer(r"\bCACHE\s+(?:LAZY\s+)?TABLE\b", clean, re.I):
        out.append(_f(wid, "L6", BLOCKER, f"CACHE TABLE is unsupported (line {at(m)})",
                      "CACHE TABLE x AS SELECT becomes a temp view; a plain CACHE TABLE is removed (automated rewrite)"))
    for m in re.finditer(r"\b(?:UNCACHE\s+TABLE|CLEAR\s+CACHE|REFRESH\s+TABLE)\b", clean, re.I):
        out.append(_f(wid, "L6", BLOCKER, f"cache/refresh statement is unsupported (line {at(m)})",
                      "remove the statement (automated rewrite)"))
    for m in re.finditer(r"\bGLOBAL\s+TEMP(?:ORARY)?\s+VIEW\b", clean, re.I):
        out.append(_f(wid, "L6", BLOCKER, f"global temp view is unsupported (line {at(m)})",
                      "use a session temp view, or materialise to Delta if shared (automated rewrite)"))
    for m in re.finditer(r"\bglobal_temp\.", clean, re.I):
        out.append(_f(wid, "L6", BLOCKER, f"reference to global_temp (line {at(m)})",
                      "drop the global_temp prefix after converting the view"))
    for m in SQL_CAST_RE.finditer(clean):
        out.append(_f(wid, "L13", WARNING, f"CAST errors on invalid input under ANSI mode (line {at(m)})",
                      "use try_cast(...) to keep NULL-on-invalid behaviour (automated rewrite)"))
    for m in re.finditer(r"::\s*\w+", clean):
        out.append(_f(wid, "L13", WARNING, f":: cast errors on invalid input under ANSI mode (line {at(m)})",
                      "use try_cast(expr AS type)"))
    for m in DIV_RE.finditer(clean):
        den = m.group("den")
        if not re.fullmatch(r"\d+(\.\d+)?", den):
            out.append(_f(wid, "L13", WARNING,
                          f"division by a non-literal errors on divide-by-zero under ANSI mode (line {at(m)})",
                          "use try_divide(a, b), which returns NULL instead of an error (manual)"))
    return out


# ---- Scala / R classification (L7) -----------------------------------------

MAGIC_LANG_RE = re.compile(r"^\s*(?:#|--|//)?\s*(?:MAGIC\s+)?%(scala|r)\b", re.I | re.M)


def classify_jvm_or_r(source: str, language: str) -> Dict[str, Any]:
    """Decide between a PySpark/SQL rewrite and keeping the notebook on classic."""
    comment = "//" if language == "SCALA" else "#"
    lines = [l for l in source.splitlines() if l.strip() and not l.strip().startswith(comment)]
    reasons: List[str] = []
    if language == "SCALA":
        if re.search(r"\.rdd\b|parallelize|sparkContext|\bsc\.", source):
            reasons.append("uses RDD/SparkContext APIs")
        foreign = [m for m in re.findall(r"^\s*import\s+([\w.]+)", source, re.M)
                   if not m.startswith(("org.apache.spark", "scala.", "java.", "javax.", "org.apache.hadoop"))]
        if foreign:
            reasons.append("imports non-Spark JVM libraries")
        if re.search(r"\bextends\s+UserDefinedAggregateFunction|\bAggregator\b", source):
            reasons.append("custom aggregators")
    else:
        libs = re.findall(r"(?:library|require)\(\s*[\"']?(\w+)", source)
        other = [l for l in libs if l.lower() not in ("sparkr", "sparklyr", "dplyr")]
        if other:
            reasons.append("uses CRAN packages beyond SparkR/sparklyr/dplyr")
    n = len(lines)
    effort = "SMALL" if n <= 100 else "MEDIUM" if n <= 400 else "LARGE"
    decision = "KEEP_ON_CLASSIC" if reasons else "REWRITE_PYSPARK"
    return {"decision": decision, "effort": effort, "lines": n, "reasons": reasons}


# ---- public scan -------------------------------------------------------------

def scan_notebook(workload_id: str, source: str, language: str) -> List[Dict[str, str]]:
    findings: List[Dict[str, str]] = []

    if language in ("SCALA", "R"):
        c = classify_jvm_or_r(source, language)
        why = f"; reasons: {', '.join(c['reasons'])}" if c["reasons"] else ""
        findings.append(_f(
            workload_id, "L7", BLOCKER,
            f"{language.title()} notebook, {c['lines']} lines{why}",
            f"recommended: {c['decision']} (rewrite effort {c['effort']}). "
            + ("Rewrite in PySpark/SQL." if c["decision"] == "REWRITE_PYSPARK"
               else "Keep this workload on classic compute.")))
        return findings

    for m in MAGIC_LANG_RE.finditer(source):
        findings.append(_f(workload_id, "L7", BLOCKER,
                           f"%{m.group(1).lower()} cell in a {language.title()} notebook (line {_line_of(source, m.start())})",
                           "Scala/R cells cannot run on serverless: rewrite the cell in PySpark/SQL"))

    if language == "PYTHON":
        findings.extend(_scan_python_rules(workload_id, source))
    for text, first_line in sql_fragments(source, language):
        findings.extend(_scan_sql_rules(workload_id, text, first_line))

    seen, out = set(), []
    for f in findings:
        k = (f["limitation_id"], f["detail"])
        if k not in seen:
            seen.add(k)
            out.append(f)
    return out


def _problem_key(f: Dict[str, str]) -> Tuple[str, str, str]:
    return (f["limitation_id"], f["severity"], re.sub(r"\s*\(line \d+\)$", "", f["detail"]))


def scan_with_fixability(workload_id: str, source: str, language: str) -> List[Dict[str, str]]:
    """scan_notebook, plus auto_fixable=True on findings that disappear once the rewrite is applied.
    Checked by rescanning the rewritten source, so it is true only when the rewrite really fixes it."""
    findings = scan_notebook(workload_id, source, language)
    new_source, changes = rewrite_notebook(source, language)
    if not changes:
        return findings
    remaining = {_problem_key(f) for f in scan_notebook(workload_id, new_source, language)}
    for f in findings:
        f["auto_fixable"] = _problem_key(f) not in remaining
    return findings


# ---- automated rewrites ------------------------------------------------------

def _rewrite_sql_text(text: str, changes: List[str]) -> str:
    def sub(pattern: str, repl, label: str, flags: int = re.I):
        nonlocal text
        text, n = re.subn(pattern, repl, text, flags=flags)
        if n:
            changes.append(f"{label} x{n}")

    sub(r"\bCACHE\s+(?:LAZY\s+)?TABLE\s+([\w.`]+)(?:\s+OPTIONS\s*\([^)]*\))?\s+AS\b",
        r"CREATE OR REPLACE TEMPORARY VIEW \1 AS", "CACHE TABLE ... AS -> temporary view")
    sub(r"^([ \t]*)((?:CACHE\s+(?:LAZY\s+)?TABLE|UNCACHE\s+TABLE|CLEAR\s+CACHE|REFRESH\s+TABLE)\b[^;\n]*;?)[ \t]*$",
        rf"\1-- {MARK} removed (unsupported, performance-only): \2", "cache/refresh statement removed",
        re.I | re.M)
    sub(r"\bCREATE\s+(?:OR\s+REPLACE\s+)?GLOBAL\s+TEMP(?:ORARY)?\s+VIEW\b",
        "CREATE OR REPLACE TEMPORARY VIEW", "global temp view -> session temp view")
    sub(r"\bglobal_temp\.", "", "global_temp prefix dropped")
    sub(r"(?<![\w.])CAST\s*\(", "try_cast(", "CAST -> try_cast")
    return text


def rewrite_notebook(source: str, language: str) -> Tuple[str, List[str]]:
    """Return (new_source, change_log). Never touches Scala/R; a rewrite there is a manual decision."""
    changes: List[str] = []
    if language not in ("PYTHON", "SQL"):
        return source, changes

    if language == "SQL":
        return _rewrite_sql_text(source, changes), changes

    text = source

    def sub(pattern: str, repl, label: str, flags: int = 0):
        nonlocal text
        text, n = re.subn(pattern, repl, text, flags=flags)
        if n:
            changes.append(f"{label} x{n}")

    # checkpoint: only the assignment form is rewritten safely
    sub(r"^([ \t]*)(\w+)\s*=\s*(\w+)\.checkpoint\([^)]*\)[ \t]*$",
        r'\1\2 = _materialise_to_delta(\3, "\2")', "df.checkpoint() -> materialise to Delta", re.M)
    sub(r"^([ \t]*)(spark\.catalog\.(?:cacheTable|uncacheTable|clearCache)\([^)]*\))[ \t]*$",
        rf"\1# {MARK} removed (unsupported, performance-only): \2", "spark.catalog cache call removed", re.M)
    sub(r"\.createGlobalTempView\(", ".createTempView(", "createGlobalTempView -> createTempView")
    sub(r"\.createOrReplaceGlobalTempView\(", ".createOrReplaceTempView(",
        "createOrReplaceGlobalTempView -> createOrReplaceTempView")
    sub(r"\bglobal_temp\.", "", "global_temp prefix dropped")
    sub(r"\.trigger\(\s*(?:processingTime|continuous)\s*=\s*[^)]*\)", ".trigger(availableNow=True)",
        "unsupported trigger -> availableNow")

    # writeStream without trigger: insert before the terminal call (work back to front to keep offsets valid)
    inserts = [end for start, end in _stream_chains(text) if ".trigger(" not in text[start:end]]
    for end in sorted(inserts, reverse=True):
        text = text[:end] + ".trigger(availableNow=True)" + text[end:]
    if inserts:
        changes.append(f"writeStream without trigger -> added availableNow x{len(inserts)}")

    # SQL inside spark.sql(...) strings
    def _fix_sql_string(m: "re.Match[str]") -> str:
        body = _rewrite_sql_text(m.group("body"), changes)
        return m.group(0)[: m.start("body") - m.start()] + body + m.group(0)[m.end("body") - m.start():]
    text = SPARK_SQL_RE.sub(_fix_sql_string, text)

    # SQL magic cells (# MAGIC %sql ...)
    new_lines, in_sql = [], False
    for line in text.splitlines():
        s = line.strip()
        if MAGIC_SQL_RE.match(s):
            in_sql = True
            new_lines.append(line)
            continue
        if in_sql and MAGIC_LINE_RE.match(s):
            prefix, content = re.match(r"^(\s*#\s*MAGIC ?)(.*)$", line).groups()
            new_lines.append(prefix + _rewrite_sql_text(content, changes))
            continue
        in_sql = False
        new_lines.append(line)
    text = "\n".join(new_lines) + ("\n" if source.endswith("\n") else "")

    if "_materialise_to_delta(" in text and "def _materialise_to_delta" not in text:
        # The helper gets a cell of its own so it never merges into a %md / %sql magic cell.
        cell_break = "\n# COMMAND ----------\n\n"
        header = "# Databricks notebook source\n"
        if text.startswith(header):
            text = header + HELPER + cell_break + text[len(header):]
        else:
            text = HELPER + cell_break + text
        changes.append("added _materialise_to_delta helper")
    return text, changes


# ---- loaders -----------------------------------------------------------------

def load_fixture_sources(fixtures_dir: str, paths: List[str]) -> Tuple[Dict[str, Tuple[str, str]], List[str]]:
    """Map notebook path -> (source, language) from fixtures/notebooks/<stem>.<ext>.
    Returns (found, missing_paths)."""
    import os
    folder = os.path.join(fixtures_dir, "notebooks")
    by_stem = {}
    if os.path.isdir(folder):
        for name in os.listdir(folder):
            lang = language_from_extension(name)
            if lang:
                by_stem[name[: name.rfind(".")]] = (os.path.join(folder, name), lang)
    found, missing = {}, []
    for p in paths:
        stem = p.rsplit("/", 1)[-1]
        if stem in by_stem:
            fpath, lang = by_stem[stem]
            with open(fpath, encoding="utf-8") as fh:
                found[p] = (fh.read(), lang)
        else:
            missing.append(p)
    return found, missing


def load_live_sources(workspace_client, paths: List[str]) -> Tuple[Dict[str, Tuple[str, str]], Dict[str, str]]:
    """Export notebook source through the SDK. Returns (found, errors); errors hold the exception
    class name only, never the message."""
    import base64
    from databricks.sdk.service.workspace import ExportFormat
    found, errors = {}, {}
    for p in paths:
        try:
            status = workspace_client.workspace.get_status(p)
            lang = status.language.value if status.language else None
            if lang is None:
                errors[p] = "not a notebook"
                continue
            data = workspace_client.workspace.export(p, format=ExportFormat.SOURCE)
            found[p] = (base64.b64decode(data.content).decode("utf-8", "replace"), lang)
        except Exception as e:  # noqa: BLE001
            errors[p] = type(e).__name__
    return found, errors
