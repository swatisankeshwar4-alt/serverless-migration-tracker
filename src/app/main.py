"""Serverless Migration Tracker: Databricks App (Streamlit).

One screen a platform owner can use: progress, readiness, blockers per limitation, the approval gate,
safety actions, validation and cost. Reads the tracker's Delta tables through a SQL warehouse.

Identity: reads run as the app's service principal. The approval gate uses the signed-in user
(forwarded by Databricks Apps) and checks that user is a workspace admin; no token means read-only.
Set TRACKER_LOCAL=1 to preview on fixtures without a workspace.
"""
import os
import sys

import pandas as pd
import streamlit as st

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # src/ for `tracker`
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from tracker import appdata, approvals  # noqa: E402

CATALOG = os.getenv("TRACKER_CATALOG", "workspace")
SCHEMA = os.getenv("TRACKER_SCHEMA", "serverless_migration")
LOCAL = os.getenv("TRACKER_LOCAL") == "1"

st.set_page_config(page_title="Serverless Migration Tracker", page_icon="🚀", layout="wide")


# ---------- data access ----------
@st.cache_resource
def _service_client():
    from databricks.sdk import WorkspaceClient
    return WorkspaceClient()


@st.cache_data(ttl=60, show_spinner="Reading tracker tables...")
def load_all():
    """name -> rows. A missing table gives an empty list plus a warning, never a crash."""
    if LOCAL:
        import demo_data
        return demo_data.build(), {}
    import db
    client, data, problems = _service_client(), {}, {}
    for name, sql in appdata.queries(CATALOG, SCHEMA).items():
        try:
            data[name] = db.run(client, sql)
        except Exception as e:  # noqa: BLE001
            data[name] = []
            if name not in appdata.OPTIONAL_QUERIES:
                problems[name] = str(e)[:300]
    return data, problems


def current_user(approver_rows):
    """(email, can_decide). Fails closed: anything unexpected means read-only.

    1. With Databricks Apps user authorization, the user's own token proves they are a workspace admin.
    2. Otherwise the signed-in email (set by the Apps proxy, not by the browser) must be on the
       `approvers` list, which only a workspace admin fills (09_grant_app).
    """
    if LOCAL:
        return "local-preview@example.com", os.getenv("TRACKER_LOCAL_ADMIN") == "1"
    try:
        headers = st.context.headers
        token, email = headers.get("x-forwarded-access-token"), headers.get("x-forwarded-email", "")
        if token:
            try:
                from databricks.sdk import WorkspaceClient
                user_client = WorkspaceClient(host=_service_client().config.host, token=token, auth_type="pat")
                is_admin, user_name = approvals.workspace_admin(user_client)
                if is_admin and user_name:
                    return user_name, True
            except Exception:  # noqa: BLE001
                pass
        return email, appdata.is_approver(email, approver_rows)
    except Exception:  # noqa: BLE001
        return "", False


def record_decisions(ids, decision, who, note):
    import db
    client = _service_client()
    ins, upd = appdata.decide_sql(CATALOG, SCHEMA, decision)
    for wid in ids:
        db.run(client, ins, {"wid": wid, "decision": decision, "who": who, "note": note or None})
        if upd:
            db.run(client, upd, {"wid": wid})


def df(rows, cols=None):
    d = pd.DataFrame(rows)
    return d[[c for c in cols if c in d.columns]] if cols and not d.empty else d


# ---------- page ----------
data, problems = load_all()
workloads = data["workloads"]

st.title("Serverless Migration Tracker")
st.caption(f"{CATALOG}.{SCHEMA}" + ("  |  LOCAL PREVIEW on fixtures" if LOCAL else ""))
head_l, head_r = st.columns([6, 1])
if head_r.button("Refresh"):
    load_all.clear()
    st.rerun()
for name, msg in problems.items():
    st.warning(f"Could not read `{name}`: {msg}. Has the tracker pipeline run, and does the app have SELECT?")
if not workloads:
    st.info("The inventory is empty. Run the `[tracker] run all` job, then press Refresh.")
    st.stop()

tab_over, tab_work, tab_block, tab_fix, tab_appr, tab_safe, tab_drift, tab_val, tab_cost = st.tabs(
    ["Overview", "Workloads", "Blockers", "Fixes", "Approvals", "Safety log", "Drift & rollback", "Validation", "Cost"])

# ---- Overview ----
with tab_over:
    k = appdata.kpis(workloads)
    st.progress(k["pct_on_serverless"] / 100, text=f"{k['on_serverless']} of {k['total_jobs']} jobs on serverless "
                                                   f"({k['pct_on_serverless']}%)")
    c = st.columns(6)
    c[0].metric("Jobs", k["total_jobs"])
    c[1].metric("Still classic", k["classic_jobs"])
    c[2].metric("Ready", k["ready"])
    c[3].metric("Blocked", k["blocked"])
    c[4].metric("Approved", k["approved"])
    c[5].metric("Avg readiness (classic)", k["avg_readiness"] if k["avg_readiness"] is not None else "n/a")

    left, right = st.columns(2)
    with left:
        st.subheader("Status funnel (jobs)")
        funnel = pd.DataFrame(appdata.status_funnel(workloads), columns=["status", "jobs"]).set_index("status")
        st.bar_chart(funnel)
    with right:
        st.subheader("Waves (classic jobs)")
        names = {1: "1 quick wins", 2: "2 automated fixes", 3: "3 manual work", 4: "4 keep on classic"}
        wv = pd.DataFrame([(names.get(int(w), str(w)), n) for w, n in sorted(k["by_wave"].items(), key=lambda x: (x[0] is None, x[0]))
                           if w != "UNKNOWN" and w is not None], columns=["wave", "jobs"]).set_index("wave")
        st.bar_chart(wv)
    st.caption("Wave 1 = no blockers. Wave 2 = every blocker has an automated fix. Wave 3 = needs manual work. "
               "Wave 4 = needs an owner decision (ML runtime, custom image, Scala/R).")

# ---- Workloads ----
with tab_work:
    wdf = df(workloads)
    f1, f2, f3, f4 = st.columns(4)
    types = f1.multiselect("Type", sorted(wdf.workload_type.dropna().unique()), default=["JOB"])
    stats = f2.multiselect("Status", [s for s in appdata.STATUS_ORDER if s in set(wdf.status.dropna())])
    wvs = f3.multiselect("Wave", sorted(int(x) for x in wdf.wave.dropna().unique()))
    text = f4.text_input("Search name or owner")
    view = wdf
    if types:
        view = view[view.workload_type.isin(types)]
    if stats:
        view = view[view.status.isin(stats)]
    if wvs:
        view = view[view.wave.isin(wvs)]
    if text:
        t = text.lower()
        view = view[view.name.fillna("").str.lower().str.contains(t) | view.owner.fillna("").str.lower().str.contains(t)]
    st.dataframe(df(view.to_dict("records"), ["workload_id", "workload_type", "name", "owner", "compute_kind", "runtime_version",
                                              "task_count", "readiness_score", "status", "wave", "blockers", "warnings"]),
                 width="stretch", hide_index=True,
                 column_config={"readiness_score": st.column_config.ProgressColumn("readiness", min_value=0, max_value=100, format="%d")})
    st.download_button("Download CSV", view.to_csv(index=False), "workloads.csv", "text/csv")

# ---- Blockers ----
with tab_block:
    cov = appdata.limitation_coverage(data["findings"])
    st.subheader("Limitations found in this workspace")
    st.dataframe(df(cov), width="stretch", hide_index=True)
    fdf = df(data["findings"])
    if not fdf.empty:
        pick = st.multiselect("Limitation", sorted(fdf.limitation_id.unique(), key=lambda x: int(x[1:])))
        sev = st.multiselect("Severity", ["BLOCKER", "WARNING", "INFO"], default=["BLOCKER", "WARNING"])
        v = fdf[fdf.severity.isin(sev)]
        if pick:
            v = v[v.limitation_id.isin(pick)]
        st.dataframe(df(v.to_dict("records"), ["limitation_id", "severity", "name", "workload_type", "detail", "suggested_fix", "auto_fixable"]),
                     width="stretch", hide_index=True)

# ---- Fixes: the automated fixes, shown on real examples ----
with tab_fix:
    import json as _json
    from tracker import fixes as _fixes
    st.subheader("Automated fixes, shown on examples")
    st.caption("Every fix writes a NEW copy or a plan. Originals are never edited, and every change can be rolled back.")

    proofs = data.get("proofs", [])
    st.markdown("#### 1. Proof: the same notebook on serverless, before and after the rewrite (L6, L13)")
    if proofs:
        cols = st.columns(len(proofs))
        for col, p in zip(cols, sorted(proofs, key=lambda x: x["version"])):
            ok = p["result"] == "SUCCESS"
            col.metric(f"{p['version'].title()} notebook on serverless", p["result"], delta="runs" if ok else "fails",
                       delta_color="normal" if ok else "inverse")
            if p.get("error"):
                col.caption("Error: " + p["error"][:300])
        by_v = {p["version"]: p for p in proofs}
        if "ORIGINAL" in by_v and "REWRITTEN" in by_v:
            st.code(_fixes.unified_diff(by_v["ORIGINAL"]["source"] or "", by_v["REWRITTEN"]["source"] or "",
                                        "runnable_demo"), language="diff")
    else:
        st.info("Run the `[tracker] apply fixes + prove on serverless` job to run the proof.")

    st.markdown("#### 2. Code rewrites for every scanned notebook (L6 cache/checkpoint/global temp views, L13 ANSI and streaming)")
    rw = [r for r in data.get("rewrites", []) if r.get("decision") == "REWRITE_PROPOSED"]
    if rw:
        pick = st.selectbox("Notebook", [r["workload_id"] for r in rw])
        r = next(x for x in rw if x["workload_id"] == pick)
        st.write("**What changed:** " + "; ".join(_fixes.changes_list(r.get("changes_json"))))
        st.write("**Applied as a copy:** " + ("yes, `" + _fixes.demo_target_path("/Shared/serverless_migration_demo", pick,
                 "_serverless") + "`" if r.get("applied") else "not yet"))
        if r.get("original_source") and r.get("proposed_source"):
            st.code(_fixes.unified_diff(r["original_source"], r["proposed_source"], pick.split("/")[-1]), language="diff")
        elif r.get("proposed_source"):
            st.code(r["proposed_source"], language="python")
        else:
            st.caption("Source not stored (LIVE mode keeps notebook code out of tables).")
    jvm = [r for r in data.get("rewrites", []) if r.get("decision") in ("REWRITE_PYSPARK", "KEEP_ON_CLASSIC")]
    if jvm:
        st.markdown("**Scala and R (L7): decision per notebook**")
        st.dataframe(pd.DataFrame([{"notebook": r["workload_id"], "language": r["language"], "decision": r["decision"],
                                    "why": "; ".join(_fixes.changes_list(r.get("changes_json")))} for r in jvm]),
                     hide_index=True, width="stretch")

    st.markdown("#### 3. Init script to serverless environment (L3)")
    it = data.get("init_translations", [])
    if it:
        a, b = st.columns(2)
        a.caption(f"Before: {it[0]['script_name']} (classic init script)")
        a.code(it[0]["original_script"], language="bash")
        b.caption("After: serverless job environment")
        b.code(it[0]["environment_yaml"], language="yaml")

    st.markdown("#### 4. Job with 12 tasks split for the 10-task limit (L2)")
    sp = [p for p in data.get("split_plans_full", []) if p.get("plan_json")]
    if sp:
        plan = _json.loads(sp[0]["plan_json"])
        st.write(f"Job **{sp[0]['workload_id']}**: verified = {sp[0]['verified']}, applied = {sp[0]['applied']}")
        st.dataframe(pd.DataFrame(_fixes.split_rows(plan)), hide_index=True, width="stretch")

# ---- Approvals ----
with tab_appr:
    who, is_admin = current_user(data.get("approvers", []))
    st.write(f"Signed in as **{who or 'unknown'}**: " + ("approver, you can decide." if is_admin else
             "read-only. Only workspace admins on the approver list can decide (run `[tracker] grant app access` as an admin)."))
    pending = appdata.pending_approvals(workloads)
    st.subheader(f"Ready for a decision ({len(pending)})")
    st.caption("Only classic jobs in wave 1 or 2 enter the gate. Waves 3 and 4 must be remediated or decided on first.")
    if pending:
        pdf = df(pending, ["workload_id", "name", "owner", "wave", "readiness_score", "status", "blockers"])
        pdf = pdf.rename(columns={"blockers": "direct_blockers"})   # findings on the job itself; notebook findings roll into the wave
        pdf.insert(0, "select", False)
        edited = st.data_editor(pdf, hide_index=True, width="stretch", disabled=[c for c in pdf.columns if c != "select"])
        chosen = edited[edited.select].workload_id.tolist()
        note = st.text_input("Note (optional)")
        b1, b2, _ = st.columns([1, 1, 6])
        for label, decision, col in (("Approve", "APPROVED", b1), ("Reject", "REJECTED", b2)):
            if col.button(label, disabled=not (is_admin and chosen), key=decision):
                try:
                    by_id = {w["workload_id"]: w for w in pending}
                    ok = [i for i in chosen if approvals.check_approval(by_id[i]["status"], by_id[i]["wave"], is_admin, decision)[0]]
                    record_decisions(ok, decision, who, note)
                    load_all.clear()
                    st.success(f"{decision.lower()}: {len(ok)} job(s)")
                    st.rerun()
                except Exception as e:  # noqa: BLE001
                    st.error(f"Could not record the decision: {str(e)[:300]}. Does the app have MODIFY on the tracker tables?")
    st.subheader("Decision history")
    st.dataframe(df(data["approvals"]), width="stretch", hide_index=True)

# ---- Safety log ----
with tab_safe:
    st.subheader("Actions the tracker took or planned (L2, L10)")
    st.caption("dry_run = true means planned only; nothing was changed in the workspace.")
    st.dataframe(df(data["actions"]), width="stretch", hide_index=True)
    st.subheader("Large-job split plans (L2)")
    st.dataframe(df(data["split_plans"]), width="stretch", hide_index=True)

# ---- Drift & rollback (L9) ----
with tab_drift:
    st.subheader("Git (Asset Bundle) vs workspace drift (L9)")
    dr = data.get("drift", [])
    if not dr:
        st.info("Run the `[tracker] drift check (Git vs workspace)` job.")
    else:
        drifted = [r for r in dr if r["field"] != "job"]
        c = st.columns(3)
        c[0].metric("Jobs checked", len({r["job_key"] for r in dr}))
        c[1].metric("Jobs drifted", len({r["job_key"] for r in drifted}))
        c[2].metric("Auto-revertable", sum(1 for r in drifted if r["auto_revert"]))
        if drifted:
            st.dataframe(df(drifted, ["job_name", "field", "in_git", "in_workspace", "auto_revert", "checked_at"]),
                         width="stretch", hide_index=True)
        else:
            st.success("Every deployed job matches Git.")
        st.caption("Top-level settings are reverted by the drift job with enforce=true; task changes are fixed by "
                   "`databricks bundle deploy`. Every revert stores its undo.")
    st.subheader("Rollback")
    acts = data.get("actions", [])
    undoable = [a for a in acts if not a.get("dry_run") and a.get("action") not in ("ROLLED_BACK",)]
    st.caption("Real changes the tracker made. Run `[tracker] rollback` with an action_id (or `latest`) to undo one.")
    st.dataframe(df(undoable, ["performed_at", "action_id", "workload_id", "action", "detail", "performed_by"]), width="stretch", hide_index=True)
    done = [a for a in acts if a.get("action") == "ROLLED_BACK"]
    if done:
        st.write("Rolled back:")
        st.dataframe(df(done, ["performed_at", "workload_id", "detail", "performed_by"]), width="stretch", hide_index=True)

# ---- Validation ----
with tab_val:
    st.subheader("Classic vs serverless output diff (L8)")
    vdf = df(data["validation"])
    if vdf.empty:
        st.info("No validation runs yet. Run the `[tracker] A/B validation` job, then press Refresh.")
    else:
        c = st.columns(3)
        c[0].metric("Tables compared", len(vdf))
        c[1].metric("Passed", int(vdf.passed.sum()))
        c[2].metric("Failed", int((~vdf.passed.astype(bool)).sum()))
        if "note" in vdf.columns and vdf.note.fillna("").str.startswith("DEMO").any():
            st.caption("Rows marked DEMO were produced with both sides on serverless (Free Edition has no classic compute).")
        if "detail" in vdf.columns:
            import json as _json
            vdf["reason"] = vdf.detail.map(lambda d: (_json.loads(d).get("reason") if d else None))
            vdf = vdf.drop(columns=["detail"])
        st.dataframe(vdf, width="stretch", hide_index=True)

# ---- Cost ----
with tab_cost:
    st.subheader("DBUs and list cost, before vs after (L12)")
    cdf = df(data["cost"])
    if cdf.empty:
        st.info("No cost data yet. Run the `[tracker] cost comparison` job.")
    else:
        t = appdata.cost_totals(data["cost"])
        c = st.columns(4)
        c[0].metric("Jobs compared", t["jobs_compared"])
        if t["before_dbus"] is not None:
            c[1].metric("DBUs before", t["before_dbus"])
            c[2].metric("DBUs after", t["after_dbus"], delta=round(t["after_dbus"] - t["before_dbus"], 2), delta_color="inverse")
        if t["before_cost_usd"] is not None and t["after_cost_usd"] is not None:
            c[3].metric("List cost after (USD)", t["after_cost_usd"], delta=round(t["after_cost_usd"] - t["before_cost_usd"], 2), delta_color="inverse")
        st.caption("Serverless often uses fewer DBUs at a higher price per DBU, so cost can rise while DBUs fall. List prices only.")
        st.dataframe(cdf, width="stretch", hide_index=True)
        if "team_tag" in cdf.columns:
            by_team = cdf.groupby(cdf.team_tag.fillna("untagged"))[["before_cost_usd", "after_cost_usd"]].sum()
            st.bar_chart(by_team)
