"""Small static result dashboard. Reads bounded JSON only; never runs the pipeline.

No optional imports, database connections, process launches, HTTP or background
refresh. Missing values remain None. Native engine artifacts are read in place.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
from html import escape
import json
import math
from pathlib import Path
import re

SCHEMA = "m4-dashboard-v1"
MAX_FILE_BYTES = 2 * 1024 * 1024
MAX_TOTAL_BYTES = 8 * 1024 * 1024
MAX_FILES = 256
MAX_ALERTS = 100
ID = re.compile(r"[A-Za-z0-9_-]{1,128}\Z")
LABELS = {"passed": "通过", "failed": "失败", "completed": "完成", "not_run": "未运行",
          "partial": "部分结果", "unavailable": "无法读取"}


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def at(value, *path):
    for name in path:
        if not isinstance(value, dict):
            return None
        value = value.get(name)
    return value


def state(value):
    return "passed" if value is True else "failed" if value is False else "not_run"


def _object(pairs):
    result = {}
    for name, value in pairs:
        if name in result:
            raise ValueError("duplicate JSON field")
        result[name] = value
    return result


def _reject_constant(value):
    raise ValueError("nonfinite JSON number: " + value)


class Reader:
    """A bounded allowlist reader restricted to a single run directory."""
    def __init__(self, root):
        self.root = Path(root).resolve()
        self.sources = {}
        self._texts = {}
        self.issues = []
        self.total_bytes = 0

    def issue(self, source, message):
        record = {"source": str(source), "message": message}
        if record not in self.issues:
            self.issues.append(record)

    def path(self, name):
        path = Path(name)
        path = (path if path.is_absolute() else self.root / path).resolve()
        if path == self.root or self.root not in path.parents:
            self.issue("run", "拒绝读取运行目录外的文件")
            return None
        return path

    def text(self, name):
        path = self.path(name)
        if path is None:
            return None, None
        relative = path.relative_to(self.root).as_posix()
        if relative in self.sources:
            return self._texts.get(relative), relative
        record = {"path": relative, "status": "missing"}
        self.sources[relative] = record
        try:
            if not path.exists():
                return None, relative
            if not path.is_file():
                raise ValueError("source is not a regular file")
            size = path.stat().st_size
            if len(self.sources) > MAX_FILES or size > MAX_FILE_BYTES or self.total_bytes + size > MAX_TOTAL_BYTES:
                raise ValueError("bounded result reader limit exceeded")
            with path.open("rb") as stream:
                payload = stream.read(MAX_FILE_BYTES + 1)
            if len(payload) > MAX_FILE_BYTES or len(payload) != size:
                raise ValueError("source changed or exceeded read limit")
            self.total_bytes += len(payload)
            record.update(status="loaded", bytes=len(payload), sha256=hashlib.sha256(payload).hexdigest())
            decoded = payload.decode("utf-8-sig")
            self._texts[relative] = decoded
            return decoded, relative
        except (OSError, ValueError, UnicodeError) as exc:
            record["status"] = "unavailable"
            self.issue(relative, str(exc))
            return None, relative

    def json(self, name):
        payload, relative = self.text(name)
        if payload is None:
            return None
        try:
            result = json.loads(payload, parse_constant=_reject_constant, object_pairs_hook=_object)
            if not isinstance(result, dict):
                raise ValueError("expected a JSON object")
            return result
        except (ValueError, TypeError) as exc:
            self.sources[relative]["status"] = "unavailable"
            self.issue(relative, str(exc))
            return None

    def number(self, value, source, *, integer=False, allow_negative=False):
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, (int, float)) or (isinstance(value, float) and not math.isfinite(value)) or (value < 0 and not allow_negative) or (integer and not isinstance(value, int)):
            self.issue(source, "结果数值的类型或范围不符合合同")
            return None
        return value

    def alerts(self, name, scope):
        payload, relative = self.text(name)
        if payload is None:
            return {"status": "not_run", "count": None, "records": [], "scope": scope}
        unique = {}
        malformed = False
        for line_number, line in enumerate(payload.splitlines(), 1):
            if not line.strip():
                continue
            try:
                record = json.loads(line, parse_constant=_reject_constant, object_pairs_hook=_object)
                if not isinstance(record, dict) or not isinstance(record.get("alert_id"), str) or not record["alert_id"]:
                    raise ValueError("missing structured alert identity")
                unique[record["alert_id"]] = {k: record.get(k) for k in
                                            ("alert_id", "timestamp_utc", "partition", "check", "reason")}
            except (ValueError, TypeError):
                malformed = True
                self.issue(relative, "无效的结构化告警行 " + str(line_number))
        records = list(unique.values())
        return {"status": "unavailable" if malformed else "completed", "count": None if malformed else len(records),
                "records": records[-MAX_ALERTS:], "display_limited": len(records) > MAX_ALERTS,
                "scope": scope, "source": relative}


def linked_downstream(parent_reader, child_root, parent_manual):
    """Read two bounded evidence folders; never overwrite parent artifacts."""
    child = Reader(child_root)
    binding = child.json("input-binding.json")
    run = child.json("downstream-run.json")
    verification = child.json("input-verification.json")
    events = child.json("downstream-events.json")
    if not all(isinstance(value, dict) for value in (binding, run, verification, events)):
        raise ValueError("downstream completion receipts missing or invalid")
    parent = binding.get("parent")
    if not isinstance(parent, dict) or parent.get("run_root") != str(parent_reader.root):
        raise ValueError("downstream belongs to a different data run")
    if parent.get("manual_run_id") != at(parent_manual, "run_id") or run.get("parent") != parent or verification.get("parent") != parent:
        raise ValueError("downstream parent run identity mismatch")
    for name, digest in parent.get("receipt_sha256", {}).items():
        payload, relative = parent_reader.text(name)
        if payload is None or parent_reader.sources[relative].get("sha256") != digest:
            raise ValueError("downstream parent receipt no longer matches: " + name)
    if not parent.get("receipt_sha256") or run.get("success") is not True or verification.get("unchanged") is not True:
        raise ValueError("downstream did not complete with unchanged accepted inputs")
    if verification.get("binding_sha256") != child.sources["input-binding.json"].get("sha256"):
        raise ValueError("downstream binding verification mismatch")
    keys = [event.get("asset_key") for event in events.get("events", [])
            if isinstance(event, dict) and event.get("event_type") == "ASSET_MATERIALIZATION"]
    if sorted(keys) != [["api_benchmark"], ["recsys_train_eval"]]:
        raise ValueError("downstream actual materializations do not match selected assets")
    model, api = child.json("model-run.json"), child.json("api-run.json")
    if at(model, "status") != "completed" or at(model, "seeds") != [42, 43, 44] or at(api, "status") != "completed":
        raise ValueError("downstream model/API are not completed")
    raw = child.json(at(api, "raw_http")) if isinstance(at(api, "raw_http"), str) else None
    if raw is None or child.issues:
        raise ValueError("downstream HTTP/source evidence unavailable")
    lineage = {"parent_run_id": parent["manual_run_id"], "child_run_id": run.get("run_id"),
               "parent_source_git_sha": parent.get("source_git_sha"),
               "child_run_root": str(child.root), "scope": "data replay from parent; model/API from independent append-only child"}
    return model, api, raw, lineage, [{**source, "run_root": str(child.root), "scope": "downstream_child"}
                                     for source in child.sources.values()]


def build_snapshot(root, *, generated_at=None, downstream_root=None):
    rd = Reader(root)
    manual = rd.json("manual-run.json")
    summary = rd.json("summary.json")
    provenance = rd.json("provenance.json")
    config = rd.json("config.json")
    manifest = rd.json("arrival_manifest.json")
    promoted = rd.json("promoted.json")
    parity = rd.json("full-incremental-parity.json")
    full = rd.json("full-reference/reference.json")
    coordinator = rd.json("coordinator.json")
    spark = rd.json("spark-original/spark-run.json")
    spark_parity = rd.json("spark-original/spark-dbt-parity.json")
    model = rd.json("model-run.json")
    api = rd.json("api-run.json")
    demos = rd.json("demonstrations.json")
    freshness_demo = rd.json("freshness-demo.json")
    if model is None:
        model = at(summary, "model")
    if api is None:
        api = at(summary, "api")
    latest_checks = None
    attempt = at(promoted, "attempt_id")
    if isinstance(attempt, str) and ID.fullmatch(attempt):
        latest_checks = rd.json(f"attempts/{attempt}/output-checks.json")
    elif attempt is not None:
        rd.issue("promoted.json#/attempt_id", "无效的运行标识")

    checks = at(latest_checks, "checks")
    if not isinstance(checks, list):
        checks = []
    malformed_members = any(not isinstance(c, dict) for c in checks)
    if malformed_members:
        rd.issue("output-checks.json#/checks", "检查列表包含无效记录")
    checks = [c for c in checks if isinstance(c, dict)]
    invalid_states = any(not isinstance(c.get("passed"), bool) for c in checks)
    if invalid_states:
        rd.issue("output-checks.json#/checks", "检查状态缺失或不符合合同")
    quality_state = "failed" if any(c.get("passed") is False for c in checks) else "unavailable" if malformed_members or invalid_states else "passed" if checks else "not_run"
    fresh = next((c for c in checks if c.get("name") == "processing_freshness"), None)
    partitions = at(manifest, "batches")
    received = at(promoted, "visible_prefix")
    batch_rows = []
    per_batch = at(summary, "per_batch")
    if isinstance(per_batch, list):
        for row in per_batch[:MAX_FILES]:
            if isinstance(row, dict):
                seconds = rd.number(row.get("etl_and_test_wall_seconds"), "summary.json#/per_batch")
                batch_rows.append({"partition": row.get("partition"), "seconds": seconds,
                                   "source": "summary.json#/per_batch", "status": "completed" if seconds is not None else "unavailable" if row.get("etl_and_test_wall_seconds") is not None else "not_run"})
    else:
        ids = at(coordinator, "arrival_child_run_ids")
        if isinstance(ids, list):
            for run_id in ids[:MAX_FILES]:
                if not isinstance(run_id, str) or not ID.fullmatch(run_id):
                    rd.issue("coordinator.json#/arrival_child_run_ids", "无效的子运行标识")
                    continue
                child = rd.json(f"children/{run_id}.json")
                values = []
                for label in ("dbt-stage", "dbt-input-tests", "dbt-marts"):
                    process = rd.json(f"attempts/{run_id}/{label}/process.json")
                    values.append(rd.number(at(process, "wall_seconds"), f"attempts/{run_id}/{label}/process.json#/wall_seconds"))
                batch_rows.append({"partition": at(child, "partition"), "seconds": sum(values) if all(v is not None for v in values) else None,
                                   "status": "completed" if at(child, "success") is True else "failed" if at(child, "success") is False else "not_run",
                                   "source": f"children/{run_id}.json"})

    def demo_file(pointer, name, summary_key):
        branch = at(demos, pointer)
        value = rd.json(Path(branch) / name) if isinstance(branch, str) else None
        return value if value is not None else at(summary, summary_key)
    backfill = demo_file("backfill_branch", "backfill-proof.json", "backfill")
    bad_batch = demo_file("bad_batch_branch", "bad-batch-proof.json", "quality_gate")
    recovery = demo_file("bad_batch_branch", "recovery-proof.json", "recovery")
    alerts = [rd.alerts("alerts/alerts.jsonl", "主回放")]
    bad_branch = at(demos, "bad_batch_branch")
    if isinstance(bad_branch, str):
        alerts.append(rd.alerts(Path(bad_branch) / "alerts/alerts.jsonl", "拦截演示"))
    api_raw = None
    raw_path = at(api, "raw_http")
    if at(api, "status") == "completed" and isinstance(raw_path, str):
        api_raw = rd.json(raw_path)
    downstream_lineage, downstream_sources = None, []
    if downstream_root is not None:
        try:
            model, api, api_raw, downstream_lineage, downstream_sources = linked_downstream(rd, downstream_root, manual)
        except (OSError, ValueError, TypeError, KeyError) as exc:
            rd.issue("downstream", str(exc))
    spark_tables = at(spark, "tables") if spark is not None else at(summary, "spark_tables")
    spark_rows = []
    if isinstance(spark_tables, dict):
        for name in ("fct_order_lines", "batch_department_integer_stats", "batch_hourly_integer_stats", "batch_product_integer_stats"):
            table = spark_tables.get(name)
            if isinstance(table, dict):
                spark_rows.append({"name": name, "rows": rd.number(table.get("rows"), "spark tables/" + name, integer=True),
                                   "partitions": rd.number(table.get("nonempty_partition_count"), "spark tables/" + name, integer=True),
                                   "files": rd.number(at(table, "physical", "parquet_file_count"), "spark tables/" + name, integer=True),
                                   "bytes": rd.number(at(table, "physical", "parquet_bytes"), "spark tables/" + name, integer=True)})
    model_rows = []
    for method in ("training_popularity", "two_tower_hnsw", "two_tower_lambdamart"):
        values = at(model, "summary", method)
        if at(model, "status") == "completed" and isinstance(values, dict):
            model_rows.append({"method": method, "metrics": {metric: {
                "mean": rd.number(at(values, metric, "mean"), "model-run.json#/summary/" + method + "/" + metric),
                "std": rd.number(at(values, metric, "std"), "model-run.json#/summary/" + method + "/" + metric)}
                for metric in ("ndcg@10", "recall@10")}})
    status = "completed" if at(manual, "success") is True else "failed" if at(manual, "success") is False else "partial" if any(s["status"] == "loaded" for s in rd.sources.values()) else "not_run"
    if rd.issues and status == "completed":
        status = "partial"
    kind = "synthetic_test" if at(provenance, "data_kind") == "synthetic_test" else "test_subset" if at(config, "fixture") is True else "runtime"
    incremental = rd.number(at(summary, "incremental_etl_and_tests_wall_seconds_total"), "summary.json#/incremental_etl_and_tests_wall_seconds_total")
    if incremental is None and batch_rows and all(b["seconds"] is not None for b in batch_rows):
        incremental = sum(b["seconds"] for b in batch_rows)
    snapshot = {
        "schema": SCHEMA, "generated_at_utc": generated_at or utc_now(), "data_kind": kind,
        "run": {"status": status, "id": at(manual, "run_id") or at(coordinator, "parent_run_id"),
                "environment": at(provenance, "environment")},
        "replay": {"status": state(at(parity, "passed") if parity is not None else at(summary, "incremental_equals_full")),
                   "line_rows": rd.number(at(parity, "tables", "marts.fct_order_lines", "incremental", "rows"), "full-incremental-parity.json#/tables/marts.fct_order_lines/incremental/rows", integer=True),
                   "order_rows": rd.number(at(parity, "tables", "marts.fct_orders", "incremental", "rows"), "full-incremental-parity.json#/tables/marts.fct_orders/incremental/rows", integer=True),
                   "received_batches": len(received) if isinstance(received, list) else None,
                   "declared_batches": len(partitions) if isinstance(partitions, list) else None,
                   "full_seconds": rd.number(at(full, "process", "wall_seconds") if full is not None else at(summary, "full_refresh_etl_and_tests_wall_seconds"), "full-reference/reference.json#/process/wall_seconds"),
                   "incremental_seconds": incremental, "batches": batch_rows},
        "quality": {"status": quality_state, "checks": [{"name": c.get("name"), "status": state(c.get("passed"))} for c in checks],
                    "passed_count": sum(c.get("passed") is True for c in checks) if checks else None,
                    "failed_count": sum(c.get("passed") is False for c in checks) if checks else None},
        "freshness": {"status": state(at(fresh, "passed")), "age_seconds": rd.number(at(fresh, "age_seconds"), "processing_freshness/age_seconds", allow_negative=True),
                      "maximum_age_seconds": rd.number(at(fresh, "maximum_age_seconds"), "processing_freshness/maximum_age_seconds"),
                      "materialized_at": at(promoted, "materialized_at"), "scope": "检查执行时的处理时效与回放进度；不是真实订单事件时效",
                      "controlled_demo": {"fresh": state(at(freshness_demo, "fresh", "passed")),
                                          "stale": state(at(freshness_demo, "stale", "passed")),
                                          "clock_injected": at(freshness_demo, "clock_injected") is True}},
        "spark": {"status": state(at(spark_parity, "passed") if spark_parity is not None else at(summary, "spark_exact_parity")),
                  "seconds": rd.number(at(spark, "etl_wall_seconds_excluding_physical_hashes") if spark is not None else at(summary, "spark_etl_wall_seconds"), "spark-run.json#/etl_wall_seconds_excluding_physical_hashes"),
                  "master": at(spark, "master_actual") if spark is not None else at(summary, "spark_master_actual"), "tables": spark_rows},
        "backfill": {"status": state(at(backfill, "corrected_parity_passed")),
                     "selected": at(backfill, "selected"), "changed": at(backfill, "changed"), "unchanged": at(backfill, "unchanged"),
                     "blocking_demo": "passed" if at(bad_batch, "run_success") is False and at(bad_batch, "downstream_blocked") is True and at(bad_batch, "promoted_pointer_unchanged") is True else "not_run" if bad_batch is None else "failed",
                     "recovery": state(at(recovery, "original_input_parity", "passed"))},
        "alerts": alerts,
        "recommendations": {"status": at(model, "status") if at(model, "status") in ("completed", "not run") else "not_run", "rows": model_rows,
                            "seeds": at(model, "seeds"), "std_definition": "copied standard deviations; the run artifact declares the estimator"},
        "api": {"status": at(api, "status") if at(api, "status") in ("completed", "not run") else "not_run",
                "requests": rd.number(at(api_raw, "requests"), "api/raw-http.json#/requests", integer=True),
                "errors": rd.number(at(api_raw, "errors"), "api/raw-http.json#/errors", integer=True),
                "client_p50_ms": rd.number(at(api_raw, "client_latency_ms", "p50"), "api/raw-http.json#/client_latency_ms/p50"),
                "client_p99_ms": rd.number(at(api_raw, "client_latency_ms", "p99"), "api/raw-http.json#/client_latency_ms/p99")},
        "sources": list(rd.sources.values()) + downstream_sources, "issues": rd.issues,
        "downstream_lineage": downstream_lineage,
        "refresh": "manual regeneration only; no polling, daemon, schedule or external requests",
        "provenance": provenance,
    }
    # Data discovered late (e.g. invalid numeric fields) must not leave a green headline.
    if rd.issues and snapshot["run"]["status"] == "completed":
        snapshot["run"]["status"] = "partial"
    return snapshot


def text(value):
    return escape(str(value), quote=True)


def display(value, unit=""):
    if value is None:
        return "未运行"
    # No rounding or estimated values: preserve the number produced by JSON.
    return text(value) + (" " + text(unit) if unit else "")


def pill(status):
    status = "not_run" if status == "not run" else status
    return '<span class="pill ' + text(status) + '">' + text(LABELS.get(status, "未运行")) + '</span>'


def render_html(snapshot):
    replay, quality, fresh = snapshot["replay"], snapshot["quality"], snapshot["freshness"]
    spark, model, api = snapshot["spark"], snapshot["recommendations"], snapshot["api"]
    batches = '<p class="empty">尚无批次构建记录。</p>'
    if replay["batches"]:
        batches = '<table><thead><tr><th>批次</th><th>构建与测试</th><th>状态</th></tr></thead><tbody>' + ''.join(
            '<tr><td>' + text(b["partition"]) + '</td><td>' + display(b["seconds"], "s") + '</td><td>' + pill(b["status"]) + '</td></tr>' for b in replay["batches"]) + '</tbody></table>'
    spark_labels = {"fct_order_lines": "订单行", "batch_department_integer_stats": "部门", "batch_hourly_integer_stats": "时段", "batch_product_integer_stats": "商品"}
    spark_table = '<p class="empty">尚无 Spark 结果。</p>'
    if spark["tables"]:
        spark_table = '<table><thead><tr><th>数据</th><th>行数</th><th>分区</th><th>Parquet 字节</th></tr></thead><tbody>' + ''.join(
            '<tr><td>' + text(spark_labels[t["name"]]) + '</td><td>' + display(t["rows"]) + '</td><td>' + display(t["partitions"]) + '</td><td>' + display(t["bytes"]) + '</td></tr>' for t in spark["tables"]) + '</tbody></table>'
    names = {"training_popularity": "Popularity", "two_tower_hnsw": "Two-tower", "two_tower_lambdamart": "Two-tower + LambdaMART"}
    model_table = '<p class="empty">本次尚未运行推荐模型。</p>'
    if model["rows"]:
        def score(row, metric):
            item = row["metrics"][metric]
            return "未运行" if item["mean"] is None or item["std"] is None else display(item["mean"]) + ' ± ' + display(item["std"])
        model_table = '<table><thead><tr><th>方法</th><th>NDCG@10</th><th>Recall@10</th></tr></thead><tbody>' + ''.join(
            '<tr><td>' + text(names[row["method"]]) + '</td><td>' + score(row, "ndcg@10") + '</td><td>' + score(row, "recall@10") + '</td></tr>' for row in model["rows"]) + '</tbody></table><p class="note">均值 ± 标准差。</p>'
    check_names = {"batch_row_reconciliation": "批次行数", "null_key_rate_zero": "必需字段", "unique_grains": "唯一键", "arrived_order_catalog_fk": "订单与商品关联",
                   "order_line_correction_closure": "订单汇总一致", "arrival_completeness": "到达完整性", "source_fact_exact_values": "源数据与事实值", "processing_freshness": "处理时效"}
    checks = '<p class="empty">尚无质量检查记录。</p>' if not quality["checks"] else '<ul class="checks">' + ''.join(
        '<li><span>' + text(check_names.get(c["name"], "数据检查")) + '</span>' + pill(c["status"]) + '</li>' for c in quality["checks"]) + '</ul>'
    alert_rows = []
    for group in snapshot["alerts"]:
        for alert in group["records"]:
            alert_rows.append('<tr><td>' + text(group["scope"]) + '</td><td>' + text(alert["timestamp_utc"] or "时间未记录") + '</td><td>' + text(alert["partition"] or "—") + '</td><td>' + text(alert["reason"] or "原因未记录") + '</td></tr>')
    alerts = '<p class="empty">暂无结构化告警记录。</p>' if not alert_rows else '<table><thead><tr><th>范围</th><th>时间</th><th>批次</th><th>记录</th></tr></thead><tbody>' + ''.join(alert_rows) + '</tbody></table>'
    backfill = snapshot["backfill"]
    selected = '未运行' if not isinstance(backfill["selected"], list) else ', '.join(text(p) for p in backfill["selected"])
    issues = '' if not snapshot["issues"] else '<div class="notice">部分结果无法读取，详情见快照 JSON 的来源记录。</div>'
    kind = snapshot["data_kind"]
    fixture = '<div class="notice">Synthetic test fixture · 合成测试数据，用于流程验证。</div>' if kind == "synthetic_test" else '<div class="notice">测试子集 · 仅用于正确性检查，不是全量性能结果。</div>' if kind == "test_subset" else ''
    lineage = snapshot.get("downstream_lineage")
    if lineage:
        fixture += '<div class="notice">数据回放：' + text(lineage["parent_run_id"]) + '；推荐与HTTP来自独立下游运行：' + text(lineage["child_run_id"]) + '。</div>'
    progress = "未运行" if replay["received_batches"] is None or replay["declared_batches"] is None else display(replay["received_batches"]) + ' / ' + display(replay["declared_batches"])
    return '''<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>订单数据看板</title><style>
*{box-sizing:border-box}body{margin:0;background:#f5f6f3;color:#20332d;font:15px/1.55 system-ui,-apple-system,"Segoe UI",sans-serif}main{max-width:1180px;margin:auto;padding:40px 24px}header,.topline,.section-title{display:flex;align-items:center;justify-content:space-between;gap:16px}h1{font-size:30px;letter-spacing:-.04em;margin:0}h2{font-size:18px;margin:0}p{margin:8px 0}.muted,.note{color:#66766e}.note{font-size:12px}.topline{font-size:12px;margin-bottom:10px}.actions{display:flex;gap:10px;align-items:center;flex-wrap:wrap}button,a.button{border:1px solid #ccd5ce;border-radius:8px;background:white;color:#294a3a;padding:9px 13px;text-decoration:none;font:inherit;cursor:pointer}.grid{display:grid;grid-template-columns:1fr 1fr;gap:20px;margin-top:20px}.cards{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-top:26px}.card,section{background:white;border:1px solid #dce2dc;border-radius:12px;padding:20px}.card strong{display:block;font-size:25px;letter-spacing:-.04em;margin:12px 0 3px;overflow-wrap:anywhere}.card label{color:#66766e;font-size:13px}.span{grid-column:1/-1}.pill{display:inline-block;white-space:nowrap;border-radius:20px;padding:3px 10px;font-size:12px;background:#edf0ed;color:#65716a}.passed,.completed{background:#e1f0e6;color:#23613a}.failed{background:#f9e5df;color:#953d29}.partial,.unavailable{background:#fff0d4;color:#875b15}table{width:100%;border-collapse:collapse;font-size:13px;margin-top:16px}th,td{text-align:left;padding:10px 5px;border-bottom:1px solid #edf0ec;vertical-align:top;overflow-wrap:anywhere}th{font-size:12px;color:#66766e;font-weight:500}.empty{color:#7a877f;padding:22px 0}.measure{display:flex;justify-content:space-between;border-bottom:1px solid #edf0ec;padding:12px 0;gap:15px}.measure b{font-weight:500;text-align:right;overflow-wrap:anywhere}.checks{list-style:none;padding:0;margin:12px 0 0}.checks li{display:flex;align-items:center;justify-content:space-between;padding:9px 0;border-bottom:1px solid #edf0ec}.notice{margin-top:18px;padding:12px 16px;border-radius:8px;background:#fff0d4;color:#6c4b17;font-size:13px}footer{color:#718078;font-size:12px;margin-top:24px}.scroll{overflow:auto}.section-title{margin-bottom:6px}a{color:#2a6948} @media(max-width:800px){.cards{grid-template-columns:repeat(2,1fr)}.grid{grid-template-columns:1fr}header{align-items:flex-start;flex-direction:column}}@media(max-width:460px){main{padding:24px 14px}.card,section{padding:16px}.card strong{font-size:21px}.topline{align-items:flex-start;flex-direction:column}}
</style></head><body><main><div class="topline"><span>GROCERY DATA</span><span>手动快照 · ''' + text(snapshot["generated_at_utc"]) + '''</span></div><header><div><h1>订单数据看板</h1><p class="muted">查看每次回放的构建、检查与推荐结果。</p></div><div class="actions">''' + pill(snapshot["run"]["status"]) + '''<a class="button" href="snapshot.json">查看快照 JSON</a><button type="button" onclick="location.reload()">刷新页面</button></div></header>''' + fixture + issues + '''<div class="cards"><div class="card"><label>订单行</label><strong>''' + display(replay["line_rows"]) + '''</strong><span class="note">来自本次增量结果</span></div><div class="card"><label>已接收批次</label><strong>''' + progress + '''</strong><span class="note">模拟到达</span></div><div class="card"><label>增量与全量</label><strong>''' + pill(replay["status"]) + '''</strong><span class="note">逐行与哈希对比</span></div><div class="card"><label>处理时效</label><strong>''' + pill(fresh["status"]) + '''</strong><span class="note">检查执行时的状态</span></div></div><div class="grid"><section><div class="section-title"><h2>构建记录</h2>''' + pill(replay["status"]) + '''</div><div class="measure"><span>全量构建与测试</span><b>''' + display(replay["full_seconds"], "s") + '''</b></div><div class="measure"><span>累计增量构建与测试</span><b>''' + display(replay["incremental_seconds"], "s") + '''</b></div><div class="scroll">''' + batches + '''</div><p class="note">时长包含构建与测试；准备、额外审计和导出另计。</p></section><section><div class="section-title"><h2>Spark 与 Parquet</h2>''' + pill(spark["status"]) + '''</div><div class="measure"><span>处理时长</span><b>''' + display(spark["seconds"], "s") + '''</b></div><div class="scroll">''' + spark_table + '''</div><p class="note">整数统计精确对比；物理文件哈希另计。</p></section><section><div class="section-title"><h2>质量与时效</h2>''' + pill(quality["status"]) + '''</div>''' + checks + '''<div class="measure"><span>检查时的物化年龄</span><b>''' + display(fresh["age_seconds"], "s") + '''</b></div><div class="measure"><span>允许的处理年龄</span><b>''' + display(fresh["maximum_age_seconds"], "s") + '''</b></div><p class="note">处理时效与模拟回放进度，按检查时的记录展示。</p></section><section><div class="section-title"><h2>回填与恢复</h2>''' + pill(backfill["status"]) + '''</div><div class="measure"><span>所选批次</span><b>''' + selected + '''</b></div><div class="measure"><span>坏批次拦截演示</span>''' + pill(backfill["blocking_demo"]) + '''</div><div class="measure"><span>干净输入恢复</span>''' + pill(backfill["recovery"]) + '''</div><p class="note">受控更正与坏批次拦截演示。</p></section><section class="span"><div class="section-title"><h2>推荐结果</h2>''' + pill(model["status"]) + '''</div><div class="scroll">''' + model_table + '''</div><div class="measure"><span>HTTP 测量</span>''' + pill(api["status"]) + '''</div><div class="measure"><span>客户端 p50 / p99</span><b>''' + display(api["client_p50_ms"], "ms") + ' / ' + display(api["client_p99_ms"], "ms") + '''</b></div></section><section class="span"><h2>本地告警</h2><div class="scroll">''' + alerts + '''</div><p class="note">主回放与测试拦截记录分别标明。</p></section></div><footer>缺失结果显示“未运行”。手动生成新快照后刷新页面。</footer></main></body></html>'''


def write_dashboard(root, destination, *, generated_at=None, downstream_root=None):
    destination = Path(destination).resolve()
    snapshot = build_snapshot(root, generated_at=generated_at, downstream_root=downstream_root)
    # Create-only: a manual refresh writes a new version, never edits old evidence.
    destination.mkdir(parents=True, exist_ok=False)
    with (destination / "snapshot.json").open("x", encoding="utf-8", newline="\n") as stream:
        json.dump(snapshot, stream, indent=2, ensure_ascii=False, allow_nan=False)
        stream.write("\n")
    with (destination / "index.html").open("x", encoding="utf-8", newline="\n") as stream:
        stream.write(render_html(snapshot))
    return snapshot
