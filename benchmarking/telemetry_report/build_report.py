# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Build a self-contained HTML run report from ate_env telemetry outputs.

Inputs are the three telemetry layers, any subset of them:

  * ``--report``  RunReport JSON files written by ``RunReport.write(REPORT_DIR)``
                  (a file or a directory; every ``*.json`` in it is read)
  * ``--spans``   OpenTelemetry spans in OTLP/JSON, one export request per line,
                  as written by the collector's ``file`` exporter or by
                  ``ate_env.telemetry.tracing`` test exporters (a file or directory)
  * ``--metrics`` Prometheus text scrapes (``curl .../metrics > x.prom``) of the
                  trainer process and/or ate-env-api, or the collector's ``file``
                  exporter metrics output (OTLP/JSON lines, e.g. the
                  ``prometheus`` receiver scraping ate-env-api); files or a directory

Writes ``run.json`` (the aggregated data, the report's only input) and
``report.html`` into ``--out``. Standard library only.

    python benchmarking/telemetry_report/build_report.py \\
        --report /tmp/reports --spans /tmp/otel/spans.jsonl \\
        --metrics /tmp/scrapes --out /tmp/telemetry-report --title verl-e1
"""

from __future__ import annotations

import argparse
import glob
import json
import math
import os
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any

TEMPLATE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "template.html")

# The phase order of RunReport.summary(), see interfaces.md.
PHASE_ORDER = ["preflight", "plan", "ensure_template", "warm_pool", "acquire", "process",
               "recycle", "release", "teardown"]

MAX_TRACES = 200       # waterfalls embedded in the HTML
MAX_SPANS_PER_TRACE = 400

# OTLP span kinds; the timeline shows the caller's side of a trace (fleet
# phases, client calls, benchmark events), not the servers that answer them.
SPAN_KIND_SERVER = 2
_SPAN_KINDS = {"SPAN_KIND_UNSPECIFIED": 0, "SPAN_KIND_INTERNAL": 1, "SPAN_KIND_SERVER": 2,
               "SPAN_KIND_CLIENT": 3, "SPAN_KIND_PRODUCER": 4, "SPAN_KIND_CONSUMER": 5}
# service.name of the ate-env servers (internal/telemetry); their spans stay
# out of the timeline even when INTERNAL.
SERVER_SERVICES = ("ate-env-api", "ate-env-guest")


# ----------------------------------------------------------------------------- io


def _files(src: str | None, patterns: tuple[str, ...]) -> list[str]:
    if not src:
        return []
    out: list[str] = []
    for part in src.split(","):
        part = part.strip()
        if os.path.isdir(part):
            for pat in patterns:
                out.extend(sorted(glob.glob(os.path.join(part, "**", pat), recursive=True)))
        elif part:
            out.extend(sorted(glob.glob(part)) or [part])
    return out


def load_reports(src: str | None) -> list[dict[str, Any]]:
    reports = []
    for path in _files(src, ("*.json",)):
        with open(path, encoding="utf-8") as fh:
            try:
                data = json.load(fh)
            except json.JSONDecodeError:
                continue
        if isinstance(data, dict) and "phases" in data and "strategy" in data:
            data = dict(data)
            data["_file"] = os.path.basename(path)
            reports.append(data)
    return reports


def _attr_value(v: dict[str, Any]) -> Any:
    for k in ("stringValue", "boolValue", "doubleValue"):
        if k in v:
            return v[k]
    if "intValue" in v:
        return int(v["intValue"])
    if "arrayValue" in v:
        return [_attr_value(x) for x in v["arrayValue"].get("values", [])]
    return None


def _attrs(lst: list[dict[str, Any]] | None) -> dict[str, Any]:
    return {a["key"]: _attr_value(a.get("value", {})) for a in (lst or [])}


def _hex_id(v: str | None) -> str:
    """OTLP/JSON ids are hex; tolerate base64 (proto JSON) as well."""
    if not v:
        return ""
    if re.fullmatch(r"[0-9a-fA-F]+", v):
        return v.lower()
    import base64

    try:
        return base64.b64decode(v).hex()
    except Exception:  # noqa: BLE001
        return v


def parse_otlp(obj: dict[str, Any]) -> list[dict[str, Any]]:
    spans = []
    for rs in obj.get("resourceSpans", []) or []:
        res = _attrs((rs.get("resource") or {}).get("attributes"))
        service = res.get("service.name", "unknown")
        for ss in rs.get("scopeSpans", []) or rs.get("instrumentationLibrarySpans", []) or []:
            for s in ss.get("spans", []) or []:
                start = int(s.get("startTimeUnixNano", 0))
                end = int(s.get("endTimeUnixNano", start))
                status = (s.get("status") or {}).get("code", 0)
                if isinstance(status, str):
                    status = {"STATUS_CODE_UNSET": 0, "STATUS_CODE_OK": 1,
                              "STATUS_CODE_ERROR": 2}.get(status, 0)
                kind = s.get("kind", 0)
                if isinstance(kind, str):
                    kind = _SPAN_KINDS.get(kind, 0)
                spans.append({
                    "trace_id": _hex_id(s.get("traceId")),
                    "span_id": _hex_id(s.get("spanId")),
                    "parent_id": _hex_id(s.get("parentSpanId")),
                    "name": s.get("name", ""),
                    "service": service,
                    "start": start / 1e9,
                    "dur": max(0.0, (end - start) / 1e9),
                    "error": status == 2,
                    "kind": kind,
                    "attrs": _attrs(s.get("attributes")),
                })
    return spans


def load_spans(src: str | None) -> list[dict[str, Any]]:
    spans: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for path in _files(src, ("*.json", "*.jsonl")):
        with open(path, encoding="utf-8") as fh:
            text = fh.read()
        chunks: list[Any] = []
        try:
            chunks = [json.loads(text)]
        except json.JSONDecodeError:
            for line in text.splitlines():
                line = line.strip()
                if line:
                    try:
                        chunks.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        for c in chunks:
            for obj in (c if isinstance(c, list) else [c]):
                if not isinstance(obj, dict):
                    continue
                for s in parse_otlp(obj):
                    key = (s["trace_id"], s["span_id"])
                    if key not in seen:
                        seen.add(key)
                        spans.append(s)
    return spans


_SAMPLE = re.compile(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{(.*)\})?\s+(\S+)')
_LABEL = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"')


def parse_prom(text: str) -> list[tuple[str, dict[str, str], float]]:
    out = []
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        m = _SAMPLE.match(line)
        if not m:
            continue
        try:
            val = float(m.group(4))
        except ValueError:
            continue
        labels = dict(_LABEL.findall(m.group(3) or ""))
        out.append((m.group(1), labels, val))
    return out


def _num(v: Any) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _point_value(dp: dict[str, Any]) -> float:
    return _num(dp["asDouble"]) if "asDouble" in dp else _num(dp.get("asInt", 0))


def parse_otlp_metrics(text: str) -> dict[str, list[tuple[str, dict[str, str], float]]]:
    """Read the collector ``file`` exporter's OTLP/JSON metrics lines.

    Every line is one export of cumulative points, so only the newest point
    of each series counts. Returns Prometheus-style samples per resource
    (one scraped process each), so histograms become ``_bucket``/``_sum``/
    ``_count`` samples and monotonic sums get their ``_total`` name back.
    """
    latest: dict[tuple[str, str, str], tuple[int, list[tuple[str, dict[str, str], float]]]] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        for obj in (obj if isinstance(obj, list) else [obj]):
            if not isinstance(obj, dict):
                continue
            for rm in obj.get("resourceMetrics", []) or []:
                res = _attrs((rm.get("resource") or {}).get("attributes"))
                origin = str(res.get("service.instance.id") or res.get("instance")
                             or res.get("service.name") or "otlp")
                for sm in rm.get("scopeMetrics", []) or []:
                    for m in sm.get("metrics", []) or []:
                        name = m.get("name", "")
                        if not name.startswith(("ate_env_", "ate_env_fleet_")):
                            continue
                        for kind in ("histogram", "sum", "gauge"):
                            if kind in m:
                                break
                        else:
                            continue
                        body = m[kind]
                        for dp in body.get("dataPoints", []) or []:
                            labels = {k: str(v) for k, v in _attrs(dp.get("attributes")).items()}
                            ts = int(_num(dp.get("timeUnixNano", 0)))
                            samples: list[tuple[str, dict[str, str], float]] = []
                            if kind == "histogram":
                                bounds = [_num(b) for b in dp.get("explicitBounds", []) or []]
                                counts = [_num(c) for c in dp.get("bucketCounts", []) or []]
                                cum = 0.0
                                for i, c in enumerate(counts):
                                    cum += c
                                    le = repr(bounds[i]) if i < len(bounds) else "+Inf"
                                    samples.append((name + "_bucket", {**labels, "le": le}, cum))
                                samples.append((name + "_count", labels, _num(dp.get("count", cum))))
                                samples.append((name + "_sum", labels, _num(dp.get("sum", 0))))
                            elif kind == "sum" and body.get("isMonotonic"):
                                total = name if name.endswith("_total") else name + "_total"
                                samples.append((total, labels, _point_value(dp)))
                            else:
                                samples.append((name, labels, _point_value(dp)))
                            key = (origin, name, _lkey(labels))
                            if key not in latest or ts >= latest[key][0]:
                                latest[key] = (ts, samples)
    out: dict[str, list[tuple[str, dict[str, str], float]]] = defaultdict(list)
    for (origin, _name, _labels), (_ts, samples) in sorted(latest.items()):
        out[origin].extend(samples)
    return dict(out)


def load_metrics(src: str | None) -> dict[str, list[tuple[str, dict[str, str], float]]]:
    """Return {source: samples}; ate_env_*, ate_env_fleet_* families only.

    A source is a Prometheus text file, or one process (resource) of an
    OTLP/JSON metrics file.
    """
    out = {}
    for path in _files(src, ("*.prom", "*.txt", "*metrics*", "*.jsonl")):
        if not os.path.isfile(path):
            continue
        with open(path, encoding="utf-8", errors="replace") as fh:
            text = fh.read()
        base = os.path.basename(path)
        if text.lstrip().startswith(("{", "[")):
            for origin, samples in parse_otlp_metrics(text).items():
                out[f"{base}:{origin}"] = samples
            continue
        samples = [s for s in parse_prom(text) if s[0].startswith(("ate_env_", "ate_env_fleet_"))]
        if samples:
            out[base] = samples
    return out


# ------------------------------------------------------------------------ aggregate


def _pct(xs: list[float], q: float) -> float | None:
    if not xs:
        return None
    xs = sorted(xs)
    k = (len(xs) - 1) * q
    lo, hi = math.floor(k), math.ceil(k)
    return xs[lo] + (xs[hi] - xs[lo]) * (k - lo)


def _stats(xs: list[float]) -> dict[str, Any]:
    return {"n": len(xs), "p50": _pct(xs, .5), "p90": _pct(xs, .9), "p99": _pct(xs, .99),
            "max": max(xs) if xs else None, "sum": sum(xs)}


def _short(name: str) -> str:
    return name.split("/")[-1] if "/" in name else name


def aggregate_reports(reports: list[dict[str, Any]]) -> dict[str, Any]:
    if not reports:
        return {"runs": [], "phases": []}
    phases: dict[str, dict[str, float]] = defaultdict(lambda: {"count": 0, "total_s": 0.0,
                                                              "max_s": 0.0})
    for r in reports:
        for name, p in (r.get("phases") or {}).items():
            agg = phases[name]
            agg["count"] += p.get("count", 0)
            agg["total_s"] += p.get("total_s", 0.0)
            agg["max_s"] = max(agg["max_s"], p.get("max_s", 0.0))
    order = [p for p in PHASE_ORDER if p in phases] + sorted(set(phases) - set(PHASE_ORDER))
    runs = [{k: r.get(k) for k in ("_file", "strategy", "total_s", "claims", "tasks_ok",
                                   "tasks_err", "warm_replicas_total", "warm_replicas_peak",
                                   "environment")} for r in reports]
    return {"runs": runs, "phases": [{"name": n, **phases[n],
                                      "mean_s": phases[n]["total_s"] / max(1, phases[n]["count"])}
                                     for n in order]}


def aggregate_spans(spans: list[dict[str, Any]]) -> dict[str, Any]:
    if not spans:
        return {"n": 0}
    by_trace: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for s in spans:
        by_trace[s["trace_id"]].append(s)
    ids = {(s["trace_id"], s["span_id"]) for s in spans}

    by_name: dict[tuple[str, str], list[float]] = defaultdict(list)
    errors: Counter = Counter()
    for s in spans:
        by_name[(s["service"], s["name"])].append(s["dur"])
        if s["error"]:
            errors[(s["service"], s["name"])] += 1
    latency = sorted(({"service": k[0], "name": k[1], "errors": errors[k], **_stats(v)}
                      for k, v in by_name.items()), key=lambda r: (-r["sum"]))

    services = sorted({s["service"] for s in spans})
    traces = []
    joined = Counter()
    orphans = 0
    for tid, ss in by_trace.items():
        ss.sort(key=lambda s: s["start"])
        roots = [s for s in ss if not s["parent_id"] or (tid, s["parent_id"]) not in ids]
        # A root whose parent id is set but missing means the parent was not exported.
        orphans += sum(1 for s in roots if s["parent_id"])
        svc = sorted({s["service"] for s in ss})
        for name in svc:
            joined[name] += 1
        t0 = ss[0]["start"]
        t1 = max(s["start"] + s["dur"] for s in ss)
        root = roots[0] if roots else ss[0]
        traces.append({"trace_id": tid, "root": root["name"], "root_service": root["service"],
                       "start": t0, "dur": t1 - t0, "n": len(ss), "services": svc,
                       "error": any(s["error"] for s in ss)})
    traces.sort(key=lambda t: t["start"])
    keep = sorted(traces, key=lambda t: (-len(t["services"]), -t["dur"]))[:MAX_TRACES]
    keep_ids = {t["trace_id"] for t in keep}
    waterfalls = {}
    for tid in keep_ids:
        ss = by_trace[tid][:MAX_SPANS_PER_TRACE]
        t0 = min(s["start"] for s in ss)
        depth: dict[str, int] = {}
        index = {s["span_id"]: s for s in ss}

        def d(s: dict[str, Any], guard: int = 0) -> int:
            if s["span_id"] in depth:
                return depth[s["span_id"]]
            p = index.get(s["parent_id"])
            depth[s["span_id"]] = 0 if p is None or guard > 64 else d(p, guard + 1) + 1
            return depth[s["span_id"]]

        rows = []
        for s in ss:
            rows.append({"name": s["name"], "service": s["service"], "o": s["start"] - t0,
                         "dur": s["dur"], "depth": d(s), "error": s["error"],
                         "id": s["span_id"], "parent": s["parent_id"],
                         "attrs": {k: v for k, v in s["attrs"].items()
                                   if not k.startswith(("rpc.grpc.", "net.", "server."))}})
        waterfalls[tid] = _tree_order(rows)

    multi = sum(1 for t in traces if len(t["services"]) > 1)
    start = min(s["start"] for s in spans)
    end = max(s["start"] + s["dur"] for s in spans)
    # Timeline: the caller's side (fleet phases, Python and Go client calls,
    # swebench-loader events), one lane per span name; server spans are in
    # the waterfalls.
    timeline = [{"service": s["service"], "name": s["name"], "o": s["start"] - start,
                 "dur": s["dur"], "error": s["error"]}
                for s in sorted(spans, key=lambda s: s["start"])
                if s.get("kind") != SPAN_KIND_SERVER and s["service"] not in SERVER_SERVICES][:5000]
    return {"n": len(spans), "n_traces": len(traces), "services": services,
            "multi_service_traces": multi, "orphan_roots": orphans,
            "traces_by_service": dict(joined), "window_s": end - start,
            "latency": latency, "traces": traces[:2000], "waterfalls": waterfalls,
            "timeline": timeline}


def _tree_order(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    kids: dict[str, list[dict[str, Any]]] = defaultdict(list)
    ids = {r["id"] for r in rows}
    roots = []
    for r in rows:
        if r["parent"] and r["parent"] in ids:
            kids[r["parent"]].append(r)
        else:
            roots.append(r)
    out: list[dict[str, Any]] = []

    def walk(r: dict[str, Any]) -> None:
        out.append(r)
        for k in sorted(kids[r["id"]], key=lambda x: x["o"]):
            walk(k)

    for r in sorted(roots, key=lambda x: x["o"]):
        walk(r)
    return out


def aggregate_metrics(scrapes: dict[str, list[tuple[str, dict[str, str], float]]]) -> dict:
    """Counters and histograms by family, summed over files (one scrape per process)."""
    counters: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    hists: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    gauges: dict[str, dict[str, float]] = defaultdict(dict)
    for _f, samples in scrapes.items():
        for name, labels, val in samples:
            if name.endswith("_bucket"):
                fam = name[:-7]
                key = _lkey({k: v for k, v in labels.items() if k != "le"})
                h = hists[fam].setdefault(key, {"buckets": defaultdict(float), "count": 0.0,
                                                "sum": 0.0})
                h["buckets"][labels.get("le", "+Inf")] += val
            elif name.endswith("_count") and not name.endswith("_total_count"):
                fam = name[:-6]
                h = hists[fam].setdefault(_lkey(labels), {"buckets": defaultdict(float),
                                                          "count": 0.0, "sum": 0.0})
                h["count"] += val
            elif name.endswith("_sum"):
                fam = name[:-4]
                h = hists[fam].setdefault(_lkey(labels), {"buckets": defaultdict(float),
                                                          "count": 0.0, "sum": 0.0})
                h["sum"] += val
            elif name.endswith("_total") or name.endswith("_created"):
                if name.endswith("_total"):
                    counters[name][_lkey(labels)] += val
            else:
                gauges[name][_lkey(labels)] = val
    hist_out = {}
    for fam, series in hists.items():
        rows = []
        for key, h in series.items():
            if not h["count"]:
                continue
            b = sorted(((float(le), c) for le, c in h["buckets"].items()), key=lambda x: x[0])
            rows.append({"labels": key, "count": h["count"], "mean": h["sum"] / h["count"],
                         "p50": _hq(b, .5), "p90": _hq(b, .9), "p99": _hq(b, .99)})
        if rows:
            hist_out[fam] = sorted(rows, key=lambda r: -r["count"])
    return {"files": sorted(scrapes), "counters": {k: dict(v) for k, v in counters.items()},
            "histograms": hist_out, "gauges": {k: dict(v) for k, v in gauges.items()}}


def _lkey(labels: dict[str, str]) -> str:
    return ",".join(f"{k}={v}" for k, v in sorted(labels.items())) or "-"


def _hq(buckets: list[tuple[float, float]], q: float) -> float | None:
    """Quantile from cumulative buckets, linear within the bucket (as PromQL)."""
    if not buckets:
        return None
    total = buckets[-1][1]
    if total <= 0:
        return None
    rank = q * total
    prev_le, prev_c = 0.0, 0.0
    for le, c in buckets:
        if c >= rank:
            if math.isinf(le):
                return prev_le
            if c == prev_c:
                return le
            return prev_le + (le - prev_le) * (rank - prev_c) / (c - prev_c)
        prev_le, prev_c = le, c
    return prev_le


def build(report_src: str | None, spans_src: str | None, metrics_src: str | None,
          title: str | None = None) -> dict[str, Any]:
    reports = load_reports(report_src)
    spans = load_spans(spans_src)
    metrics = load_metrics(metrics_src)
    if not (reports or spans or metrics):
        raise SystemExit("no inputs found: pass --report, --spans and/or --metrics")
    return {
        "meta": {"title": title or "ate_env run", "inputs": {
            "reports": len(reports), "spans": len(spans), "metric_files": len(metrics)}},
        "report": aggregate_reports(reports),
        "spans": aggregate_spans(spans),
        "metrics": aggregate_metrics(metrics),
    }


def render_html(data: dict[str, Any], template_path: str = TEMPLATE) -> str:
    with open(template_path, encoding="utf-8") as fh:
        tpl = fh.read()
    # "<" only occurs inside JSON strings, where \u003c is the same character;
    # escaping all of them keeps "</script>" and "<!--" out of the script.
    blob = json.dumps(data, separators=(",", ":"), default=str).replace("<", "\\u003c")
    title = re.sub(r"[<>&\"]", "", str(data["meta"].get("title") or "run"))
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    # The data goes in last, so placeholders inside span names or attributes
    # are never substituted.
    return (tpl.replace("__TITLE__", title).replace("__GENERATED__", generated)
               .replace("__RUN_JSON__", blob))


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--report", help="RunReport JSON file(s) or directory (REPORT_DIR)")
    p.add_argument("--spans", help="OTLP/JSON span file(s) or directory")
    p.add_argument("--metrics", help="Prometheus text scrape file(s) or directory")
    p.add_argument("--title", help="report title, e.g. the run id")
    p.add_argument("--out", required=True, help="output directory for run.json and report.html")
    args = p.parse_args(argv)
    data = build(args.report, args.spans, args.metrics, args.title)
    os.makedirs(args.out, exist_ok=True)
    with open(os.path.join(args.out, "run.json"), "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=1, default=str)
    with open(os.path.join(args.out, "report.html"), "w", encoding="utf-8") as fh:
        fh.write(render_html(data))
    s = data["spans"]
    print(json.dumps({"out": args.out, **data["meta"]["inputs"],
                      "traces": s.get("n_traces", 0), "services": s.get("services", []),
                      "multi_service_traces": s.get("multi_service_traces", 0)}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
