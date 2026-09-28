#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
cv-perf-driver: HugeGraph temporal performance gate driver.

Implements the measurement harness for gates #1-#6 of
docs/TEMPORAL_CLUSTER_VERIFICATION.md section 2 (frozen values):

  #1  normal   普通非时序查询 P95 回归  (fixed request set, 30 min per side)
  #2  asof     as_of P95/P99            (single entity, 1000 intervals, 10k iters)
  #3  between  between/overlap P95/P99  (single entity 1000 / multi entity 100x100)
  #4  append   >=1000 mutation/s, 0 errors (16 threads, 10 min, 1KB, seq/rand/hot)
  #5  scan     scanned_rows <= returned x 10 (REST response header)
  #6  consist  当前/历史 0 不一致样本 (expected interval set vs queried set)

Pure Python 3 standard library; one keep-alive connection per thread.
Run inside the cluster node (stg-121) against http://10.129.33.121:8080 so
client network noise stays out of the latency numbers.

Examples:
  python3 cv_perf_driver.py probe
  python3 cv_perf_driver.py normal --duration 1800 --concurrency 8 --tag a-old
  python3 cv_perf_driver.py prepare --label perf-asof --entity-prefix perf-asof \
      --entities 1 --intervals 1000
  python3 cv_perf_driver.py prepare --label perf-multi --entity-prefix perf-multi \
      --entities 100 --intervals 100
  python3 cv_perf_driver.py asof --label perf-asof --entity perf-asof-1 --iters 10000
  python3 cv_perf_driver.py between --label perf-asof --entity perf-asof-1 \
      --multi-label perf-multi --multi-prefix perf-multi --multi-entities 100 --iters 1500
  python3 cv_perf_driver.py append --duration 600 --threads 16 --tag b-new
  python3 cv_perf_driver.py scan --label perf-asof --entity perf-asof-1
  python3 cv_perf_driver.py consist --label perf-asof --entity-prefix perf-asof \
      --entities 1 --intervals 1000
"""

import argparse
import array
import gzip
import http.client
import json
import os
import random
import statistics
import sys
import threading
import time
import urllib.parse
import uuid
from collections import namedtuple

DEFAULT_BASE = "http://10.129.33.121:8080/graphspaces/DEFAULT/graphs/hugegraph"

# 2020-01-01T00:00:00Z epoch millis, the temporal data start used by prepare.
DEFAULT_START_MS = 1577836800000
DAY_MS = 86400000

# 普通查询请求集（门槛 #1）。每项给出候选路径，preflight 选第一个 200 的。
NORMAL_REQS = [
    ("vertices_limit10", ["/graph/vertices?limit=10"]),
    ("vertex_by_id", ['/graph/vertices/%221166833192691302400%22',
                      '/graph/vertices/"1166833192691302400"',
                      '/graph/vertices/1166833192691302400']),
    ("vertices_label_person2", ["/graph/vertices?label=person2&limit=10"]),
    ("edges_limit10", ["/graph/edges?limit=10"]),
    ("vertices_limit100", ["/graph/vertices?limit=100"]),
]

Resp = namedtuple("Resp", "status headers body ms reconnected")


def jbody(b):
    try:
        return json.loads(b.decode("utf-8"))
    except Exception:
        return None


def err_text(resp):
    obj = jbody(resp.body)
    if isinstance(obj, dict):
        return ("%s: %s" % (obj.get("exception", ""),
                            obj.get("message", "")))[:300]
    return resp.body[:200].decode("utf-8", "replace")


class Api:
    """Keep-alive HTTP client; create one instance per thread."""

    def __init__(self, base_url, timeout=60.0):
        if not base_url.startswith("http://"):
            raise ValueError("only http:// base urls are supported")
        rest = base_url[len("http://"):]
        slash = rest.find("/")
        netloc = rest if slash < 0 else rest[:slash]
        self.base_path = "" if slash < 0 else rest[slash:]
        if ":" in netloc:
            host, port = netloc.split(":", 1)
            self.port = int(port)
        else:
            host, self.port = netloc, 80
        self.host = host
        self.timeout = timeout
        self._conn = None

    def close(self):
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    def request(self, method, path, body=None):
        data = None
        headers = {"Accept": "application/json", "Accept-Encoding": "gzip"}
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        last = None
        for attempt in (0, 1):  # one transparent reconnect on stale keep-alive
            try:
                if self._conn is None:
                    self._conn = http.client.HTTPConnection(
                        self.host, self.port, timeout=self.timeout)
                t0 = time.perf_counter()
                self._conn.request(method, self.base_path + path,
                                   body=data, headers=headers)
                resp = self._conn.getresponse()
                raw = resp.read()
                ms = (time.perf_counter() - t0) * 1000.0
                enc = (resp.getheader("Content-Encoding") or "").lower()
                if "gzip" in enc and len(raw) >= 2 and raw[0] == 0x1F and raw[1] == 0x8B:
                    raw = gzip.decompress(raw)
                hdrs = {k.lower(): v for k, v in resp.getheaders()}
                return Resp(resp.status, hdrs, raw, ms, attempt > 0)
            except Exception as e:  # connection reset / stale socket / timeout
                last = e
                self.close()
        raise last

    def get(self, path):
        return self.request("GET", path)

    def post(self, path, obj):
        return self.request("POST", path, body=obj)


# ---------------------------------------------------------------- helpers

def t_query_path(label, qtype, frm, to=None, fact_key=None,
                 limit=None, page_token=None):
    q = [("temporal_label", label), ("type", qtype), ("from", str(frm))]
    if to is not None:
        q.append(("to", str(to)))
    if fact_key is not None:
        q.append(("fact_key", json.dumps(fact_key, separators=(",", ":"),
                                         sort_keys=False)))
    if limit:
        q.append(("limit", str(limit)))
    if page_token:
        q.append(("page_token", page_token))
    return "/intervals?" + urllib.parse.urlencode(q)


def t_mutation(label, entity, fact_key, vfrom, vto, payload, mutation_id,
               op="append"):
    m = {"entity_id": entity, "temporal_label": label, "fact_key": fact_key,
         "valid_from": vfrom, "mutation_id": mutation_id, "operation": op}
    if vto is not None:
        m["valid_to"] = vto
    if payload is not None:
        m["payload"] = payload
    return m


def fact_for(entity):
    """One fact dimension per entity.

    Verified in code (2026-09-28): the Store locates an interval sequence by
    the fact-key canonical bytes alone -- TemporalMutationHandler.apply() uses
    PartitionUtils.calcHashcode(bundle.factKey()) for the partition and
    TemporalIntervalCodec.intervalKey(bundle) = factKey||version||bucket||
    from||to for the marker key; neither entity_id nor temporal_label enters
    any key, and the REST query carries no entity parameter at all.
    Distinct entity sequences therefore REQUIRE distinct fact-key values.
    """
    return {"site": entity}


def pct(sorted_xs, p):
    if not sorted_xs:
        return None
    k = (len(sorted_xs) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(sorted_xs) - 1)
    return sorted_xs[f] + (sorted_xs[c] - sorted_xs[f]) * (k - f)


def ci95(vals):
    n = len(vals)
    if n < 2:
        return (None, None)
    m = statistics.mean(vals)
    se = statistics.stdev(vals) / (n ** 0.5)
    return (m - 1.96 * se, m + 1.96 * se)


def summarize(xs):
    s = sorted(xs)
    return {
        "count": len(s),
        "mean": (statistics.mean(s) if s else None),
        "p50": pct(s, 50),
        "p95": pct(s, 95),
        "p99": pct(s, 99),
        "max": (s[-1] if s else None),
    }


def emit(mode, tag, summary, extra=None, out_dir="results"):
    os.makedirs(out_dir, exist_ok=True)
    ts = time.strftime("%Y%m%d-%H%M%S")
    rec = {"mode": mode, "tag": tag, "finished": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
           "argv": sys.argv, "summary": summary, "extra": extra or {}}
    safe = "".join(c if c.isalnum() or c in "-_." else "_" for c in tag)
    path = os.path.join(out_dir, "%s-%s-%s.json" % (mode, safe, ts))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rec, f, indent=2, sort_keys=True,
                  default=lambda o: None if o is None else str(o))
    print("[emit] %s" % path)
    return path


# ---------------------------------------------------------------- probe

def run_probe(api, args):
    ok = True
    print("== 普通查询请求集探活 ==")
    for name, cands in NORMAL_REQS:
        line = None
        found = None
        for c in cands:
            r = api.get(c)
            if r.status == 200:
                found = c
                line = "  %-24s 200  %7.1fms  %6dB  %s" % (name, r.ms, len(r.body), c)
                break
            line = "  %-24s %d  %s (%s)" % (name, r.status, c, err_text(r))
        print(line)
        if found is None:
            ok = False
    print("== temporal 闭环探活 ==")
    label = "perf-probe"
    entity = "perf-probe-1"
    fact = {"k": "probe-" + uuid.uuid4().hex[:8]}
    t0 = DEFAULT_START_MS
    ids = []
    for i in range(2):
        mid = "probe-%s-%d" % (uuid.uuid4().hex[:8], i)
        ids.append(mid)
        m = t_mutation(label, entity, fact, t0 + i * 1000, t0 + i * 1000 + 999,
                       "probe-payload-%d" % i, mid)
        r = api.post("/intervals", m)
        print("  append[%d]  %d  %7.1fms  %s" % (i, r.status, r.ms,
                                                err_text(r) if r.status != 200 else "ok"))
        ok = ok and r.status == 200
        time.sleep(0.2)
    r = api.get(t_query_path(label, "as_of", t0 + 1500, fact_key=fact))
    n = len(jbody(r.body) or []) if r.status == 200 else -1
    print("  as_of      %d  %7.1fms  hits=%s scanned=%s (expect 1)" %
          (r.status, r.ms, n, r.headers.get("scanned_rows")))
    ok = ok and r.status == 200 and n == 1
    r = api.get(t_query_path(label, "overlap", t0, t0 + 2000, fact_key=fact))
    n = len(jbody(r.body) or []) if r.status == 200 else -1
    print("  overlap    %d  %7.1fms  hits=%s scanned=%s (expect 2)" %
          (r.status, r.ms, n, r.headers.get("scanned_rows")))
    ok = ok and r.status == 200 and n == 2
    summary = {"ok": ok}
    emit("probe", args.tag, summary, out_dir=args.out_dir)
    print("== probe %s ==" % ("PASS" if ok else "FAIL"))
    return 0 if ok else 1


# ---------------------------------------------------------------- normal (#1)

def _normal_preflight(api):
    eff = []
    for name, cands in NORMAL_REQS:
        for c in cands:
            r = api.get(c)
            if r.status == 200:
                eff.append((name, c))
                break
        else:
            raise SystemExit("preflight failed for %s: all candidates non-200" % name)
    return eff


def _normal_worker(api, stop_epoch, measure_start, window_secs, reqs, out, wid):
    lat = array.array("f")
    win = array.array("i")
    ridx = array.array("b")
    stat = array.array("h")
    rng = random.Random(threading.get_ident() ^ int(time.time() * 1e6))
    n = len(reqs)
    i = rng.randrange(n)
    while True:
        now = time.time()
        if now >= stop_epoch:
            break
        _, path = reqs[i % n]
        i += 1
        try:
            r = api.request("GET", path)
            st = r.status
        except Exception:
            st = -1
            r = None
        t = time.time()
        if t >= measure_start:
            lat.append(r.ms if r is not None else -1.0)
            win.append(int((t - measure_start) // window_secs))
            ridx.append((i - 1) % n)
            stat.append(st if -32768 <= st <= 32767 else -2)
    out[wid] = (lat, win, ridx, stat, api)


def run_normal(api_factory, args):
    api0 = api_factory()
    reqs = _normal_preflight(api0)
    print("effective request set:")
    for name, path in reqs:
        print("  %-24s %s" % (name, path))
    conc = args.concurrency
    warm = args.warmup
    dur = args.duration
    window = args.window
    measure_start = time.time() + warm
    stop_epoch = measure_start + dur
    out = [None] * conc
    threads = []
    for w in range(conc):
        a = api_factory()
        t = threading.Thread(target=_normal_worker,
                             args=(a, stop_epoch, measure_start, window, reqs, out, w))
        t.start()
        threads.append(t)
    print("warmup %ds then measure %ds with %d concurrent workers ..." %
          (warm, dur, conc), flush=True)
    for t in threads:
        t.join()
    lat = array.array("f")
    win = array.array("i")
    ridx = array.array("b")
    stat = array.array("h")
    for w in range(conc):
        l, n_, r_, s_, _ = out[w]
        lat.extend(l)
        win.extend(n_)
        ridx.extend(r_)
        stat.extend(s_)
    for w in range(conc):
        out[w][4].close()

    total = len(lat)
    errs = sum(1 for s in stat if s != 200)
    err_detail = {}
    for s in stat:
        if s != 200:
            err_detail[str(s)] = err_detail.get(str(s), 0) + 1

    per_req = {}
    windows = {}
    ok_lat = array.array("f")
    for i in range(total):
        l = lat[i]
        if l < 0:
            continue  # failed request: counted in errors, not in latency
        ok_lat.append(l)
        w = win[i]
        if w not in windows:
            windows[w] = array.array("f")
        windows[w].append(l)
        rname = reqs[ridx[i]][0]
        if rname not in per_req:
            per_req[rname] = array.array("f")
        per_req[rname].append(l)

    global_sum = summarize(ok_lat)
    per_req_sum = {k: summarize(v) for k, v in sorted(per_req.items())}
    win_list = []
    win_p95s = []
    for w in sorted(windows):
        s = summarize(windows[w])
        s["window"] = w
        s["t_offset_s"] = w * window
        win_list.append(s)
        win_p95s.append(s["p95"])
    lo, hi = ci95(win_p95s)

    summary = {
        "requests": total,
        "errors": errs,
        "duration_s": dur,
        "concurrency": conc,
        "rps": (total / dur if dur else None),
        "global": global_sum,
        "per_request": per_req_sum,
        "window_p95_mean": (statistics.mean(win_p95s) if win_p95s else None),
        "window_p95_ci95": [lo, hi],
        "error_detail": err_detail,
    }
    print(json.dumps(summary, indent=2, default=str))
    emit("normal", args.tag, summary,
         extra={"windows": win_list, "request_set": [list(r) for r in reqs]},
         out_dir=args.out_dir)
    return 0


# ---------------------------------------------------------------- prepare

def _prepare_worker(args, api, entity, todo, results, lock):
    ok = 0
    err = 0
    errs = []
    step = args.step_ms
    first = args.start_ms
    for i in todo:
        vfrom = first + i * step
        vto = vfrom + step - 1
        mid = "perf-%s-%s-%d" % (args.label, entity, i)
        m = t_mutation(args.label, entity, fact_for(entity), vfrom, vto,
                       args.payload if args.payload else None, mid)
        try:
            r = api.post("/intervals", m)
            if r.status == 200:
                ok += 1
            else:
                err += 1
                if len(errs) < 5:
                    errs.append("i=%d %d %s" % (i, r.status, err_text(r)))
        except Exception as e:
            err += 1
            if len(errs) < 5:
                errs.append("i=%d EXC %s" % (i, e))
    with lock:
        results["ok"] += ok
        results["err"] += err
        results["errs"].extend(errs)


def _chunks(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def run_prepare(args):
    entities = ["%s-%d" % (args.entity_prefix, i + 1) for i in range(args.entities)]
    per_entity = list(range(args.intervals))
    results = {"ok": 0, "err": 0, "errs": []}
    lock = threading.Lock()
    t0 = time.time()
    writers = max(1, args.writers)
    threads = []
    jobs = []  # (api, entity, todo_indices, slot)
    rr = 0
    for entity in entities:
        for chunk in _chunks(per_entity, max(1, args.intervals // 10)):
            slot = rr % writers
            rr += 1
            jobs.append((entity, chunk, slot))
    apis = [Api(args.base_url) for _ in range(writers)]
    q = list(jobs)
    qlock = threading.Lock()

    def pump(slot):
        api = apis[slot]
        while True:
            with qlock:
                if not q:
                    return
                entity, chunk, _ = q.pop()
            _prepare_worker(args, api, entity, chunk, results, lock)

    for s in range(writers):
        t = threading.Thread(target=pump, args=(s,))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    for a in apis:
        a.close()
    dt = time.time() - t0

    # verify counts per entity
    vapi = Api(args.base_url)
    end = args.start_ms + args.intervals * args.step_ms + 10
    verify = []
    vok = True
    for entity in entities:
        cnt = _count_intervals(vapi, args.label, entity, fact_for(entity),
                               args.start_ms, end)
        verify.append({"entity": entity, "expected": args.intervals, "got": cnt})
        vok = vok and cnt == args.intervals
    vapi.close()

    summary = {"entities": args.entities, "intervals_per": args.intervals,
               "wrote_ok": results["ok"], "wrote_err": results["err"],
               "seconds": dt, "mps": results["ok"] / dt if dt else None,
               "verify_ok": vok, "error_samples": results["errs"][:10]}
    print(json.dumps(summary, indent=2, default=str))
    emit("prepare", args.tag, summary, extra={"verify": verify[:200]},
         out_dir=args.out_dir)
    return 0 if results["err"] == 0 and vok else 1


def _count_intervals(api, label, entity, fact, frm, to, limit=None):
    """Count intervals via overlap pagination (returns -1 on any failure)."""
    total = 0
    token = None
    for _ in range(10000):
        r = api.get(t_query_path(label, "overlap", frm, to, fact_key=fact,
                                 limit=limit, page_token=token))
        if r.status != 200:
            return -1
        rows = jbody(r.body) or []
        total += len(rows)
        token = r.headers.get("next_page_token")
        if not token:
            return total
    return -1


# ---------------------------------------------------------------- asof (#2)

def _iter_latencies(api, path_fn, iters, warmup, out, wid, stop_on_error=True):
    lat = array.array("f")
    for k in range(warmup):
        r = api.get(path_fn())
        if stop_on_error and r.status != 200:
            raise SystemExit("warmup request failed: %d %s" % (r.status, err_text(r)))
    t0 = time.time()
    for k in range(iters):
        r = api.get(path_fn())
        if r.status != 200:
            raise SystemExit("measure request %d failed: %d %s" %
                             (k, r.status, err_text(r)))
        lat.append(r.ms)
    out[wid] = (lat, t0, time.time())


def run_asof(args):
    fact = fact_for(args.entity)
    as_of = args.as_of_ms or (args.start_ms + (args.intervals // 2) * args.step_ms + 10)
    path_fn = lambda: t_query_path(args.label, "as_of", as_of, fact_key=fact)
    conc = args.concurrency
    out = [None] * conc
    threads = []
    iters_each = args.iters // conc
    for w in range(conc):
        a = Api(args.base_url)
        t = threading.Thread(target=_iter_latencies,
                             args=(a, path_fn, iters_each, args.warmup, out, w))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    lat = array.array("f")
    for w in range(conc):
        lat.extend(out[w][0])
    summary = summarize(lat)
    summary["entity"] = args.entity
    summary["as_of_ms"] = as_of
    summary["iters"] = len(lat)
    print(json.dumps(summary, indent=2, default=str))
    emit("asof", args.tag, summary, out_dir=args.out_dir)
    return 0


# ---------------------------------------------------------------- between (#3)

def run_between(args):
    step = args.step_ms
    results = {}

    def one(name, label, entity, frm, to, qtype, iters, warmup):
        fact = fact_for(entity)
        fn = lambda: t_query_path(label, qtype, frm, to, fact_key=fact)
        tl = []
        rl = []
        sl = []
        api = Api(args.base_url)
        # warmup + verify shape on the first real request
        r0 = api.get(fn())
        if r0.status != 200:
            raise SystemExit("%s warmup failed: %d %s" % (name, r0.status, err_text(r0)))
        hits0 = len(jbody(r0.body) or [])
        for k in range(warmup):
            r = api.get(fn())
            if r.status != 200:
                raise SystemExit("%s warmup failed: %d %s" % (name, r.status, err_text(r)))
        t0 = time.time()
        for k in range(iters):
            r = api.get(fn())
            if r.status != 200:
                raise SystemExit("%s iter %d failed: %d %s" %
                                 (name, k, r.status, err_text(r)))
            tl.append(r.ms)
            rl.append(int(r.headers.get("scanned_rows", -1)))
            sl.append(len(jbody(r.body) or []))
        dt = time.time() - t0
        api.close()
        sm = summarize(tl)
        sm.update({"hits_first": hits0, "iters": len(tl), "seconds": dt,
                   "scanned_max": max(rl) if rl else None,
                   "returned_min": min(sl) if sl else None,
                   "returned_max": max(sl) if sl else None})
        results[name] = sm
        print("[%s] %s" % (name, json.dumps(sm, default=str)))

    # scenario A: single entity, 1000 intervals; a 200-interval window
    if args.entity:
        frm = args.start_ms + (args.intervals // 5) * step
        to = frm + 200 * step
        one("single_between", args.label, args.entity, frm, to, "between",
            args.iters, args.warmup)
        one("single_overlap", args.label, args.entity, frm, to, "overlap",
            args.iters, args.warmup)
    # scenario B: multi entity (100 x 100); narrow window ~50 intervals
    if args.multi_prefix:
        rng = random.Random(20260928)
        ents = ["%s-%d" % (args.multi_prefix, i + 1)
                for i in range(args.multi_entities)]
        frm = args.start_ms + 40 * step
        to = frm + 50 * step

        def multi_fn():
            # t_query_path has no entity parameter (queries are fact-scoped;
            # see TEMPORAL_CLUSTER_VERIFICATION.md 10.7). Entity isolation is
            # carried by the fact key alone, so pass qtype positionally.
            ent = rng.choice(ents)
            return t_query_path(args.multi_label, "between", frm, to,
                                fact_key=fact_for(ent))
        tl = []
        rl = []
        sl = []
        api = Api(args.base_url)
        for k in range(args.warmup):
            r = api.get(multi_fn())
            if r.status != 200:
                raise SystemExit("multi warmup failed: %d %s" % (r.status, err_text(r)))
        t0 = time.time()
        for k in range(args.iters):
            r = api.get(multi_fn())
            if r.status != 200:
                raise SystemExit("multi iter %d failed: %d %s" %
                                 (k, r.status, err_text(r)))
            tl.append(r.ms)
            rl.append(int(r.headers.get("scanned_rows", -1)))
            sl.append(len(jbody(r.body) or []))
        dt = time.time() - t0
        api.close()
        sm = summarize(tl)
        sm.update({"iters": len(tl), "seconds": dt,
                   "scanned_max": max(rl) if rl else None,
                   "returned_min": min(sl) if sl else None,
                   "returned_max": max(sl) if sl else None})
        results["multi_between"] = sm
        print("[multi_between] %s" % json.dumps(sm, default=str))

    emit("between", args.tag, {}, extra={"scenarios": results},
         out_dir=args.out_dir)
    return 0


# ---------------------------------------------------------------- append (#4)

class AppendStats:
    def __init__(self):
        self.lock = threading.Lock()
        self.per_role = {r: {"ok": 0, "err": 0, "lat": array.array("f")}
                         for r in ("seq", "rand", "hot")}
        self.errors = []
        self.err_count = 0

    def record(self, role, ms, status, text):
        with self.lock:
            d = self.per_role[role]
            if status == 200:
                d["ok"] += 1
                d["lat"].append(ms)
            else:
                d["err"] += 1
                self.err_count += 1
                if len(self.errors) < 20:
                    self.errors.append("%s %d %s" % (role, status, text))


def _append_worker(cfg, tid, stop_epoch, stats, out):
    api = Api(cfg.base_url)
    rng = random.Random(0x9E3779B9 ^ tid)
    base = 1735689600000  # 2025-01-01T00:00:00Z
    run = cfg.run_id
    payload = "x" * cfg.payload_kb * 1024
    seq_i = 0
    hot_i = 0
    rand_block = 0
    rand_pool = []

    def do(role, entity, fact, vfrom, vto, mid):
        m = t_mutation(cfg.label, entity, fact, vfrom, vto, payload, mid)
        try:
            r = api.post("/intervals", m)
            stats.record(role, r.ms, r.status,
                         err_text(r) if r.status != 200 else "")
        except Exception as e:
            stats.record(role, -1.0, -1, "EXC %s" % e)

    # NOTE (2026-09-28): every fact key below carries the run id. The Store
    # scopes an interval sequence by the fact-key bytes alone (see fact_for),
    # and the valid_from progression restarts from `base` each run, so WITHOUT
    # a run dimension a second append run replays the same valid_from onto the
    # intervals the first run already committed and every overlap is rejected
    # as TEMPORAL_CONFLICT -- the run then measures conflict-rejection rate,
    # not clean append throughput. Run-scoping the fact key gives each run a
    # fresh timeline so repeated #4 measurements are comparable.
    while time.time() < stop_epoch:
        u = rng.random()
        if u < 0.5:
            role = "seq"
            entity = "%s-seq%d" % (cfg.prefix, tid)
            i = seq_i
            seq_i += 1
            fact = {"thread": str(tid), "mode": "seq", "run": run}
            vfrom = base + i * 120000
            do(role, entity, fact, vfrom, vfrom + 119999,
               "%s-%d-seq-%d" % (run, tid, i))
        elif u < 0.8:
            role = "rand"
            entity = "%s-rand%d" % (cfg.prefix, tid)
            fact = {"thread": str(tid), "mode": "rand", "run": run}
            if not rand_pool:
                block = list(range(1000))
                rng.shuffle(block)
                rand_pool = block
                rand_block += 1
            slot = (rand_block - 1) * 1000 + rand_pool.pop()
            vfrom = base + slot * 120000
            do(role, entity, fact, vfrom, vfrom + 119999,
               "%s-%d-rand-%d" % (run, tid, slot))
        else:
            role = "hot"
            entity = "%s-hot" % cfg.prefix
            fact = {"mode": "hot", "run": run}
            i = hot_i
            hot_i += 1
            slot = tid * 1000000 + i
            vfrom = base + slot * 60000
            do(role, entity, fact, vfrom, vfrom + 59999,
               "%s-%d-hot-%d" % (run, tid, i))
    api.close()
    out[tid] = True


def run_append(args):
    cfg = argparse.Namespace(base_url=args.base_url, label=args.label,
                             prefix=args.prefix, payload_kb=args.payload_kb,
                             run_id=args.run_id or ("run" + uuid.uuid4().hex[:8]))
    stats = AppendStats()
    out = [False] * args.threads
    t0 = time.time()
    stop = t0 + args.duration
    threads = []
    for tid in range(args.threads):
        t = threading.Thread(target=_append_worker,
                             args=(cfg, tid, stop, stats, out))
        t.start()
        threads.append(t)
    print("append: %d threads, %ds steady state, label=%s prefix=%s run=%s ..." %
          (args.threads, args.duration, cfg.label, cfg.prefix, cfg.run_id),
          flush=True)
    for t in threads:
        t.join()
    dt = time.time() - t0
    per_role = {}
    total_ok = 0
    total_err = 0
    for role, d in stats.per_role.items():
        s = summarize(d["lat"])
        s["ok"] = d["ok"]
        s["err"] = d["err"]
        s["rps"] = d["ok"] / dt if dt else None
        per_role[role] = s
        total_ok += d["ok"]
        total_err += d["err"]
    summary = {
        "threads": args.threads,
        "duration_s": dt,
        "total_ok": total_ok,
        "total_err": total_err,
        "mps": total_ok / dt if dt else None,
        "error_rate": (total_err / (total_ok + total_err) if (total_ok + total_err) else 0),
        "per_role": per_role,
        "error_samples": stats.errors,
    }
    print(json.dumps(summary, indent=2, default=str))
    emit("append", args.tag, summary, out_dir=args.out_dir)
    return 0 if total_err == 0 else 1


# ---------------------------------------------------------------- scan (#5)

def run_scan(args):
    fact = fact_for(args.entity)
    step = args.step_ms
    label = args.label
    entity = args.entity
    api = Api(args.base_url)
    samples = []
    verdict_ok = True

    def note(name, r, hits):
        nonlocal verdict_ok
        scanned = int(r.headers.get("scanned_rows", -1))
        amp = (scanned / hits if hits else None)
        good = (hits == 0 and scanned <= 1) or (hits > 0 and scanned <= hits * 10)
        verdict_ok = verdict_ok and good
        samples.append({"name": name, "scanned": scanned, "returned": hits,
                        "amp": amp, "pass": good, "ms": r.ms})

    # A: as_of hit 1 of 1000, x100
    as_of = args.start_ms + (args.intervals // 2) * step + 10
    for k in range(args.repeats):
        r = api.get(t_query_path(label, "as_of", as_of, fact_key=fact))
        hits = len(jbody(r.body) or [])
        note("asof_1of1000", r, hits)
    # B: wide window all 1000, limit=1000
    end = args.start_ms + args.intervals * step + 10
    for k in range(max(1, args.repeats // 10)):
        r = api.get(t_query_path(label, "overlap", args.start_ms, end,
                                 fact_key=fact, limit=2000))
        hits = len(jbody(r.body) or [])
        note("wide_all1000", r, hits)
    # C: window ~10 intervals
    frm = args.start_ms + 100 * step
    to = frm + 10 * step
    for k in range(args.repeats):
        r = api.get(t_query_path(label, "overlap", frm, to, fact_key=fact))
        hits = len(jbody(r.body) or [])
        note("narrow_10", r, hits)
    # D: pagination limit=50 over the whole range, first 30 pages
    token = None
    for page in range(args.pages):
        r = api.get(t_query_path(label, "overlap", args.start_ms, end,
                                 fact_key=fact, limit=50, page_token=token))
        if r.status != 200:
            note("paged_status_%d" % r.status, r, -1)
            break
        hits = len(jbody(r.body) or [])
        note("paged", r, hits)
        token = r.headers.get("next_page_token")
        if not token:
            break
    api.close()
    amps = [s["amp"] for s in samples if s["amp"] is not None]
    summary = {
        "samples": len(samples),
        "fail_samples": [s for s in samples if not s["pass"]][:20],
        "amp_max": (max(amps) if amps else None),
        "amp_mean": (statistics.mean(amps) if amps else None),
        "scanned_max": max(s["scanned"] for s in samples) if samples else None,
        "passed": verdict_ok,
        "detail_head": samples[:20],
    }
    print(json.dumps(summary, indent=2, default=str))
    emit("scan", args.tag, summary, extra={"all": samples}, out_dir=args.out_dir)
    return 0 if verdict_ok else 1


# ---------------------------------------------------------------- consist (#6)

def run_consist(args):
    step = args.step_ms
    api = Api(args.base_url)
    end = args.start_ms + args.intervals * step + 10
    bad = 0
    details = []
    for e in range(1, args.entities + 1):
        entity = "%s-%d" % (args.entity_prefix, e)
        fact = fact_for(entity)
        expected = set((args.start_ms + i * step,
                        args.start_ms + i * step + step - 1)
                       for i in range(args.intervals))
        rows = []
        token = None
        for _ in range(10000):
            r = api.get(t_query_path(args.label, "overlap", args.start_ms, end,
                                     fact_key=fact, limit=1000, page_token=token))
            if r.status != 200:
                raise SystemExit("consist query failed: %d %s" % (r.status, err_text(r)))
            rows.extend(jbody(r.body) or [])
            token = r.headers.get("next_page_token")
            if not token:
                break
        actual = set((r0["valid_from"], r0["valid_to"]) for r0 in rows
                     if r0.get("valid_to") is not None)
        rev_bad = [r0 for r0 in rows if not r0.get("committed_revision")]
        missing = expected - actual
        extra_ = actual - expected
        if missing or extra_ or rev_bad:
            bad += 1
        details.append({"entity": entity, "expected": len(expected),
                        "actual": len(actual), "missing": len(missing),
                        "extra": len(extra_), "rows_without_revision": len(rev_bad)})
        print("  %-20s expected=%d actual=%d missing=%d extra=%d rev_bad=%d" %
              (entity, len(expected), len(actual), len(missing), len(extra_),
               len(rev_bad)))
    api.close()
    summary = {"entities": args.entities, "mismatched_entities": bad,
               "passed": bad == 0}
    print(json.dumps(summary, indent=2, default=str))
    emit("consist", args.tag, summary, extra={"entities": details},
         out_dir=args.out_dir)
    return 0 if bad == 0 else 1


# ---------------------------------------------------------------- main

def main(argv=None):
    p = argparse.ArgumentParser(description="cv-perf gate driver")
    p.add_argument("--base-url", default=DEFAULT_BASE)
    p.add_argument("--out-dir", default="results")
    sub = p.add_subparsers(dest="mode", required=True)

    sp = sub.add_parser("probe")
    sp.add_argument("--tag", default="probe")

    sp = sub.add_parser("normal")
    sp.add_argument("--duration", type=int, default=1800)
    sp.add_argument("--concurrency", type=int, default=8)
    sp.add_argument("--warmup", type=int, default=30)
    sp.add_argument("--window", type=int, default=60)
    sp.add_argument("--tag", default="normal")

    def add_data_args(sp, entity=False):
        sp.add_argument("--label", default="perf-asof")
        sp.add_argument("--entity-prefix", default="perf-asof")
        sp.add_argument("--entities", type=int, default=1)
        sp.add_argument("--intervals", type=int, default=1000)
        sp.add_argument("--start-ms", type=int, default=DEFAULT_START_MS)
        sp.add_argument("--step-ms", type=int, default=DAY_MS)
        if entity:
            sp.add_argument("--entity", default="perf-asof-1")
        sp.add_argument("--tag", default="")

    sp = sub.add_parser("prepare")
    add_data_args(sp)
    sp.add_argument("--writers", type=int, default=4)
    sp.add_argument("--payload", default="")

    sp = sub.add_parser("asof")
    add_data_args(sp, entity=True)
    sp.add_argument("--iters", type=int, default=10000)
    sp.add_argument("--warmup", type=int, default=1000)
    sp.add_argument("--concurrency", type=int, default=1)
    sp.add_argument("--as-of-ms", type=int, default=0)

    sp = sub.add_parser("between")
    add_data_args(sp, entity=True)
    sp.add_argument("--iters", type=int, default=1500)
    sp.add_argument("--warmup", type=int, default=200)
    sp.add_argument("--multi-label", default="")
    sp.add_argument("--multi-prefix", default="")
    sp.add_argument("--multi-entities", type=int, default=100)

    sp = sub.add_parser("append")
    sp.add_argument("--label", default="perf-app")
    sp.add_argument("--prefix", default="perf-app")
    sp.add_argument("--threads", type=int, default=16)
    sp.add_argument("--duration", type=int, default=600)
    sp.add_argument("--payload-kb", type=int, default=1)
    sp.add_argument("--run-id", default="")
    sp.add_argument("--tag", default="append")

    sp = sub.add_parser("scan")
    add_data_args(sp, entity=True)
    sp.add_argument("--repeats", type=int, default=50)
    sp.add_argument("--pages", type=int, default=30)

    sp = sub.add_parser("consist")
    add_data_args(sp)

    args = p.parse_args(argv)
    if not args.tag:
        args.tag = args.mode

    if args.mode == "probe":
        return run_probe(Api(args.base_url), args)
    if args.mode == "normal":
        return run_normal(lambda: Api(args.base_url), args)
    if args.mode == "prepare":
        return run_prepare(args)
    if args.mode == "asof":
        return run_asof(args)
    if args.mode == "between":
        return run_between(args)
    if args.mode == "append":
        return run_append(args)
    if args.mode == "scan":
        return run_scan(args)
    if args.mode == "consist":
        return run_consist(args)
    raise SystemExit("unknown mode %s" % args.mode)


if __name__ == "__main__":
    sys.exit(main())
