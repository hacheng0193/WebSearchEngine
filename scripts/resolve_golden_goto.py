"""Resolve `https://www.google.com/goto?url=...` golden URLs and recompute coverage.

From batch 21 (2026-11-01) on, SerpApi returns Google redirect links for most
organic results at rank 4-10. The crawler never stores those, so they count as
uncovered. Each goto link answers with a 302 whose Location is the real page.

Read-only against every database: results live in a local SQLite file.

    python -m scripts.resolve_golden_goto export   --batch 21 --state goto.sqlite
    python -m scripts.resolve_golden_goto resolve  --state goto.sqlite --interval 5
    python -m scripts.resolve_golden_goto status   --state goto.sqlite
    python -m scripts.resolve_golden_goto coverage --state goto.sqlite \
        --crawler_db_url 172.16.191.3:5433 --select_db_url 172.16.191.1:5445
"""
import argparse
import json
import sqlite3
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from urllib.parse import urlsplit

import psycopg2
import tldextract

from Metric.Measure.CrawlerAllMetricMeasure import (
    apply_found_urls,
    build_lookup_map,
    normalize_golden_url,
)

GOTO_PREFIX = "https://www.google.com/goto?"
USER_AGENT = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/130.0 Safari/537.36")
NUM_SHARDS = 256
TAGS = ("head", "random")

SCHEMA = """
CREATE TABLE IF NOT EXISTS golden (
    id INTEGER PRIMARY KEY,          -- metric_url.id
    query_id INTEGER, keyword TEXT, tags TEXT, rank INTEGER, url TEXT,
    is_discovered INTEGER, is_crawled INTEGER, is_indexed INTEGER, shard_id INTEGER
);
CREATE TABLE IF NOT EXISTS goto (
    url TEXT PRIMARY KEY,
    status TEXT,                     -- NULL pending | resolved | blocked | error
    http_status INTEGER,
    location TEXT,
    attempts INTEGER DEFAULT 0,
    error TEXT,
    updated_at TEXT
);
"""


def is_goto(url: str) -> bool:
    return url.startswith(GOTO_PREFIX)


def classify(http_status, location):
    """Map one goto response to (status, location).

    A redirect to a non-Google host is the real page. A redirect back to Google
    (e.g. /sorry/ captcha) or a 429 means we are being throttled.
    """
    if http_status in (301, 302, 303, 307, 308) and location:
        host = (urlsplit(location).hostname or "").lower()
        if host == "google.com" or host.endswith(".google.com"):
            return "blocked", location
        return "resolved", location
    if http_status in (429, 503):
        return "blocked", location
    return "error", location


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


_opener = urllib.request.build_opener(_NoRedirect)


def fetch_location(url: str, timeout: float = 20):
    """Return (http_status, Location header) without following the redirect."""
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        resp = _opener.open(req, timeout=timeout)
        return resp.status, resp.headers.get("Location")
    except urllib.error.HTTPError as e:
        return e.code, e.headers.get("Location")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def open_state(path: str) -> sqlite3.Connection:
    con = sqlite3.connect(path)
    con.executescript(SCHEMA)
    return con


def pg_connect(user, password, host_port, dbname):
    host, port = host_port.split(":")
    con = psycopg2.connect(host=host, port=int(port), user=user, password=password,
                           dbname=dbname, connect_timeout=15)
    con.set_session(readonly=True)
    return con


def cmd_export(args):
    state = open_state(args.state)
    with pg_connect("metric", "metric", args.metric_db_url, "metricdb") as mdb:
        cur = mdb.cursor()
        cur.execute(
            """SELECT u.id, u.query_id, q.keyword, q.tags::text, u.rank, u.url,
                      u.is_discovered, u.is_crawled, u.is_indexed, u.shard_id
                 FROM metric_url u JOIN metric_queries q ON u.query_id = q.id
                WHERE q.batch_id = %s""",
            (args.batch,),
        )
        rows = cur.fetchall()
    state.executemany("INSERT OR REPLACE INTO golden VALUES (?,?,?,?,?,?,?,?,?,?)", rows)
    state.executemany("INSERT OR IGNORE INTO goto (url) VALUES (?)",
                      [(r[5],) for r in rows if is_goto(r[5])])
    state.commit()
    n_goto = state.execute("SELECT count(*) FROM goto").fetchone()[0]
    print(f"exported {len(rows)} metric_url rows of batch {args.batch}; {n_goto} distinct goto URLs")


def cmd_resolve(args):
    state = open_state(args.state)
    blocked_streak = 0
    while True:
        row = state.execute(
            "SELECT url, attempts FROM goto WHERE status IS NULL OR "
            "(status IN ('blocked', 'error') AND attempts < ?) ORDER BY attempts, url LIMIT 1",
            (args.max_attempts,),
        ).fetchone()
        if row is None:
            break
        url, attempts = row
        try:
            http_status, location = fetch_location(url)
            status, location = classify(http_status, location)
            error = None
        except Exception as e:
            http_status, location, status, error = None, None, "error", repr(e)[:300]
        state.execute(
            "UPDATE goto SET status=?, http_status=?, location=?, attempts=?, error=?, updated_at=? "
            "WHERE url=?",
            (status, http_status, location, attempts + 1, error, now_iso(), url),
        )
        state.commit()

        if status == "blocked":
            blocked_streak += 1
            if blocked_streak >= args.max_blocked:
                print(f"{now_iso()} stopping: {blocked_streak} blocked responses in a row")
                return
            print(f"{now_iso()} blocked ({http_status}), backing off {args.backoff}s")
            time.sleep(args.backoff)
            continue
        blocked_streak = 0
        time.sleep(args.interval)
    print(f"{now_iso()} nothing left to resolve")
    cmd_status(args)


def cmd_status(args):
    state = open_state(args.state)
    for status, n in state.execute(
            "SELECT coalesce(status, 'pending'), count(*) FROM goto GROUP BY 1 ORDER BY 1"):
        print(f"{status:9} {n}")


def golden_url_for(url: str, resolved: dict) -> str:
    """The URL a golden row stands for: the redirect target for a resolved goto."""
    return resolved.get(url, url)


def scan_url_shards(crawler_dsn, shard_ids, urls):
    found = []
    con = pg_connect(*crawler_dsn)
    try:
        cur = con.cursor()
        for shard_id in shard_ids:
            cur.execute(
                f"SELECT url, last_fetch_ok IS NOT NULL FROM url_state_current_{shard_id:03d} "
                "WHERE url = ANY(%s)",
                (urls,),
            )
            found.extend((url, bool(crawled), False, shard_id) for url, crawled in cur.fetchall())
    finally:
        con.close()
    return found


def lookup_coverage(golden_urls, args):
    """Same rules as CrawlerAllMetricMeasure: discovered/crawled from every
    url_state_current shard, indexed from selectdb, team from shard or domain."""
    lookup_map = build_lookup_map(golden_urls)
    urls = list(lookup_map)
    status = {u: {"discovered": False, "crawled": False, "indexed": False, "shard_id": -1}
              for u in golden_urls}

    crawler_dsn = ("crawler", "crawler", args.crawler_db_url, "crawlerdb")
    chunks = [range(i, min(i + 16, NUM_SHARDS)) for i in range(0, NUM_SHARDS, 16)]
    with ThreadPoolExecutor(max_workers=len(chunks)) as pool:
        for found in pool.map(lambda c: scan_url_shards(crawler_dsn, c, urls), chunks):
            apply_found_urls(status, lookup_map, found)

    if args.select_db_url:
        with pg_connect("select", "select", args.select_db_url, "selectdb") as sdb:
            cur = sdb.cursor()
            cur.execute("SELECT url FROM public.selected_urls_current WHERE url = ANY(%s)", (urls,))
            for (url,) in cur.fetchall():
                for golden in lookup_map.get(url, ()):
                    status[golden]["indexed"] = True

    extract = tldextract.TLDExtract(cache_dir=False)

    def domain(url):
        e = extract(url)
        return f"{e.domain}.{e.suffix}" if e.domain and e.suffix else ""

    missing = {domain(u) for u, s in status.items() if s["shard_id"] == -1} - {""}
    with pg_connect(*crawler_dsn) as cdb:
        cur = cdb.cursor()
        cur.execute("SELECT domain, shard_id FROM domain_state WHERE domain = ANY(%s)", (list(missing),))
        domain_shard = dict(cur.fetchall())
    for u, s in status.items():
        if s["shard_id"] == -1:
            s["shard_id"] = domain_shard.get(domain(u), -1)
    return status


def summarize(golden_urls, status):
    """Count each distinct golden URL once, as CrawlerAllMetricMeasure does."""
    stats = {g: {"total": 0, "disc": 0, "crawl": 0, "idx": 0} for g in ("Total", "A", "B")}
    for url in golden_urls:
        s = status[url]
        groups = ["Total"]
        if 0 <= s["shard_id"] <= 127:
            groups.append("A")
        elif 128 <= s["shard_id"] <= 255:
            groups.append("B")
        for g in groups:
            stats[g]["total"] += 1
            stats[g]["disc"] += s["discovered"]
            stats[g]["crawl"] += s["crawled"]
            stats[g]["idx"] += s["indexed"]
    return stats


def fmt(stats):
    out = []
    for g, s in stats.items():
        t = s["total"] or 1
        out.append(f"{g:5} n={s['total']:5}  disc {s['disc'] / t:6.1%}  "
                   f"crawl {s['crawl'] / t:6.1%}  idx {s['idx'] / t:6.1%}")
    return "\n".join(out)


def cmd_coverage(args):
    state = open_state(args.state)
    resolved = dict(state.execute("SELECT url, location FROM goto WHERE status = 'resolved'"))
    n_goto = state.execute("SELECT count(*) FROM goto").fetchone()[0]
    rows = state.execute("SELECT id, tags, rank, url FROM golden").fetchall()

    # The measure normalizes every golden URL the same way, goto or not.
    original = {r[3]: normalize_golden_url(r[3]) for r in rows}
    target = {r[3]: normalize_golden_url(golden_url_for(r[3], resolved)) for r in rows}
    status = lookup_coverage(sorted(set(original.values()) | set(target.values())), args)

    print(f"goto resolved {len(resolved)}/{n_goto}; crawlerdb {args.crawler_db_url}, "
          f"selectdb {args.select_db_url or '-'}")
    report = {}
    for tag in TAGS:
        tagged = [r for r in rows if tag in json.loads(r[1])]
        before = summarize({original[r[3]] for r in tagged}, status)
        after = summarize({target[r[3]] for r in tagged}, status)
        report[tag] = {"goto_as_is": before, "goto_resolved": after}
        print(f"\n== {tag}: goto as is ==\n{fmt(before)}")
        print(f"== {tag}: goto resolved ==\n{fmt(after)}")
    if args.json:
        with open(args.json, "w") as f:
            json.dump(report, f, indent=1)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("export", help="copy the batch's metric_url rows into the state file")
    p.add_argument("--batch", type=int, required=True)
    p.add_argument("--metric_db_url", default="172.16.191.1:5433")
    p.set_defaults(func=cmd_export)

    p = sub.add_parser("resolve", help="follow each pending goto link once (resumable)")
    p.add_argument("--interval", type=float, default=5.0, help="seconds between requests")
    p.add_argument("--backoff", type=float, default=600.0, help="seconds to wait after a blocked response")
    p.add_argument("--max_blocked", type=int, default=5, help="stop after this many blocked responses in a row")
    p.add_argument("--max_attempts", type=int, default=3)
    p.set_defaults(func=cmd_resolve)

    p = sub.add_parser("status", help="count goto links by status")
    p.set_defaults(func=cmd_status)

    p = sub.add_parser("coverage", help="coverage with goto links as is vs resolved")
    p.add_argument("--crawler_db_url", default="172.16.191.3:5433")
    p.add_argument("--select_db_url", default="172.16.191.1:5445")
    p.add_argument("--json", help="also write the numbers to this file")
    p.set_defaults(func=cmd_coverage)

    for p in sub.choices.values():
        p.add_argument("--state", required=True, help="local SQLite file holding the results")
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
