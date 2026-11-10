#!/usr/bin/env python3
"""Germany collection through JobSpy (speedyapply/JobSpy, pip: python-jobspy).

Calls jobspy.scrape_jobs directly. No title filter: every search uses an empty
search term and is partitioned only by location and job type, because each
job-board search is capped at roughly 1,000 results.

Output files use the same layout as germany_collect.py so the merge step
picks them up.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import math
import sys
import time
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from germany_collect import NOW_ISO, classify_germany  # noqa: E402

LOCATIONS = [
    "Germany", "Berlin", "Frankfurt am Main", "Munich", "Hamburg", "Cologne", "Düsseldorf", "Stuttgart",
    "Darmstadt", "Mainz", "Wiesbaden", "Mannheim", "Bonn", "Leipzig", "Dresden", "Aachen", "Hanover",
    "Nuremberg", "Essen", "Dortmund", "Bremen", "Karlsruhe", "Münster", "Heidelberg", "Kiel",
]
JOB_TYPES = [None, "fulltime", "parttime", "internship", "contract"]
SITES = {"indeed": 1000, "linkedin": 200, "glassdoor": 200}


def _clean(v):
    if v is None:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    s = str(v).strip()
    return s or None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--limit", type=int, default=0, help="max number of searches (0 = all)")
    ap.add_argument("--budget-min", type=float, default=300)
    args = ap.parse_args()

    from jobspy import scrape_jobs

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    raw_f = gzip.open(out / "raw_jobspy.jsonl.gz", "wt", encoding="utf-8")
    de_f = gzip.open(out / "germany_jobspy.jsonl.gz", "wt", encoding="utf-8")
    rows, errors, totals = [], [], Counter()
    seen: set[tuple[str, str]] = set()
    t0 = time.monotonic()
    searches = [(site, loc, jt) for site in SITES for loc in LOCATIONS for jt in JOB_TYPES]
    # LinkedIn/Glassdoor rate-limit quickly: one search per location only
    searches = [s for s in searches if s[0] == "indeed" or s[2] is None]
    if args.limit:
        searches = searches[: args.limit]
    consecutive_fail = Counter()

    for n, (site, loc, jt) in enumerate(searches, 1):
        board = f"{loc} | job_type={jt or 'any'}"
        if time.monotonic() - t0 > args.budget_min * 60:
            rows.append({"source": site, "board": board, "status": "not_queried", "records": 0,
                         "germany_records": 0, "http_requests": 0, "seconds": 0, "detail": "time budget exhausted"})
            continue
        if consecutive_fail[site] >= 5:
            rows.append({"source": site, "board": board, "status": "not_queried", "records": 0,
                         "germany_records": 0, "http_requests": 0, "seconds": 0,
                         "detail": "site skipped after 5 consecutive failed searches"})
            continue
        t1 = time.monotonic()
        status, detail, new, de = "ok", "", 0, 0
        try:
            kwargs = dict(site_name=[site], search_term="", location=loc, results_wanted=SITES[site],
                          country_indeed="Germany", description_format="plain", verbose=0)
            if jt:
                kwargs["job_type"] = jt
            df = scrape_jobs(**kwargs)
            records = df.to_dict("records") if df is not None else []
        except Exception as exc:
            records, status, detail = [], "failed", f"{type(exc).__name__}: {str(exc)[:200]}"
        for r in records:
            jid = _clean(r.get("id")) or _clean(r.get("job_url")) or ""
            if (site, jid) in seen:
                continue
            seen.add((site, jid))
            remote = r.get("is_remote")
            rec = {
                "source": site,
                "title": _clean(r.get("title")),
                "company": _clean(r.get("company")),
                "job_url": _clean(r.get("job_url")),
                "location": _clean(r.get("location")),
                "remote_type": "remote" if remote is True else None,
                "salary_raw": None, "salary_min_usd": None, "salary_max_usd": None,
                "posted_at": _clean(r.get("date_posted")),
                "apply_url": _clean(r.get("job_url_direct")),
                "external_id": jid or None,
                "description": _clean(r.get("description")),
                "tags": [t for t in [_clean(r.get("job_type"))] if t],
                "metadata": {"via": "jobspy", "query_location": loc, "query_job_type": jt},
                "collected_at": NOW_ISO,
            }
            raw_f.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")
            new += 1
            rec["germany_match"] = (classify_germany(rec["location"], rec["remote_type"], site)
                                    or "jobspy_germany_query")
            de_f.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")
            de += 1
        if status == "ok" and not records:
            status = "empty"
        consecutive_fail[site] = consecutive_fail[site] + 1 if status == "failed" else 0
        totals["raw"] += new
        rows.append({"source": site, "board": board, "status": status, "records": new, "germany_records": de,
                     "http_requests": "", "seconds": round(time.monotonic() - t1, 1), "detail": detail})
        print(f"[jobspy] {n}/{len(searches)} {site} {board}: {status} returned={len(records)} new={new} {detail}",
              flush=True)
        time.sleep(8 if site == "linkedin" else 2)

    raw_f.close()
    de_f.close()
    with open(out / "boards_jobspy.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["source", "board", "status", "records", "germany_records",
                                          "http_requests", "seconds", "detail"])
        w.writeheader()
        w.writerows(rows)
    per_site = Counter(r["source"] for r in rows)
    for site in SITES:
        stats = {"tag": f"jobspy-{site}", "source": site, "collected_at": NOW_ISO,
                 "boards_discovered_total": per_site[site], "errors": [], "list_sources": []}
        (out / f"stats_jobspy-{site}.json").write_text(json.dumps(stats))
    print(json.dumps({"raw_records": totals["raw"],
                      "status": dict(Counter((r["source"], r["status"]) .__str__() for r in rows))}, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
