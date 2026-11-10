#!/usr/bin/env python3
"""Germany-wide collection runner for Job Forager.

This is NOT a scraper. Every network request and every record mapping is done
by the existing jobforager collectors (src/jobforager/search/*.py). This runner
only:

  * builds the company-board lists (engine lists + live Feashliaa/stapply-ai),
  * calls the engine's own per-board fetch function for every board,
  * wraps urllib.request.urlopen to rate-limit, retry 429/5xx and record the
    HTTP outcome of each board (the engine swallows errors silently),
  * streams the unfiltered raw records to disk and tags the Germany subset,
  * merges, dedupes (engine dedupe), validates URLs (engine validator) and
    exports CSV / XLSX / JSON + reports.

Usage:
  germany_collect.py collect --source greenhouse [--shard 0/4] --out out/
  germany_collect.py merge --in artifacts/ --out final/
"""

from __future__ import annotations

import argparse
import csv
import gzip
import html
import io
import json
import os
import re
import sys
import threading
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

NOW_ISO = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

# --------------------------------------------------------------------------
# urlopen instrumentation: rate limit + retry + per-board outcome log
# --------------------------------------------------------------------------

_ORIG_URLOPEN = urllib.request.urlopen
_TLS = threading.local()

# requests per second per host group
_RATES = {
    "greenhouse.io": 20.0,
    "lever.co": 10.0,
    "ashbyhq.com": 10.0,
    "smartrecruiters.com": 10.0,
    "personio.de": 8.0,
    "personio.com": 8.0,
    "myworkdayjobs.com": 25.0,
    "myworkdaysite.com": 10.0,
    "pinpointhq.com": 5.0,
    "hiring.cafe": 1.0,
    "githubusercontent.com": 5.0,
    "firebaseio.com": 10.0,
}
_DEFAULT_RATE = 4.0
_RETRY_CODES = {429, 500, 502, 503, 504}
_MAX_RETRIES = int(os.environ.get("GC_MAX_RETRIES", "3"))

_rl_lock = threading.Lock()
_rl_next: dict[str, float] = defaultdict(float)
REQUEST_COUNT = Counter()


def _host_group(url: str) -> str:
    host = urllib.parse.urlsplit(url).hostname or ""
    parts = host.split(".")
    return ".".join(parts[-2:]) if len(parts) >= 2 else host


def _throttle(group: str) -> None:
    interval = 1.0 / _RATES.get(group, _DEFAULT_RATE)
    with _rl_lock:
        now = time.monotonic()
        slot = max(now, _rl_next[group])
        _rl_next[group] = slot + interval
    delay = slot - time.monotonic()
    if delay > 0:
        time.sleep(delay)


def _event(method: str, url: str, status: Any) -> None:
    events = getattr(_TLS, "events", None)
    if events is not None:
        events.append((method, url, status))


def _patched_urlopen(req, *args, **kwargs):
    if isinstance(req, urllib.request.Request):
        url, method = req.full_url, req.get_method()
    else:
        url, method = str(req), "GET"
    group = _host_group(url)
    no_retry = getattr(_TLS, "no_retry", False)
    attempt = 0
    while True:
        _throttle(group)
        REQUEST_COUNT[group] += 1
        try:
            resp = _ORIG_URLOPEN(req, *args, **kwargs)
            _event(method, url, getattr(resp, "status", 200))
            return resp
        except urllib.error.HTTPError as exc:
            if exc.code in _RETRY_CODES and attempt < _MAX_RETRIES and not no_retry:
                attempt += 1
                wait = min(30.0, 2.0 ** attempt)
                ra = exc.headers.get("Retry-After") if exc.headers else None
                if ra and ra.isdigit():
                    wait = min(60.0, float(ra))
                time.sleep(wait)
                continue
            _event(method, url, exc.code)
            raise
        except Exception as exc:  # timeouts, DNS, TLS, resets
            if attempt < 1 and not no_retry:
                attempt += 1
                time.sleep(2.0)
                continue
            _event(method, url, f"{type(exc).__name__}: {str(exc)[:120]}")
            raise


urllib.request.urlopen = _patched_urlopen

# --------------------------------------------------------------------------
# Germany classification (post-collection tag; raw data is never filtered)
# --------------------------------------------------------------------------

_CITIES = """
berlin hamburg munich münchen muenchen cologne köln koeln frankfurt stuttgart
düsseldorf dusseldorf duesseldorf leipzig dortmund essen bremen dresden hanover
hannover nuremberg nürnberg nuernberg duisburg bochum wuppertal bielefeld bonn
münster muenster mannheim karlsruhe augsburg wiesbaden mönchengladbach
gelsenkirchen aachen braunschweig brunswick kiel chemnitz halle magdeburg
freiburg krefeld mainz lübeck luebeck erfurt oberhausen rostock kassel
saarbrücken saarbruecken saarbrucken potsdam ludwigshafen mülheim muelheim
oldenburg osnabrück osnabrueck leverkusen heidelberg darmstadt solingen
regensburg paderborn ingolstadt würzburg wuerzburg wurzburg ulm heilbronn
pforzheim wolfsburg göttingen goettingen gottingen bottrop reutlingen koblenz
erlangen bremerhaven recklinghausen remscheid jena trier salzgitter moers
siegen hildesheim cottbus gütersloh guetersloh kaiserslautern witten hanau
schwerin esslingen ludwigsburg tübingen tuebingen tubingen flensburg konstanz
worms dessau neuss villingen-schwenningen ratingen lüneburg lueneburg bayreuth
bamberg aschaffenburg landshut kempten rosenheim neu-ulm passau fulda gießen
giessen marburg offenbach rüsselsheim ruesselsheim eschborn "bad homburg"
oberursel kronberg walldorf weinheim sindelfingen böblingen boeblingen
leinfelden friedrichshafen ravensburg garching unterföhring unterfoehring
ismaning ottobrunn herzogenaurach fürth fuerth schweinfurt coburg zwickau
plauen görlitz greifswald stralsund wismar "frankfurt am main" "frankfurt/main"
norderstedt pinneberg wolfenbüttel celle emden wilhelmshaven delmenhorst
lingen rheine bocholt iserlohn lüdenscheid hamm unna lünen dormagen
langenfeld hilden troisdorf "bergisch gladbach" hürth kerpen düren dueren
euskirchen velbert viersen monheim meerbusch willich kaarst grevenbroich
detmold minden herford "bad oeynhausen" lippstadt soest arnsberg wetzlar
"bad vilbel" "neu-isenburg" "neu isenburg" dreieich langen bensheim
"bad nauheim" friedberg limburg speyer landau neustadt pirmasens zweibrücken
homburg neunkirchen baden-baden offenburg lörrach loerrach singen biberach
aalen "schwäbisch gmünd" "schwäbisch hall" göppingen goeppingen waiblingen
backnang heidenheim memmingen kaufbeuren straubing deggendorf amberg weiden
hof ansbach neumarkt freising erding dachau germering starnberg
holzkirchen penzberg gera weimar gotha eisenach suhl nordhausen dessau-roßlau
stendal brandenburg oranienburg eberswalde "frankfurt (oder)" ludwigsfelde
schönefeld schoenefeld grünheide gruenheide teltow kleinmachnow neubrandenburg
leuna bitterfeld wolfen merseburg wittenberg hoyerswerda bautzen freiberg
"st. leon-rot" walldorf/baden schwalbach sulzbach bietigheim-bissingen
""".strip()


def _split_terms(text: str) -> list[str]:
    return [m.group(1) or m.group(2) for m in re.finditer(r'"([^"]+)"|(\S+)', text)]


_CITY_SET = sorted(set(_split_terms(_CITIES)), key=len, reverse=True)
_CITY_RE = re.compile(
    r"(?<![a-zäöüß])(" + "|".join(re.escape(c) for c in _CITY_SET) + r")(?![a-zäöüß])"
)
_COUNTRY_RE = re.compile(
    r"(?<![a-zäöüß])(germany|deutschland|allemagne|alemania|bundesweit|deutschlandweit|"
    r"germany-wide|nationwide germany)(?![a-zäöüß])"
    r"|(?:^|[,;|/(\-–])\s*(de|deu|ger)\s*(?:$|[,;|/)\-–])"
)
_STATE_RE = re.compile(
    r"(?<![a-zäöüß])(bayern|bavaria|hessen|hesse|nordrhein-westfalen|north rhine-westphalia|"
    r"nrw|baden-württemberg|baden-wuerttemberg|baden-wurttemberg|niedersachsen|lower saxony|"
    r"sachsen|saxony|thüringen|thuringia|schleswig-holstein|saarland|rheinland-pfalz|"
    r"rhineland-palatinate|mecklenburg-vorpommern|sachsen-anhalt|saxony-anhalt)(?![a-zäöüß])"
)
# markers that a city-only hit is really a namesake outside Germany
_FOREIGN_RE = re.compile(
    r"(?<![a-z])(united states|usa|u\.s\.a?\.?|us|canada|australia|south africa|new zealand|"
    r"ontario|victoria|pennsylvania|connecticut|new york|new jersey|kentucky|wisconsin|"
    r"maryland|new hampshire|ohio|michigan|indiana|illinois|georgia|texas|california|"
    r"massachusetts|virginia|carolina|florida|minnesota|iowa|kansas|nebraska|dakota|"
    r"oklahoma|arkansas|louisiana|mississippi|alabama|tennessee|vermont|maine|missouri)(?![a-z])"
    r"|,\s*(ct|ny|nj|pa|ky|il|wi|md|nh|oh|mi|in|mo|ga|tx|ca|ma|va|nc|sc|fl|wa|or|mn|ia|ks|"
    r"ne|nd|sd|ok|ar|la|ms|al|tn|wv|vt|me|ri|az|nm|co|ut|nv|id|mt|wy|ak|hi|dc|on|bc|qc|ab)"
    r"(?:\s*\d{5})?\s*(?:$|[,;|/)])"
)
_DELAWARE_RE = re.compile(
    r"(wilmington|newark|dover|middletown|lewes|georgetown|smyrna|milford|seaford|"
    r"new castle|claymont|bear|delaware)"
)
_REMOTE_RE = re.compile(
    r"(remote|home ?office|homeoffice|work from home|wfh|anywhere|worldwide|telearbeit|distributed)"
)
_REGION_RE = re.compile(
    r"(?<![a-z])(dach|d-a-ch|europe|european union|eu|emea|europa|worldwide|global|anywhere|"
    r"cet|central european)(?![a-z])"
)
_REMOTE_BOARDS = {"remotive", "remoteok", "weworkremotely"}


def _segment_match(seg: str) -> str | None:
    country = _COUNTRY_RE.search(seg)
    if country:
        # ", DE" is also Delaware
        token = country.group(2)
        if token == "de" and (_DELAWARE_RE.search(seg) or _FOREIGN_RE.search(seg)):
            pass
        else:
            return "germany_country"
    if _STATE_RE.search(seg) and not _FOREIGN_RE.search(seg):
        return "germany_state"
    if _CITY_RE.search(seg) and not _FOREIGN_RE.search(seg):
        return "germany_city"
    return None


def classify_germany(location: str | None, remote_type: str | None, source: str) -> str | None:
    """Return a match label if the record is a Germany / Germany-eligible remote job."""
    if not location:
        return None
    loc = location.lower().strip()
    segments = [s for s in re.split(r"\s*(?:;|\||\bor\b|\band\b|•|\n)\s*", loc) if s] or [loc]
    best = None
    for seg in segments:
        m = _segment_match(seg)
        if m == "germany_country":
            return m
        best = best or m
    if best:
        return best
    is_remote = bool(_REMOTE_RE.search(loc)) or remote_type == "remote" or source in _REMOTE_BOARDS
    if is_remote and _REGION_RE.search(loc):
        return "remote_region"
    return None


# --------------------------------------------------------------------------
# Board lists
# --------------------------------------------------------------------------

_FEASH = "https://raw.githubusercontent.com/Feashliaa/job-board-aggregator/main/data"
_STAPPLY = "https://raw.githubusercontent.com/stapply-ai/ats-scrapers/main/ats-companies"
LIST_LOG: list[dict[str, Any]] = []


def _get(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (compatible; jobforager/1.0)"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return resp.read()


def _fetch_list(ats: str, origin: str, url: str, parse: Callable[[bytes], list]) -> list:
    try:
        items = parse(_get(url))
        LIST_LOG.append({"ats": ats, "list": origin, "url": url, "status": "ok", "entries": len(items)})
        return items
    except Exception as exc:
        LIST_LOG.append({"ats": ats, "list": origin, "url": url, "status": f"failed: {exc}", "entries": 0})
        return []


def _csv_rows(raw: bytes) -> list[dict[str, str]]:
    return list(csv.DictReader(io.StringIO(raw.decode("utf-8", "replace"))))


def _dedupe_ci(items: list[str]) -> list[str]:
    seen: dict[str, str] = {}
    for it in items:
        it = (it or "").strip().strip("/")
        if it and it.lower() not in seen:
            seen[it.lower()] = it
    return sorted(seen.values(), key=str.lower)


def _string_boards(ats: str, engine_list: list[str], feash: bool = True) -> list[str]:
    LIST_LOG.append({"ats": ats, "list": "engine (data/*.json + engine fetch)", "url": "", "status": "ok",
                     "entries": len(engine_list)})
    items = list(engine_list)
    if feash:
        items += _fetch_list(ats, "Feashliaa", f"{_FEASH}/{ats}_companies.json",
                             lambda b: [x for x in json.loads(b) if isinstance(x, str)])
    items += _fetch_list(ats, "stapply-ai", f"{_STAPPLY}/{ats}.csv",
                         lambda b: [r.get("slug", "") for r in _csv_rows(b)])
    return _dedupe_ci(items)


def build_boards(source: str) -> list[Any]:
    from jobforager import company_lists as cl

    if source == "greenhouse":
        return _string_boards("greenhouse", cl.get_greenhouse_tokens())
    if source == "lever":
        return _string_boards("lever", cl.get_lever_slugs())
    if source == "ashby":
        return _string_boards("ashby", cl.get_ashby_boards())
    if source == "smartrecruiters":
        return _string_boards("smartrecruiters", cl.get_smartrecruiters_slugs(), feash=False)
    if source == "personio":
        return _string_boards("personio", cl.get_personio_subdomains(), feash=False)
    if source == "pinpoint":
        from jobforager.search import pinpoint

        return _string_boards("pinpoint", list(pinpoint._PINPOINT_SUBDOMAINS), feash=False)
    if source == "workday":
        engine = cl.get_workday_companies()
        LIST_LOG.append({"ats": "workday", "list": "engine (data/*.json + engine fetch)", "url": "",
                         "status": "ok", "entries": len(engine)})
        comps = list(engine)

        def parse_feash(b: bytes) -> list[dict[str, str]]:
            out = []
            for x in json.loads(b):
                p = str(x).split("|")
                if len(p) == 3 and all(p):
                    out.append({"name": p[0], "url": f"https://{p[0]}.{p[1]}.myworkdayjobs.com/{p[2]}"})
            return out

        comps += _fetch_list("workday", "Feashliaa", f"{_FEASH}/workday_companies.json", parse_feash)
        comps += _fetch_list(
            "workday", "stapply-ai", f"{_STAPPLY}/workday.csv",
            lambda b: [{"name": r.get("name", ""), "url": r.get("url", "")} for r in _csv_rows(b) if r.get("url")],
        )
        seen: dict[str, dict[str, str]] = {}
        for c in comps:
            key = c["url"].rstrip("/").lower()
            if key and key not in seen:
                seen[key] = c
        return [seen[k] for k in sorted(seen)]
    raise ValueError(source)


# --------------------------------------------------------------------------
# Per-board fetch through the engine
# --------------------------------------------------------------------------

def _engine_fetcher(source: str) -> Callable[[Any], list[dict[str, Any]]]:
    if source == "greenhouse":
        from jobforager.search.greenhouse import _fetch_greenhouse_board

        return lambda b: _fetch_greenhouse_board(b, include_content=False)
    if source == "lever":
        from jobforager.search.lever import _fetch_lever_company

        return _fetch_lever_company
    if source == "ashby":
        from jobforager.search.ashby import _fetch_ashby_board

        return lambda b: _fetch_ashby_board(b, include_compensation=True)
    if source == "smartrecruiters":
        from jobforager.search.smartrecruiters import _fetch_smartrecruiters_company

        return _fetch_smartrecruiters_company
    if source == "workday":
        from jobforager.search.workday import _fetch_workday_company

        return lambda b: _fetch_workday_company(b, None)
    if source == "personio":
        from jobforager.search.personio import _fetch_personio_company

        return _fetch_personio_company
    if source == "pinpoint":
        from jobforager.search.pinpoint import _fetch_subdomain, _normalize_posting

        def fetch(sd: str) -> list[dict[str, Any]]:
            out = []
            for posting in _fetch_subdomain(sd):
                rec = _normalize_posting(posting, sd) if isinstance(posting, dict) else None
                if rec:
                    out.append(rec)
            return out

        return fetch
    raise ValueError(source)


def _board_id(board: Any) -> str:
    return board["url"] if isinstance(board, dict) else str(board)


def _status(source: str, n: int, events: list[tuple]) -> tuple[str, str]:
    errs = [e for e in events if not (isinstance(e[2], int) and e[2] < 400)]
    detail = "; ".join(f"{m} {s}" for m, _, s in errs[:3])
    if n > 0:
        return ("partial" if errs else "ok"), detail
    if errs:
        if all(e[2] in (404, 410) for e in errs):
            return "not_found", detail
        return "failed", detail
    if source == "workday" and not any(e[0] == "POST" for e in events):
        return "failed", "no CSRF token returned by career site (engine skipped board)"
    return "empty", ""


def _tag(rec: dict[str, Any]) -> str | None:
    return classify_germany(rec.get("location"), rec.get("remote_type"), rec.get("source", ""))


def run_board(source: str, fetch: Callable, board: Any) -> dict[str, Any]:
    _TLS.events = []
    t0 = time.monotonic()
    try:
        records = fetch(board) or []
        exc = ""
    except Exception as e:  # engine bug on odd payloads
        records, exc = [], f"{type(e).__name__}: {e}"
    events = list(_TLS.events)
    germany = [(r, _tag(r)) for r in records]
    germany = [(r, t) for r, t in germany if t]
    # Greenhouse: descriptions need content=true; refetch only boards with German jobs
    if source == "greenhouse" and germany:
        from jobforager.search.greenhouse import _fetch_greenhouse_board

        _TLS.events = []
        full = _fetch_greenhouse_board(board, include_content=True) or []
        if full:
            germany = [(r, _tag(r)) for r in full]
            germany = [(r, t) for r, t in germany if t]
        events += list(_TLS.events)
    status, detail = _status(source, len(records), events)
    if exc:
        status, detail = "failed", exc
    _TLS.events = None
    return {
        "board": _board_id(board), "status": status, "detail": detail, "records": records,
        "germany": germany, "requests": len(events), "seconds": round(time.monotonic() - t0, 2),
    }


def _json_default(o: Any) -> str:
    return o.isoformat() if hasattr(o, "isoformat") else str(o)


def cmd_collect(args: argparse.Namespace) -> int:
    source = args.source
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    tag = source if not args.shard else f"{source}-{args.shard.replace('/', 'of')}"
    raw_f = gzip.open(out / f"raw_{tag}.jsonl.gz", "wt", encoding="utf-8")
    de_f = gzip.open(out / f"germany_{tag}.jsonl.gz", "wt", encoding="utf-8")
    board_rows: list[dict[str, Any]] = []
    errors: list[str] = []
    totals = Counter()
    t_start = time.monotonic()
    deadline = t_start + float(args.budget_min) * 60

    def emit(records: list[dict], germany: list[tuple]) -> None:
        for r in records:
            r["collected_at"] = NOW_ISO
            raw_f.write(json.dumps(r, ensure_ascii=False, default=_json_default) + "\n")
        for r, t in germany:
            r["collected_at"] = NOW_ISO
            r["germany_match"] = t
            de_f.write(json.dumps(r, ensure_ascii=False, default=_json_default) + "\n")
        totals["raw"] += len(records)
        totals["germany"] += len(germany)

    if source in ("greenhouse", "lever", "ashby", "smartrecruiters", "workday", "personio", "pinpoint"):
        boards = build_boards(source)
        totals["boards_discovered_total"] = len(boards)
        if args.shard:
            i, n = (int(x) for x in args.shard.split("/"))
            boards = boards[i::n]
        if args.limit:
            boards = boards[: args.limit]
        fetch = _engine_fetcher(source)
        print(f"[{tag}] boards to query: {len(boards)} (discovered {totals['boards_discovered_total']})", flush=True)
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            futs = {ex.submit(run_board, source, fetch, b): b for b in boards}
            for n_done, fut in enumerate(as_completed(futs), 1):
                res = fut.result()
                emit(res["records"], res["germany"])
                board_rows.append({
                    "source": source, "board": res["board"], "status": res["status"],
                    "records": len(res["records"]), "germany_records": len(res["germany"]),
                    "http_requests": res["requests"], "seconds": res["seconds"], "detail": res["detail"],
                })
                if n_done % 250 == 0 or n_done == len(boards):
                    print(f"[{tag}] {n_done}/{len(boards)} boards, raw={totals['raw']} "
                          f"germany={totals['germany']} elapsed={int(time.monotonic() - t_start)}s", flush=True)
                if time.monotonic() > deadline:
                    errors.append(f"time budget of {args.budget_min} min hit after {n_done}/{len(boards)} boards")
                    for f in futs:
                        f.cancel()
                    break
        done = {r["board"] for r in board_rows}
        for b in boards:
            if _board_id(b) not in done:
                board_rows.append({"source": source, "board": _board_id(b), "status": "not_queried",
                                   "records": 0, "germany_records": 0, "http_requests": 0, "seconds": 0,
                                   "detail": "time budget exhausted"})
    else:
        # single-endpoint sources: call the engine collector as-is
        from jobforager.search.arbeitnow import collect_arbeitnow_jobs
        from jobforager.search.hackernews import collect_hackernews_jobs
        from jobforager.search.hiringcafe import collect_hiringcafe_jobs
        from jobforager.search.remoteok import collect_remoteok_jobs
        from jobforager.search.remotive import collect_remotive_jobs
        from jobforager.search.weworkremotely import collect_weworkremotely_jobs

        singles: dict[str, Callable[[], list]] = {
            "remotive": collect_remotive_jobs,
            "remoteok": collect_remoteok_jobs,
            "arbeitnow": collect_arbeitnow_jobs,
            "hackernews": lambda: collect_hackernews_jobs(max_comments=100000, max_workers=8),
            "weworkremotely": collect_weworkremotely_jobs,
            "hiringcafe": lambda: collect_hiringcafe_jobs(search_term=None, location="Germany"),
        }
        names = ["hiringcafe"] if source == "hiringcafe" else [
            "remotive", "remoteok", "arbeitnow", "hackernews", "weworkremotely"]
        for name in names:
            _TLS.events = []
            try:
                records = singles[name]() or []
                exc = ""
            except Exception as e:
                records, exc = [], f"{type(e).__name__}: {e}"
                errors.append(f"{name}: {exc}\n{traceback.format_exc(limit=3)}")
            events = list(_TLS.events)
            _TLS.events = None
            status, detail = _status(name, len(records), events)
            if exc:
                status, detail = "failed", exc
            if name == "hiringcafe":
                # the query itself was Germany-scoped by the engine
                germany = [(r, _tag(r) or "hiringcafe_germany_query") for r in records]
            else:
                germany = [(r, t) for r, t in ((r, _tag(r)) for r in records) if t]
            emit(records, germany)
            board_rows.append({"source": name, "board": "(single endpoint)", "status": status,
                               "records": len(records), "germany_records": len(germany),
                               "http_requests": len(events), "seconds": 0, "detail": detail})
            print(f"[{name}] status={status} records={len(records)} germany={len(germany)} {detail}", flush=True)

    raw_f.close()
    de_f.close()
    with open(out / f"boards_{tag}.csv", "w", newline="", encoding="utf-8") as f:
        cols = ["source", "board", "status", "records", "germany_records", "http_requests", "seconds", "detail"]
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(sorted(board_rows, key=lambda r: (r["source"], r["board"])))
    stats = {
        "tag": tag, "source": source, "collected_at": NOW_ISO,
        "boards_discovered_total": totals["boards_discovered_total"],
        "raw_records": totals["raw"], "germany_records": totals["germany"],
        "board_status": dict(Counter(r["status"] for r in board_rows)),
        "http_requests_by_host": dict(REQUEST_COUNT),
        "list_sources": LIST_LOG, "errors": errors,
        "elapsed_seconds": int(time.monotonic() - t_start),
    }
    (out / f"stats_{tag}.json").write_text(json.dumps(stats, indent=2))
    print(json.dumps({k: v for k, v in stats.items() if k != "list_sources"}, indent=2))
    for row in LIST_LOG:
        print("list_source", row)
    return 0


# --------------------------------------------------------------------------
# Merge / dedupe / validate / export
# --------------------------------------------------------------------------

_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"[ \t\r\f\v]+")
_ILLEGAL_XLSX = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")


def _plain(text: str | None) -> str:
    if not text:
        return ""
    t = html.unescape(text)
    if "<" in t:
        t = re.sub(r"(?i)<(br|/p|/div|/li|/h\d)[^>]*>", "\n", t)
        t = _TAG_RE.sub(" ", t)
        t = html.unescape(t)
    t = _WS_RE.sub(" ", t)
    return re.sub(r"\n\s*\n+", "\n\n", t).strip()


def cmd_merge(args: argparse.Namespace) -> int:
    from jobforager.cli.pipeline import dedupe_records
    from jobforager.normalize import normalize_raw_job_record
    from jobforager.search.validation import validate_job_url

    src = Path(args.inp)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    stats = [json.loads(p.read_text()) for p in sorted(src.rglob("stats_*.json"))]
    boards: list[dict[str, str]] = []
    for p in sorted(src.rglob("boards_*.csv")):
        with open(p, newline="", encoding="utf-8") as f:
            boards.extend(csv.DictReader(f))

    normalized = []
    extras: dict[int, dict[str, Any]] = {}
    norm_errors = Counter()
    collected = Counter()
    for p in sorted(src.rglob("germany_*.jsonl.gz")):
        with gzip.open(p, "rt", encoding="utf-8") as f:
            for line in f:
                raw = json.loads(line)
                collected[raw.get("source", "?")] += 1
                extra = {"germany_match": raw.pop("germany_match", ""), "collected_at": raw.pop("collected_at", NOW_ISO),
                         "posted_raw": raw.get("posted_at") if isinstance(raw.get("posted_at"), str) else ""}
                try:
                    job = normalize_raw_job_record(raw)
                except Exception as exc:
                    norm_errors[f"{raw.get('source')}: {exc}"] += 1
                    continue
                extras[id(job)] = extra
                normalized.append(job)

    unique, keys = dedupe_records(normalized)
    print(f"germany records collected={sum(collected.values())} normalized={len(normalized)} unique={len(unique)}",
          flush=True)

    # URL validation with the engine validator (concurrent, rate-limited by the urlopen wrapper)
    url_of = [(j.apply_url or j.job_url) for j in unique]
    results: list[dict[str, Any] | None] = [None] * len(unique)
    deadline = time.monotonic() + float(args.validate_budget_min) * 60
    # engine pipeline skips these two; job-board aggregators block automated HEAD checks
    skip_sources = {"weworkremotely", "hiringcafe", "indeed", "linkedin", "glassdoor"}

    def check(i: int) -> None:
        if time.monotonic() > deadline or unique[i].source in skip_sources:
            return
        _TLS.no_retry = False
        try:
            results[i] = validate_job_url(url_of[i], timeout=10.0)
        except Exception as exc:
            results[i] = {"status": "error", "http_code": None, "message": f"{type(exc).__name__}: {exc}"}

    if not args.no_validate:
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            for n, _ in enumerate(ex.map(check, range(len(unique))), 1):
                if n % 2000 == 0:
                    print(f"validated {n}/{len(unique)}", flush=True)

    cols = ["job_title", "employer", "location", "germany_match", "remote_type", "ats_provider", "source",
            "posting_date", "posting_date_raw", "collection_date", "application_url", "job_url",
            "url_status", "url_http_code", "external_id", "tags", "dedupe_key", "description"]
    rows = []
    for i, (job, key) in enumerate(zip(unique, keys)):
        ex = extras[id(job)]
        res = results[i]
        if res is None:
            status = "skipped" if job.source in skip_sources else "not_checked"
            code = ""
        else:
            status, code = res["status"], res.get("http_code") or ""
        rows.append({
            "job_title": job.title, "employer": job.company, "location": job.location or "",
            "germany_match": ex["germany_match"], "remote_type": job.remote_type or "",
            "ats_provider": job.ats_platform or job.source, "source": job.source,
            "posting_date": job.posted_at.isoformat() if job.posted_at else "",
            "posting_date_raw": ex["posted_raw"], "collection_date": ex["collected_at"],
            "application_url": url_of[i], "job_url": job.job_url, "url_status": status, "url_http_code": code,
            "external_id": job.external_id or "", "tags": "; ".join(job.tags), "dedupe_key": key,
            "description": _plain(job.description),
        })
    rows.sort(key=lambda r: (r["source"], r["employer"].lower(), r["job_title"].lower()))

    with open(out / "all_jobs.csv", "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)
    (out / "all_jobs.json").write_text(json.dumps(rows, ensure_ascii=False, indent=1), encoding="utf-8")

    from openpyxl import Workbook

    wb = Workbook(write_only=True)
    ws = wb.create_sheet("all_jobs")
    ws.append(cols)
    for r in rows:
        ws.append([_ILLEGAL_XLSX.sub("", str(r[c]))[:32000] for c in cols])

    # ---- reports -------------------------------------------------------
    by_src_boards: dict[str, Counter] = defaultdict(Counter)
    raw_by_src = Counter()
    for b in boards:
        by_src_boards[b["source"]][b["status"]] += 1
        raw_by_src[b["source"]] += int(b["records"] or 0)
    uniq_by_src = Counter(r["source"] for r in rows)
    ok_by_src = Counter(r["source"] for r in rows if r["url_status"] == "ok")
    desc_by_src = Counter(r["source"] for r in rows if r["description"])
    checked_by_src = Counter(r["source"] for r in rows if r["url_status"] in ("ok", "unreachable", "error"))
    employers_by_src: dict[str, set] = defaultdict(set)
    for r in rows:
        employers_by_src[r["source"]].add(r["employer"].lower())
    discovered = Counter()
    for s in stats:
        discovered[s["source"]] = max(discovered[s["source"]], s.get("boards_discovered_total", 0))

    rep_cols = ["source", "boards_discovered", "boards_queried", "boards_ok", "boards_partial", "boards_empty",
                "boards_not_found", "boards_failed", "boards_not_queried", "raw_records_worldwide",
                "germany_records_collected", "germany_unique_records", "germany_employers",
                "with_description", "urls_checked", "urls_working", "queried_successfully"]
    report = []
    for s in sorted(set(by_src_boards) | set(collected)):
        c = by_src_boards[s]
        queried = sum(c.values()) - c["not_queried"]
        report.append({
            "source": s, "boards_discovered": discovered.get(s) or (1 if c else 0), "boards_queried": queried,
            "boards_ok": c["ok"], "boards_partial": c["partial"], "boards_empty": c["empty"],
            "boards_not_found": c["not_found"], "boards_failed": c["failed"], "boards_not_queried": c["not_queried"],
            "raw_records_worldwide": raw_by_src[s], "germany_records_collected": collected[s],
            "germany_unique_records": uniq_by_src[s], "germany_employers": len(employers_by_src[s]),
            "with_description": desc_by_src[s],
            "urls_checked": checked_by_src[s], "urls_working": ok_by_src[s],
            "queried_successfully": "yes" if raw_by_src[s] > 0 else "no",
        })
    with open(out / "source_report.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=rep_cols)
        w.writeheader()
        w.writerows(report)
    ws2 = wb.create_sheet("source_report")
    ws2.append(rep_cols)
    for r in report:
        ws2.append([r[c] for c in rep_cols])
    wb.save(out / "all_jobs.xlsx")

    failed = [b for b in boards if b["status"] in ("failed", "partial", "not_found", "not_queried")]
    with open(out / "failed_boards.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["source", "board", "status", "records", "http_requests", "detail"],
                           extrasaction="ignore")
        w.writeheader()
        w.writerows(failed)
    with open(out / "boards_report.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(boards[0].keys()) if boards else ["source"])
        w.writeheader()
        w.writerows(boards)

    exec_errors = [f"{s['tag']}: {e}" for s in stats for e in s.get("errors", [])]
    list_rows = [dict(r, tag=s["tag"]) for s in stats for r in s.get("list_sources", [])]
    summary = {
        "generated_at": NOW_ISO,
        "raw_records_worldwide_total": sum(raw_by_src.values()),
        "germany_records_collected": sum(collected.values()),
        "germany_unique_after_dedup": len(rows),
        "duplicates_removed": len(normalized) - len(unique),
        "normalization_errors": sum(norm_errors.values()),
        "normalization_error_samples": dict(norm_errors.most_common(10)),
        "unique_employers_germany": len({r["employer"].lower() for r in rows}),
        "company_boards_discovered": sum(discovered.values()),
        "company_boards_queried": sum(sum(c.values()) - c["not_queried"] for c in by_src_boards.values()),
        "sources_with_records": sorted(s for s in raw_by_src if raw_by_src[s] > 0),
        "sources_without_records": sorted(s for s in by_src_boards if raw_by_src[s] == 0),
        "germany_unique_by_source": dict(uniq_by_src),
        "germany_by_match_type": dict(Counter(r["germany_match"] for r in rows)),
        "board_status_by_source": {s: dict(c) for s, c in by_src_boards.items()},
        "url_status": dict(Counter(r["url_status"] for r in rows)),
        "records_with_description": sum(desc_by_src.values()),
        "working_application_urls": sum(1 for r in rows if r["url_status"] == "ok"),
        "execution_errors": exec_errors,
        "list_sources": list_rows,
    }
    (out / "run_summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False))

    md = ["## Germany collection", "", f"- Raw records (worldwide, all boards): **{summary['raw_records_worldwide_total']}**",
          f"- Germany records collected: **{summary['germany_records_collected']}**",
          f"- Unique after dedup: **{summary['germany_unique_after_dedup']}**",
          f"- Unique employers: **{summary['unique_employers_germany']}**",
          f"- Boards discovered / queried: **{summary['company_boards_discovered']} / {summary['company_boards_queried']}**",
          f"- Records with a job description: **{summary['records_with_description']}**",
          f"- Working application URLs: **{summary['working_application_urls']}** ({summary['url_status']})",
          f"- Execution errors: {len(exec_errors)}", "", "| " + " | ".join(rep_cols) + " |",
          "|" + "---|" * len(rep_cols)]
    md += ["| " + " | ".join(str(r[c]) for c in rep_cols) + " |" for r in report]
    md += ["", "### Execution errors"] + [f"- {e}" for e in exec_errors or ["none"]]
    text = "\n".join(md) + "\n"
    (out / "run_summary.md").write_text(text)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as f:
            f.write(text)
    print(text)
    return 0


# --------------------------------------------------------------------------
# Description enrichment (Germany records only)
# The engine's Workday and SmartRecruiters collectors return no description:
# both list APIs omit it. One detail request per Germany record fills it in.
# --------------------------------------------------------------------------

_UA = {"User-Agent": "Mozilla/5.0 (compatible; jobforager/1.0)", "Accept": "application/json"}


def _get_json(url: str) -> Any:
    req = urllib.request.Request(url, headers=_UA)
    with urllib.request.urlopen(req, timeout=20) as resp:
        return json.loads(resp.read().decode("utf-8", "replace"))


def _enrich_workday(rec: dict[str, Any]) -> str:
    from jobforager.search.workday import _parse_workday_url

    meta = rec.get("metadata") or {}
    site, path = meta.get("career_site_url"), meta.get("external_path")
    if not site or not path or str(path).startswith("http"):
        return "no_path"
    data = _get_json(_parse_workday_url(site)["api_base_url"] + path)
    info = data.get("jobPostingInfo") or {}
    desc = info.get("jobDescription")
    if info.get("startDate"):
        rec["posted_at"] = info["startDate"]
    if info.get("location") and not rec.get("location"):
        rec["location"] = info["location"]
    if desc:
        rec["description"] = desc
        return "filled"
    return "no_description"


def _enrich_smartrecruiters(rec: dict[str, Any]) -> str:
    slug = (rec.get("metadata") or {}).get("company_slug")
    jid = rec.get("external_id")
    if not slug or not jid:
        return "no_path"
    data = _get_json(f"https://api.smartrecruiters.com/v1/companies/{slug}/postings/{jid}")
    sections = ((data.get("jobAd") or {}).get("sections") or {})
    parts = []
    for key in ("companyDescription", "jobDescription", "qualifications", "additionalInformation"):
        sec = sections.get(key) or {}
        if sec.get("text"):
            parts.append(f"<h3>{sec.get('title') or key}</h3>\n{sec['text']}")
    if data.get("applyUrl") and not rec.get("apply_url"):
        rec["apply_url"] = data["applyUrl"]
    if parts:
        rec["description"] = "\n".join(parts)
        return "filled"
    return "no_description"


def cmd_enrich(args: argparse.Namespace) -> int:
    handlers = {"workday": _enrich_workday, "smartrecruiters": _enrich_smartrecruiters}
    deadline = time.monotonic() + float(args.budget_min) * 60
    grand = Counter()
    for p in sorted(Path(args.inp).rglob("germany_*.jsonl.gz")):
        with gzip.open(p, "rt", encoding="utf-8") as f:
            recs = [json.loads(line) for line in f]
        todo = [r for r in recs if r.get("source") in handlers and not r.get("description")]
        if args.title_regex:
            rx = re.compile(args.title_regex, re.I)
            todo = [r for r in todo if rx.search(r.get("title") or "")]
        if not todo:
            continue
        outcome = Counter()

        def work(rec: dict[str, Any]) -> str:
            if time.monotonic() > deadline:
                return "not_attempted_time_budget"
            try:
                return handlers[rec["source"]](rec)
            except urllib.error.HTTPError as exc:
                return f"http_{exc.code}"
            except Exception as exc:
                return f"error_{type(exc).__name__}"

        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            for n, res in enumerate(ex.map(work, todo), 1):
                outcome[res] += 1
                if n % 2000 == 0:
                    print(f"[enrich] {p.name} {n}/{len(todo)} {dict(outcome)}", flush=True)
        with gzip.open(p, "wt", encoding="utf-8") as f:
            for r in recs:
                f.write(json.dumps(r, ensure_ascii=False, default=_json_default) + "\n")
        print(f"[enrich] {p.name}: {len(todo)} records without description -> {dict(outcome)}", flush=True)
        for k, v in outcome.items():
            grand[f"{todo[0]['source']}:{k}"] += v
    print("[enrich] total", json.dumps(dict(grand), indent=1))
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("collect")
    c.add_argument("--source", required=True)
    c.add_argument("--shard", default="")
    c.add_argument("--out", required=True)
    c.add_argument("--workers", type=int, default=24)
    c.add_argument("--limit", type=int, default=0)
    c.add_argument("--budget-min", type=float, default=320)
    m = sub.add_parser("merge")
    m.add_argument("--in", dest="inp", required=True)
    m.add_argument("--out", required=True)
    m.add_argument("--workers", type=int, default=32)
    m.add_argument("--validate-budget-min", type=float, default=240)
    m.add_argument("--no-validate", action="store_true")
    e = sub.add_parser("enrich")
    e.add_argument("--in", dest="inp", required=True)
    e.add_argument("--workers", type=int, default=24)
    e.add_argument("--budget-min", type=float, default=150)
    e.add_argument("--title-regex", default="", help="only enrich records whose title matches")
    args = ap.parse_args()
    return {"collect": cmd_collect, "merge": cmd_merge, "enrich": cmd_enrich}[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())
