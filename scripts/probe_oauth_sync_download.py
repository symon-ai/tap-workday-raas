#!/usr/bin/env python3
"""Download RaaS report data using tap_workday_raas (config + catalog), with HTTP metrics.

Runs the same path as sync: stream_report (and optionally sync_report transform) for each
catalog stream that matches a report in config. Counts token POSTs vs report GETs so you
can verify one streaming download per stream.

Examples:
  poetry run python scripts/probe_oauth_sync_download.py -c sample_config.oauth.json
  poetry run python scripts/probe_oauth_sync_download.py -c sample_config.oauth.json --catalog catalog.json
  poetry run python scripts/probe_oauth_sync_download.py -c sample_config.oauth.json --stream DanielTest2 --max-records 10
  poetry run python scripts/probe_oauth_sync_download.py -c sample_config.oauth.json --repeat 2 --sleep 15
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

import requests
from singer.catalog import Catalog

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import tap_workday_raas.client as client_mod  # noqa: E402
from tap_workday_raas.client import stream_report  # noqa: E402
from tap_workday_raas.oauth_middleware import (  # noqa: E402
    WorkdayOAuthError,
    WorkdayOAuthTokenProvider,
    raas_config_uses_oauth,
    validate_raas_tap_config,
)
from tap_workday_raas.symon_exception import SymonException  # noqa: E402
from tap_workday_raas.sync import sync_report  # noqa: E402

_DEFAULT_CONFIG = _ROOT / "sample_config.oauth.json"
_DEFAULT_CATALOG = _ROOT / "catalog.json"

# Optional reuse across --repeat runs (see --reuse-provider).
_SHARED_OAUTH: Dict[str, Any] = {"session": None, "provider": None}


class _HttpMetrics:
    def __init__(self) -> None:
        self.token_posts = 0
        self.report_gets = 0

    def as_dict(self) -> Dict[str, int]:
        return {
            "token_posts": self.token_posts,
            "report_gets": self.report_gets,
        }


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.is_file():
        raise FileNotFoundError(f"File not found: {path}")
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _wrap_provider_token_post(provider: WorkdayOAuthTokenProvider, metrics: _HttpMetrics) -> None:
    original_post = provider._http.post

    def counting_post(*args: Any, **kwargs: Any) -> requests.Response:
        metrics.token_posts += 1
        return original_post(*args, **kwargs)

    provider._http.post = counting_post  # type: ignore[method-assign]


def _wrap_session_report_get(session: requests.Session, metrics: _HttpMetrics) -> None:
    original_get = session.get

    def counting_get(url: str, *args: Any, **kwargs: Any) -> requests.Response:
        url_str = str(url)
        if "customreport2" in url_str or "customreport" in url_str:
            metrics.report_gets += 1
        return original_get(url, *args, **kwargs)

    session.get = counting_get  # type: ignore[method-assign]


def _install_http_counters(metrics: _HttpMetrics, *, reuse_provider: bool) -> None:
    """Mirror client._session_for_config but count token POST before apply_to_session."""

    def wrapped(config: Dict[str, Any]) -> Tuple[requests.Session, Any]:
        if reuse_provider and _SHARED_OAUTH["provider"] is not None:
            session = _SHARED_OAUTH["session"]
            provider = _SHARED_OAUTH["provider"]
            provider.apply_to_session(session)
            return session, provider

        disable_ssl = config.get("disable_ssl_verification", True)
        verify = not disable_ssl
        session = requests.Session()
        if disable_ssl:
            session.verify = False
        if raas_config_uses_oauth(config):
            provider = WorkdayOAuthTokenProvider.from_config(config, verify=verify)
            _wrap_provider_token_post(provider, metrics)
            provider.apply_to_session(session)
            _wrap_session_report_get(session, metrics)
            if reuse_provider:
                _SHARED_OAUTH["session"] = session
                _SHARED_OAUTH["provider"] = provider
            return session, provider
        session.auth = (config["username"], config["password"])
        _wrap_session_report_get(session, metrics)
        return session, None

    client_mod._session_for_config = wrapped  # type: ignore[assignment]


def _streams_for_run(catalog: Catalog, config: Dict[str, Any], stream_filter: str | None):
    reports = {r["report_name"]: r for r in config.get("reports", [])}
    state: Dict[str, Any] = {}
    selected = list(catalog.get_selected_streams(state))
    if selected:
        streams = selected
    else:
        streams = [s for s in catalog.streams if s.tap_stream_id in reports]
    if stream_filter:
        streams = [s for s in streams if s.tap_stream_id == stream_filter]
    missing = [s.tap_stream_id for s in streams if s.tap_stream_id not in reports]
    if missing:
        raise ValueError(
            "No report in config for stream(s): {}. config.reports names: {}".format(
                ", ".join(missing), ", ".join(sorted(reports))
            )
        )
    if not streams:
        raise ValueError(
            "No streams to sync. Select streams in catalog metadata (selected: true) "
            "or ensure catalog stream ids match config.reports[].report_name."
        )
    return streams, reports


def _download_stream(
    report: Dict[str, str],
    stream,
    config: Dict[str, Any],
    *,
    use_sync_report: bool,
    max_records: int | None,
    sample_out: Path | None,
) -> int:
    if use_sync_report:
        import io
        import contextlib

        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            count = sync_report(report, stream, config)
        if max_records is not None and count > max_records:
            print(
                f"  (sync_report wrote {count} Singer records; --max-records only applies to stream_report mode)",
                file=sys.stderr,
            )
        return count

    count = 0
    samples: List[Dict[str, Any]] = []
    for record in stream_report(report["report_url"], config):
        count += 1
        if sample_out is not None and len(samples) < 5:
            samples.append(record)
        if max_records is not None and count >= max_records:
            break

    if sample_out is not None and samples:
        with open(sample_out, "w", encoding="utf-8") as fp:
            json.dump(samples, fp, indent=2)
    return count


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Download RaaS reports via tap_workday_raas using config + catalog."
    )
    parser.add_argument("-c", "--config", default=str(_DEFAULT_CONFIG), help="Tap config JSON.")
    parser.add_argument(
        "--catalog",
        default=str(_DEFAULT_CATALOG),
        help=f"Catalog JSON (default: {_DEFAULT_CATALOG.name}).",
    )
    parser.add_argument(
        "--stream",
        default=None,
        help="Only sync this tap_stream_id (must match report_name in config).",
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=None,
        help="Stop after N records per stream (stream_report mode only).",
    )
    parser.add_argument(
        "--use-sync-report",
        action="store_true",
        help="Call sync_report (Singer messages to stdout) instead of stream_report.",
    )
    parser.add_argument(
        "--sample-out",
        default=None,
        help="Write first few raw records to this JSON file (stream_report mode).",
    )
    parser.add_argument(
        "--repeat",
        type=int,
        default=1,
        help="Run the full catalog sync N times (e.g. 2 to see token refresh between runs).",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.0,
        help="Seconds to sleep between --repeat runs.",
    )
    parser.add_argument(
        "--cache-seconds",
        type=int,
        default=None,
        help="Target token reuse window; sets leeway=expires_in-N and min_cache=0.",
    )
    parser.add_argument(
        "--expires-in",
        type=int,
        default=3600,
        dest="expires_in_assume",
        help="Assumed expires_in when using --cache-seconds (default 3600).",
    )
    parser.add_argument(
        "--leeway",
        type=int,
        default=None,
        help="oauth_access_token_refresh_leeway_seconds (high value = short cache, e.g. 3590 ≈ 10s).",
    )
    parser.add_argument(
        "--min-cache",
        type=int,
        default=None,
        dest="min_cache",
        help="oauth_access_token_min_cache_seconds (default 60 in tap if unset).",
    )
    parser.add_argument(
        "--reuse-provider",
        action="store_true",
        help="Reuse one OAuth provider across --repeat runs so leeway affects token POST count.",
    )
    args = parser.parse_args()

    if args.repeat < 1:
        print("--repeat must be at least 1.", file=sys.stderr)
        return 1

    config = _read_json(Path(args.config))
    validate_raas_tap_config(config)
    if args.cache_seconds is not None:
        if args.cache_seconds < 0:
            print("--cache-seconds must be non-negative.", file=sys.stderr)
            return 1
        config["oauth_access_token_refresh_leeway_seconds"] = max(
            0, args.expires_in_assume - args.cache_seconds
        )
        config["oauth_access_token_min_cache_seconds"] = 0
    else:
        if args.leeway is not None:
            config["oauth_access_token_refresh_leeway_seconds"] = args.leeway
        if args.min_cache is not None:
            config["oauth_access_token_min_cache_seconds"] = args.min_cache

    catalog_dict = _read_json(Path(args.catalog))
    catalog = Catalog.from_dict(catalog_dict)
    streams, reports = _streams_for_run(catalog, config, args.stream)

    verify = not bool(config.get("disable_ssl_verification", True))
    print(f"Config: {args.config}")
    print(f"Catalog: {args.catalog}")
    print(f"Streams: {[s.tap_stream_id for s in streams]}")
    print(f"Auth: {'oauth' if raas_config_uses_oauth(config) else 'basic'}")
    print(f"TLS verify: {verify}")
    print(f"Mode: {'sync_report' if args.use_sync_report else 'stream_report'}")
    if args.max_records is not None:
        print(f"Max records per stream: {args.max_records}")
    print(f"Repeat: {args.repeat}")
    if raas_config_uses_oauth(config):
        leeway = int(config.get("oauth_access_token_refresh_leeway_seconds", 60))
        min_cache = int(config.get("oauth_access_token_min_cache_seconds", 60))
        planned = max(min_cache, args.expires_in_assume - leeway)
        print(
            f"OAuth cache: leeway={leeway}s, min_cache={min_cache}s "
            f"→ ~{planned}s reuse if expires_in={args.expires_in_assume}"
        )
        if args.repeat > 1 and not args.reuse_provider:
            print(
                "Note: without --reuse-provider, each --repeat run creates a new provider "
                "and always fetches a token once (leeway ignored between runs)."
            )
        if args.repeat == 1:
            print(
                "Note: one stream_report calls get_access_token once at start; leeway does not "
                "refresh mid-download. Use probe_oauth_token_refresh.py or --reuse-provider --repeat."
            )
    print("")

    sample_path = Path(args.sample_out) if args.sample_out else None
    total_records = 0

    try:
        for run_idx in range(args.repeat):
            if args.repeat > 1:
                print(f"=== Run {run_idx + 1}/{args.repeat} ===")
            metrics = _HttpMetrics()
            _install_http_counters(metrics, reuse_provider=args.reuse_provider)
            run_started = time.time()

            for stream in streams:
                report = reports[stream.tap_stream_id]
                print(f"Downloading {stream.tap_stream_id} ...")
                print(f"  URL: {report['report_url']}")
                stream_started = time.time()
                before = dict(metrics.as_dict())

                n = _download_stream(
                    report,
                    stream,
                    config,
                    use_sync_report=args.use_sync_report,
                    max_records=args.max_records,
                    sample_out=sample_path,
                )
                elapsed = time.time() - stream_started
                delta_token = metrics.token_posts - before["token_posts"]
                delta_get = metrics.report_gets - before["report_gets"]
                total_records += n
                print(
                    f"  Records: {n} in {elapsed:.1f}s | "
                    f"token POSTs: +{delta_token} (total {metrics.token_posts}) | "
                    f"report GETs: +{delta_get} (total {metrics.report_gets})"
                )
                if sample_path is not None:
                    print(f"  Sample records: {sample_path}")

            print(
                f"Run {run_idx + 1} total: {time.time() - run_started:.1f}s, "
                f"metrics={metrics.as_dict()}"
            )
            if args.repeat > 1 and run_idx + 1 < args.repeat and args.sleep > 0:
                print(f"Sleeping {args.sleep}s ...")
                time.sleep(args.sleep)
            print("")

        print(f"Done. Total records: {total_records}")
        return 0

    except (WorkdayOAuthError, SymonException) as exc:
        print(f"\nWorkday error: {exc}", file=sys.stderr)
        return 2
    except requests.RequestException as exc:
        print(f"\nHTTP error: {exc}", file=sys.stderr)
        return 2
    except ValueError as exc:
        print(f"\nConfig/catalog error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
