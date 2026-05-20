#!/usr/bin/env python3
"""Exercise Workday OAuth access-token cache expiry and refresh against a live token URL.

Uses WorkdayOAuthTokenProvider with short cache windows so each iteration is likely to
call the token endpoint again without waiting for Workday's full access-token lifetime.

Examples:
  poetry run python scripts/probe_oauth_token_refresh.py -c sample_config.oauth.json
  poetry run python scripts/probe_oauth_token_refresh.py -c sample_config.oauth.json --iterations 5 --sleep 12
  poetry run python scripts/probe_oauth_token_refresh.py -c sample_config.oauth.json --force-refresh
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any, Callable, Dict

import requests

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from tap_workday_raas.oauth_middleware import (  # noqa: E402
    WorkdayOAuthError,
    WorkdayOAuthTokenProvider,
    raas_config_uses_oauth,
)

_DEFAULT_CONFIG = _ROOT / "sample_config.oauth.json"


def _read_config(path: Path) -> Dict[str, Any]:
    if not path.is_file():
        raise FileNotFoundError(f"Config not found: {path}")
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _install_token_post_counter(provider: WorkdayOAuthTokenProvider) -> Callable[..., requests.Response]:
    """Wrap provider HTTP post to count token-endpoint calls."""
    original_post = provider._http.post
    counter = {"count": 0}

    def counting_post(*args: Any, **kwargs: Any) -> requests.Response:
        counter["count"] += 1
        return original_post(*args, **kwargs)

    provider._http.post = counting_post  # type: ignore[method-assign]
    return lambda: counter["count"]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Probe OAuth access-token refresh by calling get_access_token() repeatedly."
    )
    parser.add_argument(
        "-c",
        "--config",
        default=str(_DEFAULT_CONFIG),
        help=f"Path to tap config JSON (default: {_DEFAULT_CONFIG.name}).",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=5,
        help="Number of get_access_token() calls (default: 5).",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=12.0,
        help="Seconds to sleep between iterations (default: 12).",
    )
    parser.add_argument(
        "--cache-seconds",
        type=int,
        default=None,
        help=(
            "Target access-token reuse window in seconds (sets leeway = expires_in - N). "
            "Assumes Workday expires_in=3600 unless you also pass --expires-in."
        ),
    )
    parser.add_argument(
        "--expires-in",
        type=int,
        default=3600,
        dest="expires_in_assume",
        help="Used with --cache-seconds to compute leeway (default: 3600).",
    )
    parser.add_argument(
        "--leeway",
        type=int,
        default=3590,
        help=(
            "oauth_access_token_refresh_leeway_seconds (subtracted from expires_in). "
            "NOT 'seconds to cache': high leeway (e.g. 3590) = ~10s cache when expires_in=3600."
        ),
    )
    parser.add_argument(
        "--min-cache",
        type=int,
        default=0,
        dest="min_cache",
        help="oauth_access_token_min_cache_seconds override (default: 0).",
    )
    parser.add_argument(
        "--force-refresh",
        action="store_true",
        help="Call force_refresh() each iteration instead of relying on cache expiry only.",
    )
    parser.add_argument(
        "--use-config-cache-settings",
        action="store_true",
        help="Do not override oauth_access_token_refresh_leeway_seconds / min_cache_seconds.",
    )
    args = parser.parse_args()

    if args.iterations < 1:
        print("--iterations must be at least 1.", file=sys.stderr)
        return 1
    if args.sleep < 0:
        print("--sleep must be non-negative.", file=sys.stderr)
        return 1

    config = _read_config(Path(args.config))
    if not raas_config_uses_oauth(config):
        print("Config must use OAuth (client_id, client_secret, token_url).", file=sys.stderr)
        return 1

    probe_config = dict(config)
    if not args.use_config_cache_settings:
        leeway = args.leeway
        min_cache = args.min_cache
        if args.cache_seconds is not None:
            if args.cache_seconds < 0:
                print("--cache-seconds must be non-negative.", file=sys.stderr)
                return 1
            leeway = max(0, args.expires_in_assume - args.cache_seconds)
            min_cache = 0
        probe_config["oauth_access_token_refresh_leeway_seconds"] = leeway
        probe_config["oauth_access_token_min_cache_seconds"] = min_cache

    verify = not bool(probe_config.get("disable_ssl_verification", True))
    provider = WorkdayOAuthTokenProvider.from_config(probe_config, verify=verify)
    token_post_count = _install_token_post_counter(provider)

    leeway = int(probe_config.get("oauth_access_token_refresh_leeway_seconds", 60))
    min_cache = int(probe_config.get("oauth_access_token_min_cache_seconds", 60))
    assumed_expires = args.expires_in_assume
    planned_cache = max(min_cache, assumed_expires - leeway)
    print(f"Config: {args.config}")
    print(f"Token URL: {probe_config.get('token_url')}")
    print(f"Grant type: {probe_config.get('oauth_grant_type', 'client_credentials')}")
    print(f"TLS verify: {verify}")
    print(
        f"Cache settings: leeway={leeway}s, min_cache={min_cache}s "
        f"→ planned reuse ~{planned_cache}s if expires_in={assumed_expires}"
    )
    print(f"Mode: {'force_refresh each iteration' if args.force_refresh else 'get_access_token only'}")
    print(f"Iterations: {args.iterations}, sleep: {args.sleep}s")
    print("")

    prev_token: str | None = None
    try:
        for i in range(args.iterations):
            posts_before = token_post_count()
            if args.force_refresh:
                token = provider.force_refresh()
            else:
                token = provider.get_access_token()
            posts_after = token_post_count()
            new_posts = posts_after - posts_before

            changed = prev_token is not None and token != prev_token
            cache_left = max(0.0, provider._expires_at - time.time())
            print(
                f"[{i + 1}/{args.iterations}] token_posts={posts_after} "
                f"(+{new_posts} this step) cache_valid_for={cache_left:.1f}s "
                f"token_changed={changed} prefix={token[:16]}..."
            )
            prev_token = token

            if i + 1 < args.iterations and args.sleep > 0:
                time.sleep(args.sleep)
    except WorkdayOAuthError as exc:
        print(f"\nToken error: {exc}", file=sys.stderr)
        if exc.response_body:
            print(f"Response body: {exc.response_body[:500]}", file=sys.stderr)
        return 2
    except requests.RequestException as exc:
        print(f"\nHTTP error: {exc}", file=sys.stderr)
        return 2

    total_posts = token_post_count()
    print("")
    print(f"Done. Total token endpoint POSTs: {total_posts}")
    if total_posts < 2 and not args.force_refresh:
        print(
            "Hint: use --cache-seconds 10 --sleep 12 (not --leeway 10), or raise --sleep above "
            "the planned reuse window so get_access_token() runs again on the same provider."
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
