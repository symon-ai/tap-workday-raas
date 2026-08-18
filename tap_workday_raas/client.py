import re
from urllib.parse import urlparse

import requests
import ijson.backends.yajl2_c as ijson
import ijson as ijson_core
from tap_workday_raas.symon_exception import SymonException
from tap_workday_raas.oauth_middleware import (
    WorkdayOAuthError,
    WorkdayOAuthTokenProvider,
    raas_config_uses_oauth,
    workday_oauth_error_details,
)


# Native Workday REST bases look like .../ccx/api/v1/{tenant} (version may vary).
_REST_API_PATH_RE = re.compile(
    r"^(?P<prefix>.*?)/ccx/api(?:/v\d+)?(?:/(?P<tenant>[^/]+))?$",
    re.IGNORECASE,
)

# RaaS report paths are .../ccx/service/customreport2/{tenant}/{owner}/{report}.
_SERVICE_PATH_MARKER = "/ccx/service/"


def resolve_raas_service_root(api_base_url):
    """Derive the RaaS service root (`.../ccx/service`) from ``api_base_url``.

    Native Workday REST endpoints (`.../ccx/api/v1/{tenant}`) are converted to the
    service path on the same origin (preserving any proxy path prefix before
    `/ccx/api`). Opaque customer proxy bases append `/ccx/service`.
    """
    raw = (api_base_url or "").strip().rstrip("/")
    parsed = urlparse(raw)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise ValueError(
            "api_base_url must be a valid http(s) URL, got: {!r}".format(api_base_url)
        )

    path = (parsed.path or "").rstrip("/")
    match = _REST_API_PATH_RE.match(path)
    if match:
        prefix = match.group("prefix") or ""
        return "{}://{}{}/ccx/service".format(parsed.scheme, parsed.netloc, prefix)

    return "{}://{}{}/ccx/service".format(parsed.scheme, parsed.netloc, path)


def _raas_report_path_fragments(report_url):
    """Return (path_after_service, query, fragment) from a full RaaS report URL.

    Expected path shape: ``/ccx/service/customreport2/{tenant}/{owner}/{report}``.
    """
    parsed = urlparse(report_url)
    path = parsed.path or ""
    idx = path.lower().find(_SERVICE_PATH_MARKER)
    if idx < 0:
        raise ValueError(
            "report_url must contain {} path when api_base_url is set, got: {!r}".format(
                _SERVICE_PATH_MARKER.rstrip("/"),
                report_url,
            )
        )
    suffix = path[idx + len(_SERVICE_PATH_MARKER) :]
    if not suffix:
        raise ValueError(
            "report_url is missing path fragments after {}, got: {!r}".format(
                _SERVICE_PATH_MARKER.rstrip("/"),
                report_url,
            )
        )
    query = "?" + parsed.query if parsed.query else ""
    fragment = "#" + parsed.fragment if parsed.fragment else ""
    return suffix, query, fragment


def resolve_raas_request_url(url, config):
    """Resolve the URL used for a RaaS report/XSD request.

    When ``api_base_url`` is absent or blank, the configured ``report_url`` is
    used as-is (no host rewriting). When ``api_base_url`` is set, the request
    URL is built from that base plus the expected ``/ccx/service/...`` path
    fragments taken from ``report_url`` — the report URL host is ignored.
    """
    api_base_url = (config.get("api_base_url") or "").strip()
    if not api_base_url:
        return url

    service_root = resolve_raas_service_root(api_base_url)
    suffix, query, fragment = _raas_report_path_fragments(url)
    return "{}/{}{}{}".format(service_root, suffix, query, fragment)


def _session_for_config(config):
    """Build a requests.Session with basic auth or OAuth Bearer, and optional OAuth provider for retry."""
    disable_ssl = config.get("disable_ssl_verification", False)
    verify = not disable_ssl
    session = requests.Session()
    if disable_ssl:
        session.verify = False
    if raas_config_uses_oauth(config):
        provider = WorkdayOAuthTokenProvider.from_config(config, verify=verify)
        provider.apply_to_session(session)
        return session, provider
    session.auth = (config["username"], config["password"])
    return session, None


WORKDAY_OAUTH_TOKEN_REQUEST_FAILED_MESSAGE = (
    "The Workday OAuth token request failed. Check the token configuration and expiration."
)


def _wrap_oauth_error(exc):
    return SymonException(
        WORKDAY_OAUTH_TOKEN_REQUEST_FAILED_MESSAGE,
        "workday.OAuthError",
        workday_oauth_error_details(exc),
    )


def stream_report(report_url, config):
    # Force the format query param to be set to format=json

    try:
        request_url = resolve_raas_request_url(report_url, config)
    except ValueError as e:
        raise SymonException(str(e), "workday.InvalidConfig")

    # Split query params off
    url_breakdown = request_url.split("?")

    # Gather all params that are not format
    if len(url_breakdown) == 1:
        params = []
    else:
        params = [x for x in url_breakdown[1].split("&") if not x.startswith("format=")]

    # Add the format param
    params.append("format=json")
    param_string = "&".join(params)

    # Put the url back together
    corrected_url = url_breakdown[0] + "?" + param_string

    try:
        session, oauth_provider = _session_for_config(config)
    except ValueError as e:
        raise SymonException(str(e), "workday.InvalidConfig")
    except WorkdayOAuthError as e:
        raise _wrap_oauth_error(e)

    try:
        with session.get(corrected_url, stream=True) as resp:
            if resp.status_code == 401 and oauth_provider is not None:
                oauth_provider.force_refresh()
                resp.close()
                with session.get(corrected_url, stream=True) as resp2:
                    resp2.raise_for_status()
                    yield from _iter_report_json(resp2)
            else:
                resp.raise_for_status()
                yield from _iter_report_json(resp)
    except WorkdayOAuthError as e:
        raise _wrap_oauth_error(e)
    except requests.exceptions.HTTPError as e:
        if "401 Client Error: Unauthorized for url" in str(e) or (
            e.response is not None and e.response.status_code == 401
        ):
            if oauth_provider is not None:
                raise SymonException(
                    "Report returned 401 Unauthorized. Check OAuth token, scopes, and report access.",
                    "workday.OAuthUnauthorized",
                )
            raise SymonException(
                "The username or password provided is incorrect. Please check and try again",
                "workday.InvalidUsernameOrPassword",
            )
        if e.response is not None and e.response.text:
            raise SymonException(
                "Import failed with the following WorkdayRaaS error: {}".format(e.response.text),
                "workday.WorkdayApiError",
            )
        raise
    except requests.exceptions.ConnectionError as e:
        message = str(e)
        display_url = corrected_url if (config.get("api_base_url") or "").strip() else report_url
        if "nodename nor servname provided, or not known" in message or "Name or service not known" in message:
            raise SymonException(
                'The report URL "{}" was not found. Please check the report URL and try again.'.format(display_url),
                "workdayRaaS.WorkdayRaaSInvalidReportURL",
            )
        raise SymonException(
            'Sorry, we couldn\'t connect to the specified report URL "{}". Please ensure all the connection form values are correct.'.format(
                display_url
            ),
            "workday.ConnectionFailed",
        )


def _iter_report_json(resp):
    # Set up our search key
    report_entry_key = b"Report_Entry"
    search_prefix = report_entry_key.decode("utf-8") + ".item"

    # NB This creates a "push" style interface with the ijson iterable
    # parser This sendable_list will be populated with intermediate
    # values by the items_coro() when send() is called. The
    # sendable_list must then be purged of values before it can be
    # used again. We have an explicit check for whether we find the
    # 'Report_Entry' key because if we do not find it the parser
    # yields 0 records instead of failing and this allows us to know
    # if the schema is changed
    records = ijson_core.sendable_list()
    coro = ijson.items_coro(records, search_prefix)

    found_key = False
    for chunk in resp.iter_content(chunk_size=512):
        if report_entry_key in chunk:
            found_key = True
        coro.send(chunk)
        for rec in records:
            yield rec
        del records[:]

    if not found_key:
        raise Exception(
            "Did not see '{}' key in response. Report does not conform to expected schema, failing.".format(
                report_entry_key
            )
        )

    coro.close()


def download_xsd(report_url, config, session=None, oauth_provider=None):
    try:
        request_url = resolve_raas_request_url(report_url, config)
    except ValueError as e:
        raise SymonException(str(e), "workday.InvalidConfig")

    if "?" in request_url:
        xsds_url = request_url.split("?")[0] + "?xsds"
    else:
        xsds_url = request_url + "?xsds"

    if session is None:
        try:
            session, oauth_provider = _session_for_config(config)
        except ValueError as e:
            raise SymonException(str(e), "workday.InvalidConfig")
        except WorkdayOAuthError as e:
            raise _wrap_oauth_error(e)

    def _fetch():
        return session.get(xsds_url)

    try:
        response = _fetch()
        if response.status_code == 401 and oauth_provider is not None:
            oauth_provider.force_refresh()
            response = _fetch()
        response.raise_for_status()
    except WorkdayOAuthError as e:
        raise _wrap_oauth_error(e)
    except requests.exceptions.HTTPError as e:
        if '500 Server Error: Internal Server Error for url' in str(e):
            raise SymonException("Sorry, we couldn't access your report. Verify your report URL and name, and ensure your Workday account permissions and security settings allow access. If the issue persists, contact your Workday administrator.",
                                 'workdayRaaS.WorkdayRaaSInvalidReportURL')
        if e.response is not None and e.response.text:
            raise SymonException(
                "Import failed with the following WorkdayRaaS error: {}".format(e.response.text),
                "workday.WorkdayApiError",
            )
        raise
    except requests.exceptions.ConnectionError as e:
        message = str(e)
        display_url = xsds_url if (config.get("api_base_url") or "").strip() else report_url
        if "nodename nor servname provided, or not known" in message or "Name or service not known" in message:
            raise SymonException(
                "The report URL {} was not found. Please check the report URL and try again.".format(display_url),
                "workdayRaaS.WorkdayRaaSInvalidReportURL",
            )
        raise SymonException(
            'Sorry, we couldn\'t connect to the specified report URL "{}". Please ensure all the connection form values are correct.'.format(
                display_url
            ),
            "workday.ConnectionFailed",
        )

    return response.text
