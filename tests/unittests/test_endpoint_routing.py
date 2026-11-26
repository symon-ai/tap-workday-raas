"""Use real OAuth, report parsing and requests routing; mock only HTTP transport."""
import importlib.util
import json
import sys
import types
import unittest
from pathlib import Path
from unittest import mock

import requests

_ROOT = Path(__file__).resolve().parents[2]


def _load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


_MODULE_NAMES = (
    "tap_workday_raas", "tap_workday_raas.symon_exception",
    "tap_workday_raas.oauth_middleware", "tap_workday_raas.client",
)
_previous_modules = {name: sys.modules.get(name) for name in _MODULE_NAMES}
try:
    sys.modules.setdefault("tap_workday_raas", types.ModuleType("tap_workday_raas"))
    _load_module("tap_workday_raas.symon_exception", _ROOT / "tap_workday_raas" / "symon_exception.py")
    oauth = _load_module("tap_workday_raas.oauth_middleware", _ROOT / "tap_workday_raas" / "oauth_middleware.py")
    client = _load_module("tap_workday_raas.client", _ROOT / "tap_workday_raas" / "client.py")
finally:
    for _name, _previous in _previous_modules.items():
        if _previous is None:
            sys.modules.pop(_name, None)
        else:
            sys.modules[_name] = _previous

REPORT_PATH = "/ccx/service/customreport2/tenant/owner/Report_Name"
REPORT = "https://vendor.workday.com" + REPORT_PATH + "?region=A%2FB&format=xml"


def response(request, body, status=200):
    result = requests.Response()
    result.status_code = status
    result.url = request.url
    result.request = request
    result._content = body
    result._content_consumed = True
    return result


class TestEndpointRouting(unittest.TestCase):
    def config(self, **kwargs):
        return dict(auth_type="oauth", client_id="id", client_secret="secret",
                    token_url="https://customer.example/oauth/token", **kwargs)

    def test_schema_and_stream_use_returned_endpoint_and_refresh_transition(self):
        for discover in (False, True):
            with self.subTest(discover=discover):
                seen = []
                tokens = iter([
                    {"access_token": "opaque-not-a-jwt", "instance_url": "https://first.example/proxy"},
                    {"access_token": "opaque-refreshed", "service_url": "https://second.example/new-prefix"},
                ])
                gets = 0

                def send(adapter, request, **kwargs):
                    nonlocal gets
                    seen.append((request.method, request.url, request.headers.get("Authorization"), kwargs["verify"]))
                    if request.method == "POST":
                        return response(request, json.dumps(next(tokens)).encode())
                    gets += 1
                    body = b"<schema/>" if discover else b'{"Report_Entry": [{"row_id": "1"}]}'
                    return response(request, b"" if gets == 1 else body, 401 if gets == 1 else 200)

                with mock.patch.object(requests.adapters.HTTPAdapter, "send", new=send):
                    config = self.config(base_url="https://configured.example/original")
                    if discover:
                        report = "https://configured.example/original" + REPORT_PATH + "?region=A%2FB&format=xml"
                        self.assertEqual(client.download_xsd(report, config), "<schema/>")
                    else:
                        report = "https://configured.example/original" + REPORT_PATH + "?region=A%2FB&format=xml"
                        self.assertEqual(list(client.stream_report(report, config)), [{"row_id": "1"}])
                query = "?region=A%2FB&xsds" if discover else "?region=A%2FB&format=json"
                self.assertEqual([(method, url) for method, url, _, _ in seen], [
                    ("POST", "https://customer.example/oauth/token"),
                    ("GET", "https://first.example/proxy" + REPORT_PATH + query),
                    ("POST", "https://customer.example/oauth/token"),
                    ("GET", "https://second.example/new-prefix" + REPORT_PATH + query),
                ])
                self.assertEqual(seen[1][2], "Bearer opaque-not-a-jwt")
                self.assertEqual(seen[3][2], "Bearer opaque-refreshed")
                self.assertTrue(all(verify is True for _, _, _, verify in seen))

    def test_defaults_and_configured_prefix_basic_and_seeded_oauth(self):
        for oauth_enabled in (False, True):
            for base in (None, "https://custom.example/prefix/"):
                for discover in (False, True):
                    with self.subTest(oauth=oauth_enabled, base=base, discover=discover):
                        seen = []
                        def send(adapter, request, **kwargs):
                            seen.append((request.url, request.headers.get("Authorization"), kwargs["verify"]))
                            body = b"<schema/>" if discover else b'{"Report_Entry": [{"row_id": "1"}]}'
                            return response(request, body)
                        config = self.config(base_url=base, access_token="opaque-seeded") if oauth_enabled else {
                            "auth_type": "basic", "username": "user", "password": "secret", "base_url": base,
                        }
                        with mock.patch.object(requests.adapters.HTTPAdapter, "send", new=send):
                            if discover:
                                self.assertEqual(client.download_xsd(REPORT, config), "<schema/>")
                            else:
                                self.assertEqual(list(client.stream_report(REPORT, config)), [{"row_id": "1"}])
                        prefix = "https://custom.example/prefix" if base else "https://vendor.workday.com"
                        query = "?region=A%2FB&xsds" if discover else "?region=A%2FB&format=json"
                        self.assertEqual(seen[0][0], prefix + REPORT_PATH + query)
                        self.assertEqual(seen[0][1], "Bearer opaque-seeded" if oauth_enabled else "Basic dXNlcjpzZWNyZXQ=")
                        self.assertIs(seen[0][2], True)

    def test_preserves_nonstandard_report_path_and_does_not_duplicate_prefix(self):
        session = oauth.WorkdayRoutingSession("https://custom.example/prefix")
        seen = []
        def send(adapter, request, **kwargs):
            seen.append(request.url)
            return response(request, b"ok")
        with mock.patch.object(requests.adapters.HTTPAdapter, "send", new=send):
            session.get("https://vendor.example/special/ccx/report?filter=1")
            session.get("https://custom.example/prefix/custom/report?filter=2")
        self.assertEqual(seen, [
            "https://custom.example/prefix/special/ccx/report?filter=1",
            "https://custom.example/prefix/custom/report?filter=2",
        ])

    def test_platform_endpoint_preserves_configured_report_prefix(self):
        for discover in (False, True):
            with self.subTest(discover=discover):
                seen = []
                def send(adapter, request, **kwargs):
                    seen.append((request.url, request.headers.get("Authorization"), kwargs["verify"]))
                    body = b"<schema/>" if discover else b'{"Report_Entry": [{"row_id": "1"}]}'
                    return response(request, body)
                config = self.config(
                    base_url="https://configured.example/original",
                    service_url="https://returned.example/proxy", access_token="opaque-seeded")
                report = "https://configured.example/original" + REPORT_PATH + "?region=A%2FB&format=xml"
                with mock.patch.object(requests.adapters.HTTPAdapter, "send", new=send):
                    if discover:
                        self.assertEqual(client.download_xsd(report, config), "<schema/>")
                    else:
                        self.assertEqual(list(client.stream_report(report, config)), [{"row_id": "1"}])
                query = "?region=A%2FB&xsds" if discover else "?region=A%2FB&format=json"
                self.assertEqual(seen, [
                    ("https://returned.example/proxy" + REPORT_PATH + query, "Bearer opaque-seeded", True),
                ])

    def test_seeded_endpoint_and_prepared_resends_preserve_resource_path(self):
        for first in ("https://first.example/proxy", "https://configured.example/original/proxy"):
            with self.subTest(first=first):
                seen = []
                def send(adapter, request, **kwargs):
                    seen.append(request.url)
                    return response(request, b"ok")
                session, provider = client._session_for_config(self.config(
                    base_url="https://configured.example/original",
                    service_url=first, access_token="opaque-seeded"))
                prepared = session.prepare_request(requests.Request(
                    "GET", "https://configured.example/original" + REPORT_PATH + "?filter=A%2FB"))
                with mock.patch.object(requests.adapters.HTTPAdapter, "send", new=send):
                    session.send(prepared)
                    session.send(prepared)
                    provider._save_access_token(mock.Mock(json=lambda: {
                        "access_token": "opaque-refreshed", "service_url": "https://second.example/new-prefix",
                    }), None)
                    session.send(prepared)
                    session.send(prepared)
                    session.get(first + REPORT_PATH + "?filter=A%2FB")
                self.assertEqual(seen, [
                    first + REPORT_PATH + "?filter=A%2FB",
                    first + REPORT_PATH + "?filter=A%2FB",
                    "https://second.example/new-prefix" + REPORT_PATH + "?filter=A%2FB",
                    "https://second.example/new-prefix" + REPORT_PATH + "?filter=A%2FB",
                    "https://second.example/new-prefix" + REPORT_PATH + "?filter=A%2FB",
                ])

    def test_gateway_origin_ignores_hostname_case_and_default_port(self):
        for endpoint in ("https://Gateway.example/Original", "https://Gateway.example:443/Original"):
            with self.subTest(endpoint=endpoint):
                seen = []
                def send(adapter, request, **kwargs):
                    seen.append(request.url)
                    return response(request, b"ok")
                session, provider = client._session_for_config(self.config(
                    base_url=endpoint, service_url=endpoint, access_token="opaque-seeded"))
                with mock.patch.object(requests.adapters.HTTPAdapter, "send", new=send):
                    session.get("https://GATEWAY.example/Original" + REPORT_PATH + "?filter=A%2FB")
                    session.get("https://gateway.example/original" + REPORT_PATH)
                self.assertEqual(seen, [
                    endpoint + REPORT_PATH + "?filter=A%2FB",
                    endpoint + "/original" + REPORT_PATH,
                ])

    def test_missing_endpoint_keeps_configured_or_previous_returned_route(self):
        provider = oauth.WorkdayOAuthTokenProvider.from_config(self.config(base_url="https://custom.example/prefix"))
        for payload, expected in [
            ({"access_token": "one"}, "https://custom.example/prefix"),
            ({"access_token": "two", "instance_url": "https://instance.example/p", "service_url": "https://service.example/p"}, "https://instance.example/p"),
            ({"access_token": "three"}, "https://instance.example/p"),
        ]:
            provider._save_access_token(mock.Mock(json=lambda: payload), None)
            self.assertEqual(provider.base_url, expected)

    def test_http_urls_rejected_before_credentials_leave_process(self):
        with mock.patch.object(requests.adapters.HTTPAdapter, "send") as send:
            with self.assertRaises(ValueError):
                oauth.WorkdayOAuthTokenProvider("id", "secret", "http://insecure.example/token")
            with self.assertRaises(ValueError):
                oauth.WorkdayRoutingSession("http://insecure.example")
            with self.assertRaises(ValueError):
                oauth.WorkdayRoutingSession().get("http://insecure.example/report")
            provider = oauth.WorkdayOAuthTokenProvider.from_config(self.config())
            with self.assertRaises(ValueError):
                provider._save_access_token(mock.Mock(json=lambda: {
                    "access_token": "opaque", "service_url": "http://insecure.example"}), None)
            send.assert_not_called()
