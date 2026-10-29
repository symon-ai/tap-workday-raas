"""Prove stream_report holds one report GET for the full response stream (OAuth)."""

import json
import unittest
from unittest import mock

import requests

from tap_workday_raas import client as _client_mod
from tap_workday_raas import oauth_middleware as _oauth_mod
from tap_workday_raas.symon_exception import SymonException

stream_report = _client_mod.stream_report
_wrap_oauth_error = _client_mod._wrap_oauth_error
WorkdayOAuthError = _oauth_mod.WorkdayOAuthError
WorkdayRefreshTokenInvalidError = _oauth_mod.WorkdayRefreshTokenInvalidError


def _minimal_report_json_bytes():
    return b'{"Report_Entry": [{"row_id": "1"}, {"row_id": "2"}, {"row_id": "3"}]}'


class _StreamingReportResponse:
    def __init__(self, body: bytes, status_code: int = 200):
        self._body = body
        self.status_code = status_code

    def iter_content(self, chunk_size=512):
        mid = max(1, len(self._body) // 2)
        yield self._body[:mid]
        yield self._body[mid:]

    def raise_for_status(self):
        return None

    def close(self):
        return None


class _GetContext:
    def __init__(self, response):
        self._response = response

    def __enter__(self):
        return self._response

    def __exit__(self, *args):
        return False


class TestWrapOAuthError(unittest.TestCase):
    def test_refresh_token_invalid_uses_standard_user_message(self):
        friendly = (
            "Please update your Workday OAuth credentials: the refresh token is no longer valid "
            "(expired, revoked, or rotated). Obtain a new refresh token in Workday and update your "
            "configuration. Details: Token endpoint error: HTTP 400 — "
            '{"error":"invalid_grant"}'
        )
        exc = WorkdayRefreshTokenInvalidError(
            friendly,
            status_code=400,
            response_body='{"error":"invalid_grant"}',
        )
        wrapped = _wrap_oauth_error(exc)
        self.assertIsInstance(wrapped, SymonException)
        self.assertEqual(
            str(wrapped),
            "The Workday OAuth token request failed. Check the token configuration and expiration.",
        )

    def test_other_oauth_400_uses_standard_user_message(self):
        msg = 'Token endpoint error: HTTP 400 — {"error":"invalid_client"}'
        exc = WorkdayOAuthError(msg, status_code=400, response_body='{"error":"invalid_client"}')
        wrapped = _wrap_oauth_error(exc)
        self.assertEqual(
            str(wrapped),
            "The Workday OAuth token request failed. Check the token configuration and expiration.",
        )


class TestStreamReportOAuth(unittest.TestCase):
    @mock.patch.object(_client_mod, "_session_for_config")
    def test_single_report_get_for_entire_stream_despite_token_cache_expiry(self, mock_session_for_config):
        """
        Long syncs use one streaming GET. Expired access-token cache must not open a second
        report request mid-stream (that would restart the download from Workday).
        """
        body = _minimal_report_json_bytes()
        report_get_count = {"n": 0}

        def counting_get(*_args, **_kwargs):
            report_get_count["n"] += 1
            return _GetContext(_StreamingReportResponse(body))

        session = mock.Mock()
        session.get.side_effect = counting_get
        session.headers = {}

        provider = mock.Mock()
        provider.get_access_token = mock.Mock(return_value="access-token-1")
        provider.api_base_url = None
        provider.force_refresh = mock.Mock()
        provider._expires_at = 0.0

        def apply_to_session(sess):
            sess.headers["Authorization"] = "Bearer " + provider.get_access_token()

        provider.apply_to_session = apply_to_session

        config = {
            "auth_type": "oauth",
            "client_id": "id",
            "client_secret": "sec",
            "token_url": "https://example/token",
            "refresh_token": "rt",
            "oauth_grant_type": "refresh_token",
        }

        def fake_session_for_config(_cfg):
            provider.apply_to_session(session)
            return session, provider

        mock_session_for_config.side_effect = fake_session_for_config

        records = list(stream_report("https://example/report?format=csv", config))

        self.assertEqual(len(records), 3)
        self.assertEqual(report_get_count["n"], 1)
        self.assertEqual(session.get.call_count, 1)
        # Token is resolved once before the stream; not again while rows are read.
        self.assertEqual(provider.get_access_token.call_count, 1)
        provider.force_refresh.assert_not_called()

    @mock.patch.object(_client_mod, "_session_for_config")
    def test_401_on_report_retries_once_with_force_refresh(self, mock_session_for_config):
        """Initial 401 on the report URL refreshes token and retries a single new GET."""
        body = _minimal_report_json_bytes()
        unauthorized = _StreamingReportResponse(b"", status_code=401)
        ok = _StreamingReportResponse(body)

        session = mock.Mock()
        session.get.side_effect = [_GetContext(unauthorized), _GetContext(ok)]
        session.headers = {}

        provider = mock.Mock()
        provider.get_access_token = mock.Mock(side_effect=["stale-token", "fresh-token"])
        provider.api_base_url = None
        provider.force_refresh = mock.Mock()

        def apply_to_session(sess):
            sess.headers["Authorization"] = "Bearer " + provider.get_access_token()

        provider.apply_to_session = apply_to_session

        config = {
            "auth_type": "oauth",
            "client_id": "id",
            "client_secret": "sec",
            "token_url": "https://example/token",
            "refresh_token": "rt",
            "oauth_grant_type": "refresh_token",
        }

        def fake_session_for_config(_cfg):
            provider.apply_to_session(session)
            return session, provider

        mock_session_for_config.side_effect = fake_session_for_config

        records = list(stream_report("https://example/report", config))

        self.assertEqual(len(records), 3)
        self.assertEqual(session.get.call_count, 2)
        provider.force_refresh.assert_called_once()
        self.assertEqual(provider.get_access_token.call_count, 1)


class _RecordingAdapter(requests.adapters.BaseAdapter):
    """Keep Requests preparation and auth real; replace only network transport."""

    def __init__(self, token_bodies, report_statuses=(200,)):
        self.token_bodies = iter(token_bodies)
        self.report_statuses = iter(report_statuses)
        self.sent = []

    def send(self, request, **kwargs):
        self.sent.append(request)
        response = requests.Response()
        response.request = request
        response.url = request.url
        if request.method == "POST":
            response.status_code = 200
            response._content = json.dumps(next(self.token_bodies)).encode("utf-8")
        else:
            response.status_code = next(self.report_statuses)
            response._content = (
                b"<schema/>" if "xsds" in request.url else _minimal_report_json_bytes()
            )
        response._content_consumed = True
        return response

    def close(self):
        pass


class TestEndpointRouting(unittest.TestCase):
    REPORT = (
        "https://old.example/ccx/service/customreport2/tenant/owner/Report"
        "?filter=a%2Fb&format=csv"
    )
    PATH = "/ccx/service/customreport2/tenant/owner/Report"
    CONFIG = {
        "auth_type": "oauth",
        "client_id": "id",
        "client_secret": "secret",
        "token_url": "https://auth.example/token",
        "oauth_grant_type": "refresh_token",
        "refresh_token": "refresh",
        "base_url": "https://configured.example/proxy/",
    }

    def _session(self, token_bodies, report_statuses=(200,)):
        session = requests.Session()
        adapter = _RecordingAdapter(token_bodies, report_statuses)
        session.mount("https://", adapter)
        return session, adapter

    def test_gateway_query_merges_with_report_query_and_schema_selector(self):
        endpoint = (
            "https://gateway.example/workday?route=stale&route=customerA"
            "&shared=endpoint%2Fvalue&format=xml&xsds=ignored&wsdl=ignored"
        )
        report = self.REPORT + "&route=report&shared=report"
        for auth in ("basic", "base", "instance_url", "service_url"):
            for seeded in (False, True):
                for schema_request in (False, True):
                    with self.subTest(auth=auth, seeded=seeded, schema=schema_request):
                        config = dict(self.CONFIG)
                        tokens = []
                        if auth == "basic":
                            config = {"username": "user", "password": "password",
                                      "base_url": endpoint}
                        elif auth == "base":
                            config["base_url"] = endpoint
                        elif seeded:
                            config[auth] = endpoint
                        else:
                            tokens = [{"access_token": "token", auth: endpoint}]
                        if auth != "basic":
                            if seeded:
                                config["access_token"] = "token"
                            elif not tokens:
                                tokens = [{"access_token": "token"}]
                        session, adapter = self._session(tokens)
                        with mock.patch.object(_client_mod.requests, "Session", return_value=session):
                            if schema_request:
                                self.assertEqual(
                                    _client_mod.download_xsd(report, config), "<schema/>"
                                )
                            else:
                                self.assertEqual(len(list(stream_report(report, config))), 3)
                        request = adapter.sent[-1]
                        suffix = "xsds" if schema_request else "filter=a%2Fb&format=json"
                        self.assertEqual(
                            request.url,
                            "https://gateway.example/workday" + self.PATH
                            + "?route=customerA&shared=endpoint%2Fvalue&" + suffix,
                        )

    def test_refresh_replaces_gateway_query_for_report_and_schema(self):
        for initial_key in ("instance_url", "service_url"):
            for refreshed_key in ("instance_url", "service_url", "base"):
                for schema_request in (False, True):
                    with self.subTest(initial=initial_key, refreshed=refreshed_key,
                                      schema=schema_request):
                        config = {
                            **self.CONFIG,
                            "access_token": "seeded",
                            initial_key: "https://first.example/one?route=first&old=only",
                            "base_url": "https://gateway.example/base?route=configured",
                        }
                        refreshed = {"access_token": "new"}
                        if refreshed_key != "base":
                            refreshed[refreshed_key] = (
                                "https://gateway.example/two?route=new&route=customerA"
                                "&format=xml&xsds=ignored"
                            )
                        session, adapter = self._session([refreshed], (401, 200))
                        with mock.patch.object(_client_mod.requests, "Session", return_value=session):
                            if schema_request:
                                self.assertEqual(
                                    _client_mod.download_xsd(self.REPORT, config), "<schema/>"
                                )
                            else:
                                self.assertEqual(len(list(stream_report(self.REPORT, config))), 3)
                        suffix = "xsds" if schema_request else "filter=a%2Fb&format=json"
                        requests_sent = [request for request in adapter.sent
                                         if request.method == "GET"]
                        expected_base = (
                            "https://gateway.example/base" + self.PATH + "?route=configured&"
                            if refreshed_key == "base" else
                            "https://gateway.example/two" + self.PATH + "?route=customerA&"
                        )
                        self.assertEqual(
                            [request.url for request in requests_sent],
                            [
                                "https://first.example/one" + self.PATH
                                + "?route=first&old=only&" + suffix,
                                expected_base + suffix,
                            ],
                        )
                        self.assertEqual(
                            [request.headers["Authorization"] for request in requests_sent],
                            ["Bearer seeded", "Bearer new"],
                        )

    def test_oauth_precedence_on_prepared_stream_request(self):
        cases = [
            (
                {"instance_url": " https://instance.example/gateway/ ",
                 "service_url": "https://service.example"},
                "https://instance.example/gateway",
            ),
            (
                {"instance_url": " ", "service_url": "https://service.example/api"},
                "https://service.example/api",
            ),
            (
                {"instance_url": "", "service_url": " "},
                "https://configured.example/proxy",
            ),
        ]
        for endpoints, expected_base in cases:
            with self.subTest(endpoints=endpoints):
                session, adapter = self._session([{"access_token": "first", **endpoints}])
                with mock.patch.object(_client_mod.requests, "Session", return_value=session):
                    records = list(stream_report(self.REPORT, self.CONFIG))
                self.assertEqual(len(records), 3)
                self.assertEqual(adapter.sent[0].url, self.CONFIG["token_url"])
                self.assertEqual(
                    adapter.sent[1].url,
                    expected_base + self.PATH + "?filter=a%2Fb&format=json",
                )
                self.assertEqual(adapter.sent[1].headers["Authorization"], "Bearer first")

    def test_seeded_token_routes_first_report_and_schema_request(self):
        cases = [
            (
                {"instance_url": " https://seed.example/gateway/ ",
                 "service_url": "https://unused.example"},
                "https://seed.example/gateway",
            ),
            (
                {"instance_url": " ", "service_url": " https://seed.example/api/ "},
                "https://seed.example/api",
            ),
            (
                {"instance_url": None, "service_url": ""},
                "https://configured.example/proxy",
            ),
        ]
        for endpoints, expected_base in cases:
            for schema_request in (False, True):
                with self.subTest(endpoints=endpoints, schema_request=schema_request):
                    config = {**self.CONFIG, "access_token": "seeded", **endpoints}
                    session, adapter = self._session([])
                    with mock.patch.object(_client_mod.requests, "Session", return_value=session):
                        if schema_request:
                            self.assertEqual(
                                _client_mod.download_xsd(self.REPORT, config), "<schema/>"
                            )
                        else:
                            self.assertEqual(len(list(stream_report(self.REPORT, config))), 3)
                    self.assertEqual(len(adapter.sent), 1)
                    request = adapter.sent[0]
                    suffix = "?xsds" if schema_request else "?filter=a%2Fb&format=json"
                    self.assertEqual(request.method, "GET")
                    self.assertEqual(request.url, expected_base + self.PATH + suffix)
                    self.assertEqual(request.headers["Authorization"], "Bearer seeded")

    def test_refresh_without_endpoint_clears_seed_for_report_and_schema(self):
        for base in ("https://configured.example/proxy/", None, "", " "):
            for seed_key in ("instance_url", "service_url"):
                for schema_request in (False, True):
                    with self.subTest(base=base, seed_key=seed_key, schema_request=schema_request):
                        config = {
                            **self.CONFIG,
                            "access_token": "seeded",
                            seed_key: "https://seed.example/gateway/",
                        }
                        config.pop("base_url")
                        if base is not None:
                            config["base_url"] = base
                        session, adapter = self._session(
                            [{"access_token": "refreshed"}], report_statuses=(401, 200)
                        )
                        with mock.patch.object(_client_mod.requests, "Session", return_value=session):
                            if schema_request:
                                self.assertEqual(
                                    _client_mod.download_xsd(self.REPORT, config), "<schema/>"
                                )
                            else:
                                self.assertEqual(len(list(stream_report(self.REPORT, config))), 3)
                        self.assertEqual(
                            [request.method for request in adapter.sent], ["GET", "POST", "GET"]
                        )
                        suffix = "?xsds" if schema_request else "?filter=a%2Fb&format=json"
                        fallback = base.strip().rstrip("/") if base and base.strip() else "https://old.example"
                        self.assertEqual(
                            [adapter.sent[0].url, adapter.sent[2].url],
                            [
                                "https://seed.example/gateway" + self.PATH + suffix,
                                fallback + self.PATH + suffix,
                            ],
                        )
                        self.assertEqual(
                            [adapter.sent[0].headers["Authorization"],
                             adapter.sent[2].headers["Authorization"]],
                            ["Bearer seeded", "Bearer refreshed"],
                        )
                        self.assertEqual(adapter.sent[1].url, self.CONFIG["token_url"])
                        self.assertIn("grant_type=refresh_token", adapter.sent[1].body)

    def test_basic_auth_custom_base_and_legacy_defaults(self):
        cases = [
            ("https://custom.example/proxy/", "https://custom.example/proxy"),
            ("", "https://old.example"),
            ("   ", "https://old.example"),
            (None, "https://old.example"),
        ]
        for base, expected_base in cases:
            with self.subTest(base=base):
                config = {"username": "user", "password": "password"}
                if base is not None:
                    config["base_url"] = base
                session, adapter = self._session([])
                with mock.patch.object(_client_mod.requests, "Session", return_value=session):
                    list(stream_report(self.REPORT, config))
                self.assertEqual(
                    adapter.sent[0].url,
                    expected_base + self.PATH + "?filter=a%2Fb&format=json",
                )
                self.assertTrue(adapter.sent[0].headers["Authorization"].startswith("Basic "))

    def test_refresh_routes_retry_to_new_endpoint_with_new_bearer(self):
        for schema_request in (False, True):
            with self.subTest(schema_request=schema_request):
                session, adapter = self._session(
                    [
                        {"access_token": "old", "instance_url": "https://first.example/one"},
                        {
                            "access_token": "new",
                            "instance_url": " ",
                            "service_url": "https://second.example/two/",
                        },
                    ],
                    report_statuses=(401, 200),
                )
                with mock.patch.object(_client_mod.requests, "Session", return_value=session):
                    if schema_request:
                        self.assertEqual(
                            _client_mod.download_xsd(self.REPORT, self.CONFIG), "<schema/>"
                        )
                    else:
                        self.assertEqual(len(list(stream_report(self.REPORT, self.CONFIG))), 3)
                reports = [request for request in adapter.sent if request.method == "GET"]
                suffix = "?xsds" if schema_request else "?filter=a%2Fb&format=json"
                self.assertEqual(
                    [request.url for request in reports],
                    [
                        "https://first.example/one" + self.PATH + suffix,
                        "https://second.example/two" + self.PATH + suffix,
                    ],
                )
                self.assertEqual(
                    [request.headers["Authorization"] for request in reports],
                    ["Bearer old", "Bearer new"],
                )
                tokens = [request for request in adapter.sent if request.method == "POST"]
                self.assertEqual(len(tokens), 2)
                self.assertTrue(all(request.url == self.CONFIG["token_url"] for request in tokens))
                self.assertTrue(all("grant_type=refresh_token" in request.body for request in tokens))

    def test_refresh_without_endpoint_falls_back_to_configured_base(self):
        session, adapter = self._session(
            [
                {"access_token": "old", "instance_url": "https://first.example"},
                {"access_token": "new"},
            ],
            report_statuses=(401, 200),
        )
        with mock.patch.object(_client_mod.requests, "Session", return_value=session):
            list(stream_report(self.REPORT, self.CONFIG))
        self.assertEqual(
            adapter.sent[-1].url,
            "https://configured.example/proxy" + self.PATH + "?filter=a%2Fb&format=json",
        )

    def test_existing_endpoint_prefix_is_not_duplicated(self):
        session, adapter = self._session([])
        config = {
            "username": "user",
            "password": "password",
            "base_url": "https://custom.example/ccx/service/",
        }
        with mock.patch.object(_client_mod.requests, "Session", return_value=session):
            list(stream_report(self.REPORT, config))
        self.assertEqual(
            adapter.sent[0].url,
            "https://custom.example" + self.PATH + "?filter=a%2Fb&format=json",
        )


    def test_oauth_without_endpoint_or_base_keeps_report_host(self):
        for base in (None, "", " "):
            with self.subTest(base=base):
                config = dict(self.CONFIG)
                config.pop("base_url")
                if base is not None:
                    config["base_url"] = base
                session, adapter = self._session([{"access_token": "token"}])
                with mock.patch.object(_client_mod.requests, "Session", return_value=session):
                    list(stream_report(self.REPORT, config))
                self.assertEqual(
                    adapter.sent[-1].url,
                    "https://old.example" + self.PATH + "?filter=a%2Fb&format=json",
                )


if __name__ == "__main__":
    unittest.main()
