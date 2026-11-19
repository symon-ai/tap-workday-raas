"""API endpoint routing through real requests sessions and token acquisition."""

import json
import unittest
from unittest import mock

import requests

from tap_workday_raas.oauth_middleware import WorkdayOAuthError, WorkdayOAuthTokenProvider
from tap_workday_raas.client import download_xsd, stream_report
from tap_workday_raas.client import _session_for_config


class TestApiEndpoints(unittest.TestCase):
    def config(self, **overrides):
        return dict({
            "auth_type": "oauth", "client_id": "client", "client_secret": "secret",
            "token_url": "https://login.example/custom/token", "base_url": "https://configured.example/gateway/",
        }, **overrides)

    def response(self, payload):
        response = requests.Response()
        response.status_code = 200
        response._content = json.dumps(payload).encode()
        return response

    def test_routing_precedence_refresh_cache_and_custom_token_url(self):
        token_http = requests.Session()
        token_http.post = mock.Mock(side_effect=[
            self.response({"access_token": "one", "instance_url": "https://instance.example/prefix"}),
            self.response({"access_token": "two", "instance_url": "https://ignored.example", "service_url": "https://service.example/proxy/payroll"}),
            self.response({"access_token": "three"}),
            self.response({"access_token": "four", "instance_url": "https://new.example"}),
        ])
        provider = WorkdayOAuthTokenProvider("client", "secret", self.config()["token_url"], session=token_http,
                                              base_url=self.config()["base_url"])
        session = requests.Session()
        provider.apply_to_session(session)
        urls = []

        def send(request, **kwargs):
            urls.append((request.url, request.headers.get("Authorization"), kwargs["verify"]))
            return self.response({})

        original = "https://legacy.example/ccx/service/tenant/Payroll/v38.0?wsdl"
        with mock.patch.object(requests.adapters.HTTPAdapter, "send", side_effect=send):
            session.get(original)
            provider.get_access_token()
            self.assertEqual(token_http.post.call_count, 1)
            provider.force_refresh()
            session.post(original, data=b"<soap/>")
            provider.force_refresh()
            session.get(original)
            provider.force_refresh()
            session.get(original)
        self.assertEqual(urls, [
            ("https://instance.example/prefix/ccx/service/tenant/Payroll/v38.0?wsdl", "Bearer one", True),
            ("https://service.example/proxy/payroll?wsdl", "Bearer two", True),
            ("https://service.example/proxy/payroll?wsdl", "Bearer three", True),
            ("https://new.example/ccx/service/tenant/Payroll/v38.0?wsdl", "Bearer four", True),
        ])
        for call in token_http.post.call_args_list:
            self.assertEqual(call.args[0], "https://login.example/custom/token")
            self.assertTrue(call.kwargs["verify"])
            self.assertTrue(call.kwargs["headers"]["Authorization"].startswith("Basic "))

    def test_configured_base_with_seeded_token_and_basic_auth(self):
        for config in (
            self.config(access_token="seeded"),
            {"auth_type": "basic", "username": "user", "password": "password", "base_url": self.config()["base_url"]},
        ):
            with self.subTest(auth_type=config["auth_type"]):
                session, provider = _session_for_config(config)
                with mock.patch.object(requests.adapters.HTTPAdapter, "send", return_value=self.response({})) as send:
                    session.get("https://legacy.example/ccx/service/customreport2/tenant/owner/report?format=json&x=1")
                    request = send.call_args.args[0]
                    self.assertEqual(request.url, "https://configured.example/gateway/ccx/service/customreport2/tenant/owner/report?format=json&x=1")
                    self.assertTrue(send.call_args.kwargs["verify"])
                    if provider:
                        self.assertEqual(request.headers["Authorization"], "Bearer seeded")
                    else:
                        self.assertTrue(request.headers["Authorization"].startswith("Basic "))

    def test_legacy_default_is_unchanged(self):
        config = self.config(access_token="seeded", base_url=None)
        session, _ = _session_for_config(config)
        url = "https://legacy.example/ccx/service/tenant/Human_Resources/v37.2?wsdl"
        with mock.patch.object(requests.adapters.HTTPAdapter, "send", return_value=self.response({})) as send:
            session.get(url)
            self.assertEqual(send.call_args.args[0].url, url)

    def test_report_and_xsd_consumers_preserve_routes_and_query(self):
        config = self.config(access_token="seeded")
        report_url = "https://legacy.example/ccx/service/customreport2/tenant/owner/report?param=1&format=xml"
        seen = []

        def send(request, **kwargs):
            seen.append(request.url)
            response = self.response({"Report_Entry": [{"id": "1"}]})
            response._content_consumed = True
            return response

        with mock.patch.object(requests.adapters.HTTPAdapter, "send", side_effect=send):
            self.assertEqual(list(stream_report(report_url, config)), [{"id": "1"}])
            self.assertIn("Report_Entry", download_xsd(report_url, config))
        self.assertEqual(seen, [
            "https://configured.example/gateway/ccx/service/customreport2/tenant/owner/report?param=1&format=json",
            "https://configured.example/gateway/ccx/service/customreport2/tenant/owner/report?xsds",
        ])

    def test_seeded_service_url_is_the_complete_report_endpoint(self):
        endpoint = "https://service.example/proxy/ccx/service/customreport2/tenant/owner/report"
        config = self.config(access_token="seeded", service_url=endpoint)
        report_url = "https://legacy.example/ccx/service/customreport2/tenant/owner/report?param=1&format=xml"
        seen = []

        def send(request, **kwargs):
            seen.append((request.url, request.headers["Authorization"]))
            response = self.response({"Report_Entry": [{"id": "1"}]})
            response._content_consumed = True
            return response

        with mock.patch.object(requests.Session, "post") as token_post:
            with mock.patch.object(requests.adapters.HTTPAdapter, "send", side_effect=send):
                self.assertEqual(list(stream_report(report_url, config)), [{"id": "1"}])
                self.assertIn("Report_Entry", download_xsd(report_url, config))
            token_post.assert_not_called()
        self.assertEqual(seen, [
            (endpoint + "?param=1&format=json", "Bearer seeded"),
            (endpoint + "?xsds", "Bearer seeded"),
        ])

    def test_invalid_endpoint_fails_without_caching_access_token(self):
        for endpoint in ("http://unsafe.example", "https://user:password@example.com", "https://example.com?x=1", "/relative"):
            with self.subTest(endpoint=endpoint):
                with self.assertRaises(ValueError):
                    WorkdayOAuthTokenProvider.from_config(self.config(base_url=endpoint))
                provider = WorkdayOAuthTokenProvider("client", "secret", self.config()["token_url"])
                with self.assertRaises(ValueError):
                    WorkdayOAuthTokenProvider.from_config(self.config(access_token="seeded", service_url=endpoint))
                with self.assertRaises(WorkdayOAuthError):
                    provider._save_access_token(self.response({"access_token": "invalid", "instance_url": endpoint}), None)
                self.assertIsNone(provider._access_token)


if __name__ == "__main__":
    unittest.main()
