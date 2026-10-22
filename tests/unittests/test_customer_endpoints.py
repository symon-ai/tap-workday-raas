"""Endpoint routing exercised through real prepared requests, with an in-memory transport."""

import io
import json
import unittest
from unittest import mock

import requests
import test_client_stream_oauth as client_tests

client = client_tests._client_mod


class EndpointTransport(requests.adapters.BaseAdapter):
    def __init__(self, tokens, statuses=()):
        self.tokens = iter(tokens)
        self.statuses = iter(statuses)
        self.calls = []

    def send(self, request, **kwargs):
        self.calls.append((request.method, request.url, request.headers.get("Authorization"), kwargs["verify"]))
        response = requests.Response()
        response.request = request
        response.url = request.url
        if request.method == "POST":
            response.status_code = 200
            body = json.dumps(next(self.tokens)).encode()
        else:
            response.status_code = next(self.statuses, 200)
            body = b"<schema/>" if "xsds" in request.url else client_tests._minimal_report_json_bytes()
        response.raw = io.BytesIO(body)
        return response

    def close(self):
        pass


class TestCustomerEndpoints(unittest.TestCase):
    report = "https://stale.example/ccx/service/customreport2/tenant/owner/report?filter=a%2Fb&format=csv"
    config = {
        "auth_type": "oauth",
        "client_id": "id",
        "client_secret": "secret",
        "token_url": "https://login.customer.example/oauth/token",
        "base_url": "https://api.customer.example/proxy/",
    }

    def transport(self, tokens, statuses=()):
        transport = EndpointTransport(tokens, statuses)
        session_type = requests.Session

        def session():
            result = session_type()
            result.mount("https://", transport)
            return result

        patcher = mock.patch.object(client.requests, "Session", side_effect=session)
        patcher.start()
        self.addCleanup(patcher.stop)
        return transport

    def gets(self, transport):
        return [call for call in transport.calls if call[0] == "GET"]

    def test_configured_base_preserves_resource_query_and_tls(self):
        transport = self.transport([{"access_token": "opaque-not-a-jwt"}])
        proxied_report = self.report.replace("https://stale.example", "https://api.customer.example/proxy")
        self.assertEqual(len(list(client.stream_report(proxied_report, self.config))), 3)
        self.assertEqual(self.gets(transport), [(
            "GET",
            "https://api.customer.example/proxy/ccx/service/customreport2/tenant/owner/report?filter=a%2Fb&format=json",
            "Bearer opaque-not-a-jwt", True,
        )])
        self.assertEqual(transport.calls[0][1], self.config["token_url"])
        self.assertTrue(transport.calls[0][3])

    def test_token_precedence_and_refresh_moves_retry_off_old_host(self):
        transport = self.transport([
            {"access_token": "first", "instance_url": "https://first.customer.example/edge",
             "service_url": "https://ignored.example"},
            {"access_token": "second", "service_url": "https://second.customer.example/gateway/"},
        ], [401, 200])
        proxied_report = self.report.replace("https://stale.example", "https://old.example/old-proxy")
        self.assertEqual(len(list(client.stream_report(proxied_report, self.config))), 3)
        gets = self.gets(transport)
        self.assertEqual([call[1] for call in gets], [
            "https://first.customer.example/edge/ccx/service/customreport2/tenant/owner/report?filter=a%2Fb&format=json",
            "https://second.customer.example/gateway/ccx/service/customreport2/tenant/owner/report?filter=a%2Fb&format=json",
        ])
        self.assertEqual([call[2] for call in gets], ["Bearer first", "Bearer second"])
        self.assertEqual([c[1] for c in transport.calls if c[0] == "POST"],
                         [self.config["token_url"], self.config["token_url"]])

    def test_refresh_without_endpoint_keeps_last_base_for_retry(self):
        transport = self.transport([
            {"access_token": "first", "instance_url": " ", "service_url": "https://selected.example/base"},
            {"access_token": "second", "instance_url": "", "service_url": " "},
        ], [401, 200])
        list(client.stream_report(self.report, self.config))
        gets = self.gets(transport)
        self.assertEqual(gets[0][1], gets[1][1])
        self.assertTrue(gets[1][1].startswith("https://selected.example/base/"))
        self.assertEqual(gets[1][2], "Bearer second")

    def test_legacy_report_url_without_base(self):
        transport = self.transport([{"access_token": "opaque"}])
        config = dict(self.config)
        config.pop("base_url")
        list(client.stream_report(self.report, config))
        self.assertEqual(self.gets(transport)[0][1], self.report.replace("format=csv", "format=json"))

    def test_discovery_download_refreshes_and_reuses_latest_endpoint(self):
        transport = self.transport([
            {"access_token": "first", "instance_url": "https://first.example/proxy"},
            {"access_token": "second", "instance_url": "https://second.example/proxy"},
            {"access_token": "third"},
        ], [401, 200, 200])
        session, provider = client._session_for_config(self.config)
        self.assertEqual(client.download_xsd(
            self.report, self.config, session=session, oauth_provider=provider), "<schema/>")
        provider._expires_at = 0
        client.download_xsd(
            "https://stale.example/another-report", self.config, session=session, oauth_provider=provider)
        gets = self.gets(transport)
        self.assertEqual([call[1] for call in gets], [
            "https://first.example/proxy/ccx/service/customreport2/tenant/owner/report?filter=a%2Fb&xsds",
            "https://second.example/proxy/ccx/service/customreport2/tenant/owner/report?filter=a%2Fb&xsds",
            "https://second.example/proxy/another-report?xsds",
        ])
        self.assertEqual([call[2] for call in gets], ["Bearer first", "Bearer second", "Bearer third"])

    def test_token_endpoint_applies_without_configured_base(self):
        transport = self.transport([{"access_token": "opaque", "service_url": "https://token-selected.example/base"}])
        config = dict(self.config)
        config.pop("base_url")
        list(client.stream_report(self.report, config))
        self.assertTrue(self.gets(transport)[0][1].startswith("https://token-selected.example/base/ccx/"))

    def test_platform_access_token_uses_configured_base_without_exchange(self):
        transport = self.transport([])
        list(client.stream_report(self.report, dict(self.config, access_token="opaque-platform-token")))
        self.assertEqual(len(transport.calls), 1)
        self.assertTrue(self.gets(transport)[0][1].startswith("https://api.customer.example/proxy/ccx/"))
        self.assertEqual(self.gets(transport)[0][2], "Bearer opaque-platform-token")

    def test_basic_auth_uses_configured_base(self):
        transport = self.transport([])
        list(client.stream_report(self.report, {
            "username": "u", "password": "p", "base_url": self.config["base_url"],
        }))
        get = self.gets(transport)[0]
        self.assertTrue(get[1].startswith("https://api.customer.example/proxy/ccx/"))
        self.assertEqual(get[2], "Basic dTpw")

    def test_unsafe_configured_base_rejected_before_credentials_sent(self):
        invalid = ("http://api.example", "https://user@api.example", "https://api.example?q=1",
                   "https://api.example/#fragment", "https://api.example:bad", "//api.example",
                   "https://api.example/unsafe\npath", "https://api.example\\@other.example")
        for base in invalid:
            with self.subTest(base=base):
                with mock.patch.object(client.requests, "Session") as session:
                    with self.assertRaises(client_tests.SymonException):
                        list(client.stream_report(self.report, dict(self.config, base_url=base)))
                    session.assert_not_called()

    def test_unsafe_token_override_does_not_send_report_auth(self):
        transport = self.transport([{"access_token": "opaque", "instance_url": "http://unsafe.example"}])
        with self.assertRaises(client_tests.SymonException):
            list(client.stream_report(self.report, self.config))
        self.assertEqual(self.gets(transport), [])


if __name__ == "__main__":
    unittest.main()
