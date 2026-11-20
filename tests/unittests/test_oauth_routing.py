import socket
import unittest
from unittest import mock

import requests

from tap_workday_raas.client import download_xsd, stream_report
from tap_workday_raas.symon_exception import SymonException


_REPORT = "https://legacy.example/ccx/service/customreport2/tenant/owner/report"
_PATH = "/ccx/service/customreport2/tenant/owner/report"
_CONFIG = {
    "auth_type": "oauth", "client_id": "id", "client_secret": "secret",
    "tenant": "tenant", "base_url": "https://base.example/",
    "access_token": "opaque-token-one", "service_url": "https://first.example/hint",
    "instance_url": "https://not-selected.example",
    "disable_ssl_verification": True,
}


def _response(status, content):
    response = requests.Response()
    response.status_code = status
    response._content = content
    response._content_consumed = True
    return response


_PUBLIC_DNS = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 443))]


class TestReportRouting(unittest.TestCase):
    @mock.patch("socket.getaddrinfo", return_value=_PUBLIC_DNS)
    @mock.patch.object(requests.Session, "request")
    def test_stream_rebuilds_url_and_bearer_together_after_401(self, request, _dns):
        request.side_effect = [
            _response(401, b""),
            _response(200, b'{"access_token":"opaque-token-two","instance_url":"https://second.example/unused"}'),
            _response(200, b'{"Report_Entry":[{"row":1}]}'),
        ]
        rows = list(stream_report(_REPORT + "?prompt=a%2Fb&format=csv", _CONFIG))
        self.assertEqual(rows, [{"row": 1}])
        calls = request.call_args_list
        self.assertEqual([call.args[:2] for call in calls], [
            ("GET", "https://first.example" + _PATH + "?prompt=a%2Fb&format=json"),
            ("POST", "https://base.example/ccx/oauth2/tenant/token"),
            ("GET", "https://second.example" + _PATH + "?prompt=a%2Fb&format=json"),
        ])
        self.assertEqual(calls[0].kwargs["headers"]["Authorization"], "Bearer opaque-token-one")
        self.assertEqual(calls[-1].kwargs["headers"]["Authorization"], "Bearer opaque-token-two")
        self.assertTrue(all(c.kwargs["verify"] and not c.kwargs["allow_redirects"] for c in calls))

    @mock.patch("socket.getaddrinfo", return_value=_PUBLIC_DNS)
    @mock.patch.object(requests.Session, "request")
    def test_xsd_uses_initial_response_route_and_preserves_report_path(self, request, _dns):
        request.side_effect = [
            _response(200, b'{"access_token":"opaque-token","service_url":"https://schema.example/unused"}'),
            _response(200, b"<schema/>"),
        ]
        config = dict(_CONFIG, access_token=None, token_url="https://legacy.example/custom/token?q=1")
        self.assertEqual(download_xsd(_REPORT + "?ignored=1", config), "<schema/>")
        self.assertEqual([call.args[:2] for call in request.call_args_list], [
            ("POST", "https://base.example/custom/token?q=1"),
            ("GET", "https://schema.example" + _PATH + "?xsds"),
        ])

    @mock.patch.object(requests.Session, "request")
    def test_xsd_retry_without_new_route_falls_back_to_base_url(self, request):
        request.side_effect = [
            _response(401, b""),
            _response(200, b'{"access_token":"opaque-unrouted-token"}'),
            _response(200, b"<schema/>"),
        ]
        self.assertEqual(download_xsd(_REPORT, _CONFIG), "<schema/>")
        self.assertEqual([call.args[:2] for call in request.call_args_list], [
            ("GET", "https://first.example" + _PATH + "?xsds"),
            ("POST", "https://base.example/ccx/oauth2/tenant/token"),
            ("GET", "https://base.example" + _PATH + "?xsds"),
        ])

    @mock.patch.object(requests.Session, "request")
    def test_xsd_without_route_uses_configured_report_url(self, request):
        request.return_value = _response(200, b"<schema/>")
        config = {"client_id": "id", "client_secret": "secret", "token_url": "https://legacy.example/token",
                  "access_token": "opaque-token"}
        self.assertEqual(download_xsd(_REPORT, config), "<schema/>")
        self.assertEqual(request.call_args.args[:2], ("GET", _REPORT + "?xsds"))

    @mock.patch("socket.getaddrinfo", return_value=_PUBLIC_DNS + [
        (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("10.0.0.2", 443)),
    ])
    @mock.patch.object(requests.adapters.HTTPAdapter, "cert_verify", side_effect=AssertionError("connection attempted"))
    def test_unsafe_seeded_destination_fails_before_bearer_request(self, cert_verify, _dns):
        with self.assertRaises(SymonException) as ctx:
            download_xsd(_REPORT, _CONFIG)
        self.assertEqual(ctx.exception.code, "workday.OAuthError")
        cert_verify.assert_not_called()

    @mock.patch("socket.getaddrinfo", return_value=_PUBLIC_DNS)
    def test_public_connection_uses_checked_ip_but_keeps_tls_and_http_identity(self, _dns):
        from tap_workday_raas.oauth_routing import oauth_session

        url = "https://cluster.example:8443/report"
        with oauth_session() as session:
            adapter = session.get_adapter(url)
            request = requests.Request("GET", url).prepare()
            pool = adapter.get_connection_with_tls_context(request, verify=True)
            self.assertEqual(pool.host, "93.184.216.34")
            self.assertEqual(pool.port, 8443)
            self.assertEqual(pool.assert_hostname, "cluster.example")
            self.assertEqual(pool.conn_kw["server_hostname"], "cluster.example")
            self.assertEqual(request.headers["Host"], "cluster.example:8443")
