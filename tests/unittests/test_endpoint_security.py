"""Exercise Requests redirect handling and urllib3's actual connection boundary."""
import json
import socket
import unittest
from unittest import mock

import requests
from urllib3.connection import HTTPSConnection
from urllib3.connectionpool import HTTPSConnectionPool

from tap_workday_raas.endpoints import (
    PublicHTTPSAdapter, PublicHTTPSConnection, PublicHTTPSConnectionPool,
    configure_api_session,
)
from tap_workday_raas.oauth_middleware import WorkdayOAuthTokenProvider, WorkdayOAuthError

PUBLIC = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("8.8.8.8", 443))]
PRIVATE = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", 443))]
CONFIG = {"auth_type": "oauth", "client_id": "id", "client_secret": "secret",
          "token_url": "https://login.customer.example/token", "oauth_grant_type": "refresh_token",
          "refresh_token": "refresh", "disable_ssl_verification": True}


def response(request, body, status=200):
    result = requests.Response()
    result.status_code = status
    result.request = request
    result.url = request.url
    result._content = body
    return result


class EndpointSecurityTest(unittest.TestCase):
    @mock.patch("tap_workday_raas.endpoints.socket.getaddrinfo", return_value=PUBLIC)
    def test_real_requests_refresh_does_not_follow_307_and_verifies_tls(self, dns):
        provider = WorkdayOAuthTokenProvider.from_config(CONFIG, verify=False)
        provider._http.trust_env = False
        calls = []
        def send(adapter, request, **kwargs):
            calls.append(request.url)
            self.assertTrue(kwargs["verify"])
            self.assertIn("refresh_token=refresh", request.body)
            if len(calls) == 1:
                return response(request, json.dumps({"access_token": "seed"}).encode())
            result = response(request, b"{}", 307)
            result.headers["Location"] = "http://127.0.0.1/credentials"
            return result
        # Retain Session.send's real redirect handling; only replace adapter I/O.
        with mock.patch.object(PublicHTTPSAdapter, "send", autospec=True, side_effect=send):
            self.assertEqual(provider.get_access_token(), "seed")
            with self.assertRaises(WorkdayOAuthError):
                provider.force_refresh()
        self.assertEqual(calls, [CONFIG["token_url"], CONFIG["token_url"]])

    def test_real_api_transport_rejects_rebound_private_resolution(self):
        session = requests.Session()
        session.trust_env = False
        with mock.patch("tap_workday_raas.endpoints.socket.getaddrinfo", side_effect=[PUBLIC, PUBLIC, PRIVATE]) as dns:
            configure_api_session(session, {"base_url": "https://api.customer.example"})
            with mock.patch("tap_workday_raas.endpoints.socket.socket") as tcp:
                with self.assertRaises(requests.exceptions.ConnectionError):
                    session.get("https://legacy.workday.com/report")
                tcp.assert_not_called()
            self.assertEqual(dns.call_count, 3)
        pool = session.get_adapter("https://").poolmanager.connection_from_url("https://api.customer.example")
        self.assertIsInstance(pool, PublicHTTPSConnectionPool)
        self.assertIs(pool.ConnectionCls, PublicHTTPSConnection)
        self.assertIs(HTTPSConnectionPool.ConnectionCls, HTTPSConnection)

    def test_real_token_transport_rejects_rebound_private_resolution(self):
        provider = WorkdayOAuthTokenProvider.from_config(CONFIG, verify=False)
        provider._http.trust_env = False
        with mock.patch("tap_workday_raas.endpoints.socket.getaddrinfo", side_effect=[PUBLIC, PRIVATE]) as dns:
            with mock.patch("tap_workday_raas.endpoints.socket.socket") as tcp:
                with self.assertRaises(WorkdayOAuthError):
                    provider.get_access_token()
                tcp.assert_not_called()
            self.assertEqual(dns.call_count, 2)

    def test_checked_sockaddr_is_connected_without_second_resolution(self):
        connection = PublicHTTPSConnection("api.customer.example", timeout=2)
        with mock.patch("tap_workday_raas.endpoints.socket.getaddrinfo", return_value=PUBLIC) as dns:
            with mock.patch("tap_workday_raas.endpoints.socket.socket") as tcp:
                self.assertIs(connection._new_conn(), tcp.return_value)
                tcp.return_value.connect.assert_called_once_with(("8.8.8.8", 443))
                dns.assert_called_once()
        self.assertEqual(connection.host, "api.customer.example")

    def test_effective_proxy_fails_closed_and_unselected_proxy_does_not(self):
        adapter = PublicHTTPSAdapter()
        request = requests.Request("GET", "https://public.example/report").prepare()
        with self.assertRaisesRegex(requests.exceptions.ProxyError, "Proxies are not supported"):
            adapter.send(request, proxies={"https": "http://proxy.example:8080"})
        with mock.patch.object(requests.adapters.HTTPAdapter, "send", return_value=mock.sentinel.response) as send:
            self.assertIs(adapter.send(request, proxies={"http": "http://unused.example:8080"}), mock.sentinel.response)
            self.assertTrue(send.call_args.kwargs["verify"])
        with self.assertRaises(requests.exceptions.InvalidURL):
            adapter.send(requests.Request("GET", "http://public.example/report").prepare())
