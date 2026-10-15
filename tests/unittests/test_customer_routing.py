"""Exercise real token, report streaming and discovery requests on a local transport."""

import importlib.util
import io
import json
import sys
import types
import unittest
from pathlib import Path
from unittest import mock

import requests
from requests.adapters import BaseAdapter

_ROOT = Path(__file__).resolve().parents[2]
sys.modules.setdefault("tap_workday_raas", types.ModuleType("tap_workday_raas"))


def _load(name):
    qualified = "tap_workday_raas." + name
    spec = importlib.util.spec_from_file_location(qualified, _ROOT / "tap_workday_raas" / (name + ".py"))
    module = importlib.util.module_from_spec(spec)
    sys.modules[qualified] = module
    spec.loader.exec_module(module)
    return module


_load("symon_exception")
oauth = _load("oauth_middleware")
client = _load("client")

TOKEN_URL = "https://auth.customer.example/gateway/oauth/token"
REPORT_URL = "https://configured.customer.example/ccx/service/customreport2/tenant/owner/report?prompt=A%2FB&format=csv"
REPORT_PATH = "/ccx/service/customreport2/tenant/owner/report"


class _LocalTransport(BaseAdapter):
    def __init__(self, token_responses=(), statuses=(200,)):
        self.token_responses = iter(token_responses)
        self.statuses = iter(statuses)
        self.calls = []

    def send(self, request, **kwargs):
        self.calls.append((request, kwargs))
        response = requests.Response()
        response.request = request
        response.url = request.url
        response.status_code = 200
        if request.method == "POST":
            body = json.dumps(next(self.token_responses)).encode()
        else:
            response.status_code = next(self.statuses)
            body = b'<schema>local XSD</schema>' if request.url.endswith("?xsds") else b'{"Report_Entry":[{"id":"1"},{"id":"2"}]}'
        response.raw = io.BytesIO(body)
        response.encoding = "utf-8"
        return response

    def close(self):
        pass


class TestCustomerRouting(unittest.TestCase):
    def setUp(self):
        self.config = {
            "auth_type": "oauth", "client_id": "id", "client_secret": "secret",
            "token_url": TOKEN_URL, "access_token": "opaque.not-a-jwt.token",
            "reports": [{"report_name": "report", "report_url": REPORT_URL}],
        }

    def _sessions(self, transport):
        real_session = requests.Session

        def session_factory():
            session = real_session()
            session.trust_env = False
            session.mount("https://", transport)
            return session

        return mock.patch.object(requests, "Session", side_effect=session_factory)

    def _assert_transport_security(self, transport):
        for request, kwargs in transport.calls:
            self.assertIs(kwargs["verify"], True)
            if request.method == "GET":
                self.assertTrue(request.headers["Authorization"].startswith("Bearer "))

    def test_seeded_instance_service_and_report_fallback_for_both_requests(self):
        cases = [
            ({"instance_url": "https://instance.customer.example/prefix/", "service_url": "https://service.customer.example/other"}, "https://instance.customer.example/prefix"),
            ({"instance_url": " ", "service_url": "https://service.customer.example/prefix"}, "https://service.customer.example/prefix"),
            ({}, "https://configured.customer.example"),
        ]
        for routing, base in cases:
            with self.subTest(routing=routing):
                config = dict(self.config, **routing)
                transport = _LocalTransport(statuses=(200, 200))
                with self._sessions(transport):
                    self.assertEqual(list(client.stream_report(REPORT_URL, config)), [{"id": "1"}, {"id": "2"}])
                    self.assertEqual(client.download_xsd(REPORT_URL, config), '<schema>local XSD</schema>')
                self.assertEqual([req.url for req, _ in transport.calls], [
                    base + REPORT_PATH + "?prompt=A%2FB&format=json",
                    base + REPORT_PATH + "?xsds",
                ])
                self.assertEqual(transport.calls[0][0].headers["Authorization"], "Bearer opaque.not-a-jwt.token")
                self._assert_transport_security(transport)

    def test_401_refresh_replaces_route_for_stream_and_xsd(self):
        responses = [
            ({"instance_url": "https://new.customer.example/prefix", "service_url": "https://ignored.customer.example"}, "https://new.customer.example/prefix"),
            ({"service_url": "https://new.customer.example/service"}, "https://new.customer.example/service"),
            ({}, "https://seed.customer.example/seed"),
            ({"instance_url": "", "service_url": " "}, "https://seed.customer.example/seed"),
        ]
        for fetch in ("stream", "xsd"):
            for metadata, base in responses:
                with self.subTest(fetch=fetch, metadata=metadata):
                    config = dict(self.config, instance_url="https://seed.customer.example/seed")
                    token = dict(metadata, access_token="fresh-opaque-token", expires_in=3600)
                    transport = _LocalTransport([token], statuses=(401, 200))
                    with self._sessions(transport):
                        if fetch == "stream":
                            self.assertEqual(len(list(client.stream_report(REPORT_URL, config))), 2)
                            query = "?prompt=A%2FB&format=json"
                        else:
                            self.assertEqual(client.download_xsd(REPORT_URL, config), '<schema>local XSD</schema>')
                            query = "?xsds"
                    self.assertEqual([req.url for req, _ in transport.calls], [
                        "https://seed.customer.example/seed" + REPORT_PATH + query,
                        TOKEN_URL,
                        base + REPORT_PATH + query,
                    ])
                    self.assertEqual(transport.calls[-1][0].headers["Authorization"], "Bearer fresh-opaque-token")
                    self._assert_transport_security(transport)

    def test_initial_exchange_and_shared_discovery_session_refresh_retain_route(self):
        config = dict(self.config)
        del config["access_token"]
        transport = _LocalTransport([
            {"access_token": "initial", "instance_url": "https://instance.customer.example/gateway", "service_url": "https://ignored.customer.example"},
            {"access_token": "second", "service_url": "https://service.customer.example/changed"},
            {"access_token": "third"},
        ], statuses=(200, 200, 200))
        with self._sessions(transport):
            session, provider = client._session_for_config(config)
            for _ in range(3):
                self.assertEqual(client.download_xsd(REPORT_URL, config, session, provider), '<schema>local XSD</schema>')
                if _ < 2:
                    provider.force_refresh()
        gets = [req for req, _ in transport.calls if req.method == "GET"]
        self.assertEqual([req.url for req in gets], [
            "https://instance.customer.example/gateway" + REPORT_PATH + "?xsds",
            "https://service.customer.example/changed" + REPORT_PATH + "?xsds",
            "https://service.customer.example/changed" + REPORT_PATH + "?xsds",
        ])
        self.assertEqual([req.headers["Authorization"] for req in gets], ["Bearer initial", "Bearer second", "Bearer third"])
        self.assertEqual([req.url for req, _ in transport.calls if req.method == "POST"], [TOKEN_URL] * 3)
        self._assert_transport_security(transport)

    def test_basic_auth_ignores_oauth_routing(self):
        config = {"username": "user", "password": "pass", "instance_url": "https://unused.customer.example"}
        transport = _LocalTransport(statuses=(200, 200))
        with self._sessions(transport):
            self.assertEqual(len(list(client.stream_report(REPORT_URL, config))), 2)
            client.download_xsd(REPORT_URL, config)
        for req, kwargs in transport.calls:
            self.assertTrue(req.url.startswith("https://configured.customer.example" + REPORT_PATH))
            self.assertEqual(req.headers["Authorization"], "Basic dXNlcjpwYXNz")
            self.assertIs(kwargs["verify"], True)


if __name__ == "__main__":
    unittest.main()
