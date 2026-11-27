"""Exercise API base selection through config, OAuth, report streaming and XSD I/O."""
import json
import unittest
from io import BytesIO
from unittest import mock

import requests

from tap_workday_raas.client import _session_for_config, download_xsd, stream_report
from tap_workday_raas.oauth_middleware import WorkdayOAuthError
from tap_workday_raas.symon_exception import SymonException

REPORT_PATH = "/report-prefix/ccx/service/customreport2/tenant/user/report"
REPORT_QUERY = "filter=a%2Fb&empty=&ids=1&ids=2"
REPORT_URL = "https://legacy.workday.com" + REPORT_PATH + "?" + REPORT_QUERY + "&format=csv"
TOKEN_URL = "https://login.customer.example/oauth/token?audience=workday"


class Network:
    def __init__(self, tokens=(), statuses=()):
        self.tokens = iter(tokens)
        self.statuses = iter(statuses)
        self.requests = []

    def send(self, session, request, **kwargs):
        self.requests.append((request, kwargs))
        response = requests.Response()
        response.status_code = 200
        response.url = request.url
        response.request = request
        if request.url == TOKEN_URL:
            body = json.dumps(next(self.tokens)).encode()
        else:
            response.status_code = next(self.statuses, 200)
            body = b'<schema/>' if request.url.endswith("&xsds") else b'{"Report_Entry":[{"id":"1"},{"id":"2"}]}'
        response._content = body
        response.raw = BytesIO(body)
        return response

    def patch(self):
        return mock.patch.object(requests.sessions.Session, "send", autospec=True, side_effect=self.send)

    def api_requests(self):
        return [r for r, _ in self.requests if r.url != TOKEN_URL]


class TestApiBaseUrl(unittest.TestCase):
    def config(self, **extra):
        return {"auth_type": "oauth", "client_id": "id", "client_secret": "secret",
                "token_url": TOKEN_URL, "reports": [{"report_url": REPORT_URL}], **extra}

    def test_configured_base_retains_report_path_and_query_for_stream_and_schema(self):
        for schema in (False, True):
            with self.subTest(schema=schema):
                network = Network([{"access_token": "opaque-not-a-jwt"}])
                with network.patch():
                    config = self.config(base_url="https://api.customer.example/gateway/")
                    if schema:
                        self.assertEqual(download_xsd(REPORT_URL, config), "<schema/>")
                    else:
                        self.assertEqual(list(stream_report(REPORT_URL, config)), [{"id": "1"}, {"id": "2"}])
                request = network.api_requests()[0]
                self.assertEqual(request.url, "https://api.customer.example/gateway" + REPORT_PATH + "?" + REPORT_QUERY + ("&xsds" if schema else "&format=json"))
                self.assertEqual(request.headers["Authorization"], "Bearer opaque-not-a-jwt")
                self.assertTrue(all(options["verify"] for _, options in network.requests))
                self.assertEqual(len(network.api_requests()), 1)
                self.assertEqual(network.requests[0][0].url, TOKEN_URL)

    def test_returned_instance_priority_and_service_fallback(self):
        for fields, expected in [
            ({"instance_url": "https://instance.example/prefix", "service_url": "https://ignored.example"}, "https://instance.example/prefix"),
            ({"service_url": "https://service.example/prefix"}, "https://service.example/prefix"),
        ]:
            with self.subTest(fields=fields):
                network = Network([{"access_token": "opaque", **fields}])
                with network.patch():
                    list(stream_report(REPORT_URL, self.config(base_url="https://configured.example/base")))
                self.assertEqual(network.api_requests()[0].url, expected + REPORT_PATH + "?" + REPORT_QUERY + "&format=json")

    def test_report_401_refresh_changes_api_destination_and_token(self):
        network = Network([
            {"access_token": "initial"},
            {"access_token": "refreshed", "service_url": "https://refresh.example/new"},
        ], [401, 200])
        with network.patch():
            rows = list(stream_report(REPORT_URL, self.config(base_url="https://configured.example/base")))
        self.assertEqual(rows, [{"id": "1"}, {"id": "2"}])
        requests_sent = network.api_requests()
        suffix = REPORT_PATH + "?" + REPORT_QUERY + "&format=json"
        self.assertEqual([r.url for r in requests_sent], ["https://configured.example/base" + suffix, "https://refresh.example/new" + suffix])
        self.assertEqual([r.headers["Authorization"] for r in requests_sent], ["Bearer initial", "Bearer refreshed"])
        self.assertEqual(sum(r.url == TOKEN_URL for r, _ in network.requests), 2)

    def test_schema_401_and_proactive_refresh_use_current_base_on_shared_session(self):
        network = Network([
            {"access_token": "initial"},
            {"access_token": "refreshed", "instance_url": "https://refresh.example/new"},
            {"access_token": "expired", "service_url": "https://later.example/final"},
        ], [401, 200, 200])
        config = self.config(base_url="https://configured.example/base")
        with network.patch():
            session, provider = _session_for_config(config)
            self.assertEqual(download_xsd(REPORT_URL, config, session, provider), "<schema/>")
            provider._expires_at = 0
            self.assertEqual(download_xsd(REPORT_URL, config, session, provider), "<schema/>")
        suffix = REPORT_PATH + "?" + REPORT_QUERY + "&xsds"
        self.assertEqual([r.url for r in network.api_requests()], ["https://configured.example/base" + suffix,
            "https://refresh.example/new" + suffix, "https://later.example/final" + suffix])
        self.assertEqual(sum(r.url == TOKEN_URL for r, _ in network.requests), 3)

    def test_default_oauth_and_basic_urls_and_basic_override(self):
        configs = [(self.config(), "https://legacy.workday.com"),
                   ({"auth_type": "basic", "username": "u", "password": "p"}, "https://legacy.workday.com"),
                   ({"auth_type": "basic", "username": "u", "password": "p", "base_url": "https://basic.example/proxy"}, "https://basic.example/proxy")]
        for config, base in configs:
            with self.subTest(base=base, auth=config["auth_type"]):
                network = Network([{"access_token": "opaque"}])
                with network.patch():
                    list(stream_report(REPORT_URL, config))
                self.assertEqual(network.api_requests()[0].url, base + REPORT_PATH + "?" + REPORT_QUERY + "&format=json")
                if config["auth_type"] == "basic":
                    self.assertEqual(network.api_requests()[0].headers["Authorization"], "Basic dTpw")
                    self.assertEqual(len(network.requests), 1)

    def test_already_prefixed_report_does_not_stack_base_prefix(self):
        network = Network([{"access_token": "opaque", "instance_url": "https://instance.example/new"}])
        config = self.config(base_url="https://configured.example/gateway")
        with network.patch():
            list(stream_report(config["base_url"] + REPORT_PATH + "?" + REPORT_QUERY, config))
        self.assertEqual(network.api_requests()[0].url, "https://instance.example/new" + REPORT_PATH + "?" + REPORT_QUERY + "&format=json")

    def test_invalid_configured_and_returned_bases_fail_before_report_io(self):
        for base in ("http://api.example", "https://u:p@api.example", "file:///tmp/wsdl",
                     "https://api.example?x=1", "https://api.example/#fragment", "https://api.example:bad",
                     "https://api.example/ bad", "https://api.example\\evil"):
            with self.subTest(base=base):
                network = Network([{"access_token": "opaque", "instance_url": base}])
                with network.patch():
                    with self.assertRaises(SymonException):
                        list(stream_report(REPORT_URL, self.config(base_url=base)))
                    self.assertEqual(network.requests, [])
                    with self.assertRaises(WorkdayOAuthError):
                        _session_for_config(self.config())
                self.assertEqual(network.api_requests(), [])
