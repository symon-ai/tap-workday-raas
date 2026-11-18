"""Exercise report and schema requests through the real session and token parser."""

import json
import unittest
from unittest import mock

import requests

from tap_workday_raas.client import download_xsd, stream_report
from tap_workday_raas.symon_exception import SymonException


REPORT_URL = "https://legacy.workday.com/ccx/service/customreport2/tenant/owner/Report%20Name?limit=2&filter=a%3Fb&format=csv"
REPORT_PATH = "/ccx/service/customreport2/tenant/owner/Report%20Name"


def response(request, content, status=200):
    result = requests.Response()
    result.status_code = status
    result.url = request.url
    result.request = request
    result._content = content
    result._content_consumed = True
    return result


class TestReportRouting(unittest.TestCase):
    def test_report_401_uses_new_metadata_and_preserves_query(self):
        config = {
            "auth_type": "oauth", "client_id": "id", "client_secret": "secret",
            "token_url": "https://identity.example.com/exchange",
            "base_url": "https://configured.example.com/old",
        }
        tokens = iter([
            {"access_token": "one", "instance_url": "https://first.example.com/prefix", "service_url": "https://ignored.example.com"},
            {"access_token": "two", "service_url": "https://second.example.com/new/prefix/"},
        ])
        seen = []

        def send(session, request, **kwargs):
            seen.append((request.url, request.headers.get("Authorization"), kwargs.get("verify")))
            if request.url == config["token_url"]:
                return response(request, json.dumps(next(tokens)).encode())
            if request.headers["Authorization"] == "Bearer one":
                return response(request, b"", status=401)
            return response(request, b'{"Report_Entry":[{"id":"1"}]}')

        with mock.patch.object(requests.Session, "send", autospec=True, side_effect=send):
            self.assertEqual(list(stream_report(REPORT_URL, config)), [{"id": "1"}])
        api = [item for item in seen if item[0] != config["token_url"]]
        query = "?limit=2&filter=a%3Fb&format=json"
        self.assertEqual(api, [
            ("https://first.example.com/prefix" + REPORT_PATH + query, "Bearer one", True),
            ("https://second.example.com/new/prefix" + REPORT_PATH + query, "Bearer two", True),
        ])
        self.assertEqual(sum(item[0] == config["token_url"] for item in seen), 2)

    def test_schema_refresh_without_metadata_restores_configured_base(self):
        config = {
            "auth_type": "oauth", "client_id": "id", "client_secret": "secret",
            "token_url": "https://identity.example.com/exchange",
            "base_url": "https://configured.example.com/prefix",
        }
        tokens = iter([
            {"access_token": "one", "instance_url": "  ", "service_url": "https://first.example.com/prefix"},
            {"access_token": "two"},
        ])
        urls = []

        def send(session, request, **kwargs):
            if request.url == config["token_url"]:
                return response(request, json.dumps(next(tokens)).encode())
            urls.append(request.url)
            return response(request, b"<schema/>", 401 if len(urls) == 1 else 200)

        with mock.patch.object(requests.Session, "send", autospec=True, side_effect=send):
            self.assertEqual(download_xsd(REPORT_URL, config), "<schema/>")
        self.assertEqual(urls, [
            "https://first.example.com/prefix" + REPORT_PATH + "?xsds",
            "https://configured.example.com/prefix" + REPORT_PATH + "?xsds",
        ])

    def test_basic_configured_and_unchanged_report_fallback(self):
        for base_url, expected in [
            ("https://customer.example.com/proxy/", "https://customer.example.com/proxy" + REPORT_PATH),
            (None, "https://legacy.workday.com" + REPORT_PATH),
            ("   ", "https://legacy.workday.com" + REPORT_PATH),
        ]:
            with self.subTest(base_url=base_url):
                urls = []

                def send(session, request, **kwargs):
                    urls.append(request.url)
                    return response(request, b'{"Report_Entry":[{"id":"1"}]}')

                with mock.patch.object(requests.Session, "send", autospec=True, side_effect=send):
                    self.assertEqual(list(stream_report(REPORT_URL, {
                        "username": "user", "password": "secret", "base_url": base_url,
                    })), [{"id": "1"}])
                self.assertEqual(urls, [expected + "?limit=2&filter=a%3Fb&format=json"])

    def test_non_workday_report_path_remains_intact(self):
        with mock.patch.object(requests.Session, "send", autospec=True) as send:
            send.side_effect = lambda session, request, **kwargs: response(request, b"<schema/>")
            self.assertEqual(download_xsd("https://reports.example.com/custom/path/report?x=1", {
                "username": "user", "password": "secret", "base_url": "https://gateway.example.com/proxy",
            }), "<schema/>")
        self.assertEqual(send.call_args.args[1].url, "https://gateway.example.com/proxy/custom/path/report?xsds")

    def test_rejects_unsafe_base_url_shapes(self):
        for base_url in ("http://customer.example.com", "https://user:secret@customer.example.com", "https://customer.example.com?token=secret", "https://customer.example.com/#fragment"):
            with self.subTest(base_url=base_url), self.assertRaises(SymonException) as error:
                download_xsd(REPORT_URL, {
                    "username": "user", "password": "secret", "base_url": base_url,
                })
            self.assertEqual(error.exception.code, "workday.InvalidConfig")
