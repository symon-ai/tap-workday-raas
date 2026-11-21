import io
import json
import socket
import unittest
from unittest import mock

import requests

from tap_workday_raas.endpoints import validate_base_url, route_api_url
from tap_workday_raas.oauth_middleware import WorkdayOAuthTokenProvider, WorkdayOAuthError
from tap_workday_raas.client import stream_report
from tap_workday_raas.discover import discover_streams

API_PATH = "/ccx/service/tenant/Human_Resources/v37.2"
LEGACY = "https://legacy.workday.com" + API_PATH
TOKEN = "https://login.customer.example/token"
CONFIG = {"auth_type": "oauth", "client_id": "id", "client_secret": "secret",
          "token_url": TOKEN, "oauth_grant_type": "refresh_token", "refresh_token": "refresh",
          "base_url": "https://configured.customer.example"}
WSDL = b'''<definitions xmlns="http://schemas.xmlsoap.org/wsdl/"
 xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/" xmlns:tns="urn:test"
 xmlns:xsd="http://www.w3.org/2001/XMLSchema" targetNamespace="urn:test">
 <types><xsd:schema targetNamespace="urn:test"><xsd:element name="Ping" type="xsd:string"/>
 <xsd:element name="Pong" type="xsd:string"/></xsd:schema></types>
 <message name="In"><part name="body" element="tns:Ping"/></message>
 <message name="Out"><part name="body" element="tns:Pong"/></message>
 <portType name="Port"><operation name="Ping"><input message="tns:In"/>
 <output message="tns:Out"/></operation></portType>
 <binding name="Binding" type="tns:Port"><soap:binding style="document"
 transport="http://schemas.xmlsoap.org/soap/http"/><operation name="Ping">
 <soap:operation soapAction="Ping"/><input><soap:body use="literal"/></input>
 <output><soap:body use="literal"/></output></operation></binding>
 <service name="Service"><port name="Port" binding="tns:Binding">
 <soap:address location="https://embedded.workday.com/ccx/service/tenant/Human_Resources/v37.2"/>
 </port></service></definitions>'''
SOAP = b'<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"><s:Body><Pong xmlns="urn:test">ok</Pong></s:Body></s:Envelope>'
XSD = b'<xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema"><xsd:complexType name="Report_EntryType"><xsd:sequence><xsd:element name="Name" type="xsd:string"/></xsd:sequence></xsd:complexType></xsd:schema>'


def response(request, body, status=200, content_type="text/xml"):
    result = requests.Response()
    result.status_code = status
    result.request = request
    result.url = request.url
    result.headers["Content-Type"] = content_type
    result._content = body
    result.raw = io.BytesIO(body)
    return result


class EndpointTest(unittest.TestCase):
    def setUp(self):
        self.dns = mock.patch("tap_workday_raas.endpoints.socket.getaddrinfo", return_value=[
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("8.8.8.8", 443))])
        self.dns.start()
        self.addCleanup(self.dns.stop)

    def test_url_contract_and_service_path(self):
        self.assertEqual(route_api_url(LEGACY + "?wsdl", "https://custom.example/ccx/service/tenant"),
                         "https://custom.example" + API_PATH + "?wsdl")
        self.assertEqual(route_api_url(LEGACY, "https://custom.example/proxy"),
                         "https://custom.example/proxy" + API_PATH)
        for value in ["http://public.example", "https://user:pass@public.example", "https://public.example/?x=1",
                      "https://public.example/#fragment", "https://127.0.0.1", "https://10.0.0.1",
                      "https://[::1]", "https://169.254.169.254", "https://public.example\\evil"]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                validate_base_url(value)
        with mock.patch("tap_workday_raas.endpoints.socket.getaddrinfo", return_value=[
                (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("192.168.1.1", 443))]):
            with self.assertRaises(ValueError):
                validate_base_url("https://private.example")

    def test_token_endpoint_never_follows_api_endpoint(self):
        provider = WorkdayOAuthTokenProvider.from_config(CONFIG)
        bodies = [
            {"access_token": "opaque.not.jwt", "instance_url": "https://instance.example", "service_url": "https://ignored.example"},
            {"access_token": "second", "service_url": "https://service.example"},
            {"access_token": "third"},
            {"access_token": "unsafe", "instance_url": "http://downgrade.example"},
        ]
        def send(session, request, **kwargs):
            self.assertEqual(request.url, TOKEN)
            self.assertIn("refresh_token=refresh", request.body)
            self.assertTrue(kwargs["verify"])
            return response(request, json.dumps(bodies.pop(0)).encode(), content_type="application/json")
        with mock.patch.object(requests.Session, "send", autospec=True, side_effect=send):
            self.assertEqual(provider.get_access_token(), "opaque.not.jwt")
            self.assertEqual(provider.api_base_url, "https://instance.example")
            provider.force_refresh()
            self.assertEqual(provider.api_base_url, "https://service.example")
            provider.force_refresh()
            self.assertEqual(provider.api_base_url, "https://service.example")
            with self.assertRaises(WorkdayOAuthError):
                provider.force_refresh()

    def test_actual_report_retry_and_discovery_routes(self):
        seen = []
        tokens = ["https://instance.example", "https://service.example"]
        first_report = True
        report = "https://legacy.workday.com/ccx/service/customreport2/tenant/owner/report?filter=active"
        config = dict(CONFIG, reports=[{"report_name": "Report", "report_url": report}])
        def send(session, request, **kwargs):
            nonlocal first_report
            seen.append((request.method, request.url))
            if request.url == TOKEN:
                return response(request, json.dumps({"access_token": "opaque", "service_url": tokens.pop(0)}).encode(), content_type="application/json")
            self.assertTrue(kwargs["verify"])
            self.assertFalse(kwargs["allow_redirects"])
            if request.url.endswith("?xsds"):
                return response(request, XSD)
            if first_report:
                first_report = False
                return response(request, b"", 401)
            return response(request, b'{"Report_Entry":[{"Name":"Alice"}]}', content_type="application/json")
        with mock.patch.object(requests.Session, "send", autospec=True, side_effect=send):
            self.assertEqual(list(stream_report(report, config)), [{"Name": "Alice"}])
            # A platform-supplied access token must route discovery before refresh.
            discovery_config = dict(config, base_url="https://restored.example", access_token="seeded")
            streams = discover_streams(discovery_config)
            self.assertEqual(streams[0]["schema"]["properties"]["Name"], {"type": ["string"]})
        path = "/ccx/service/customreport2/tenant/owner/report"
        self.assertEqual(seen, [("POST", TOKEN), ("GET", "https://instance.example" + path + "?filter=active&format=json"),
                                ("POST", TOKEN), ("GET", "https://service.example" + path + "?filter=active&format=json"),
                                ("GET", "https://restored.example" + path + "?xsds")])

    def test_basic_default_and_custom_report_routes(self):
        report = "https://reports.workday.com/report?filter=active"
        for base in [None, "https://configured.example/proxy"]:
            seen = []
            config = {"username": "user", "password": "password"}
            if base:
                config["base_url"] = base
            def send(session, request, **kwargs):
                seen.append(request.url)
                self.assertTrue(request.headers["Authorization"].startswith("Basic "))
                return response(request, b'{"Report_Entry":[]}', content_type="application/json")
            with mock.patch.object(requests.Session, "send", autospec=True, side_effect=send):
                self.assertEqual(list(stream_report(report, config)), [])
            self.assertEqual(seen, [(base + "/report" if base else "https://reports.workday.com/report") + "?filter=active&format=json"])
