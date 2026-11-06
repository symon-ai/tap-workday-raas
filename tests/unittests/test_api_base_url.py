"""API routing through real sessions, OAuth refresh and a local HTTP boundary."""
import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest import mock

import requests

from tap_workday_raas.oauth_middleware import WorkdayApiSession, WorkdayOAuthTokenProvider
from tap_workday_raas.client import _session_for_config as make_session
from tap_workday_raas.client import stream_report, download_xsd


class TestApiBaseUrl(unittest.TestCase):
    def test_default_and_basic_override_keep_paths_queries_and_tls(self):
        for base, expected in [(None, "https://legacy.example/ccx/service/t/API/v1?wsdl"),
                               ("https://custom.example/root/", "https://custom.example/root/ccx/service/t/API/v1?wsdl")]:
            session = make_session({"username": "user", "password": "secret", "base_url": base})[0]
            with mock.patch.object(session, "send", return_value=requests.Response()) as send:
                session.get("https://legacy.example/ccx/service/t/API/v1?wsdl")
            self.assertEqual(send.call_args.args[0].url, expected)
            self.assertTrue(send.call_args.kwargs["verify"])
            self.assertEqual(send.call_args.args[0].headers["Authorization"], "Basic dXNlcjpzZWNyZXQ=")

    def test_token_endpoint_precedence_expiry_refresh_and_omission(self):
        http = mock.Mock()
        bodies = [
            {"access_token": "opaque-one", "instance_url": "https://instance.example/", "service_url": "https://ignored.example"},
            {"access_token": "opaque-two", "service_url": "https://service.example"},
            {"access_token": "opaque-three"},
        ]
        http.post.side_effect = [mock.Mock(status_code=200, json=mock.Mock(return_value=body)) for body in bodies]
        provider = WorkdayOAuthTokenProvider("id", "secret", "https://auth.example/token", session=http)
        session = WorkdayApiSession("https://configured.example")
        provider.apply_to_session(session)
        with mock.patch.object(session, "send", return_value=requests.Response()) as send:
            session.get("https://legacy.example/ccx/service/t/API/v1?wsdl")
            first = send.call_args.args[0]
            self.assertEqual(first.url, "https://instance.example/ccx/service/t/API/v1?wsdl")
            self.assertEqual(first.headers["Authorization"], "Bearer opaque-one")
            provider._expires_at = 0
            session.post("https://legacy.example/ccx/service/t/API/v1", data=b"soap")
            second = send.call_args.args[0]
            self.assertEqual(second.url, "https://service.example/ccx/service/t/API/v1")
            self.assertEqual(second.headers["Authorization"], "Bearer opaque-two")
            provider.force_refresh()
            session.get("https://legacy.example/report?xsds")
            third = send.call_args.args[0]
            self.assertEqual(third.url, "https://service.example/report?xsds")
            self.assertEqual(third.headers["Authorization"], "Bearer opaque-three")
        self.assertEqual(http.post.call_count, 3)
        for call in http.post.call_args_list:
            self.assertEqual(call.args[0], "https://auth.example/token")
            self.assertTrue(call.kwargs["verify"])

    def test_token_without_endpoint_uses_configured_or_legacy_url(self):
        for base in [None, "https://configured.example/"]:
            provider = WorkdayOAuthTokenProvider("id", "secret", "https://auth.example/token", session=mock.Mock())
            provider._http.post.return_value = mock.Mock(status_code=200, json=lambda: {"access_token": "not.a.jwt"})
            session = WorkdayApiSession(base)
            provider.apply_to_session(session)
            with mock.patch.object(session, "send", return_value=requests.Response()) as send:
                session.get("https://legacy.example/report?x=1")
            self.assertEqual(send.call_args.args[0].url, (base or "https://legacy.example/") + "report?x=1")

    def test_real_http_factory_and_refresh_routing(self):
        observed = []
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass
            def do_POST(self):
                data = self.rfile.read(int(self.headers.get("Content-Length", 0)))
                observed.append((self.command, self.path, self.headers.get("Authorization"), data))
                if self.path == "/token":
                    token_number = sum(path == "/token" for _, path, _, _ in observed)
                    body = {"access_token": "opaque-" + str(token_number), "instance_url": root + "/api" + str(token_number)}
                else:
                    body = {"ok": True}
                self.send_response(200)
                self.end_headers()
                self.wfile.write(json.dumps(body).encode())
            def do_GET(self):
                observed.append((self.command, self.path, self.headers.get("Authorization"), b""))
                if self.path.startswith("/api3/report"):
                    self.send_response(401)
                    self.end_headers()
                    return
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"<schema/>" if self.path.endswith("?xsds") else b'{"Report_Entry": [{"id": "1"}]}')
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        root = "http://127.0.0.1:" + str(server.server_port)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            config = {"auth_type": "oauth", "client_id": "id", "client_secret": "secret", "token_url": root + "/token", "base_url": root + "/configured"}
            result = make_session(config)
            session, provider = result[-2:]
            response = session.get("https://unreachable.invalid/ccx/service/t/API/v1?wsdl")
            self.assertEqual(response.status_code, 200)
            provider.force_refresh()
            response = session.post("https://unreachable.invalid/ccx/service/t/API/v1", data=b"<opaque-soap/>")
            self.assertEqual(response.status_code, 200)
            self.assertEqual([row[1] for row in observed], ["/token", "/api1/ccx/service/t/API/v1?wsdl", "/token", "/api2/ccx/service/t/API/v1"])
            self.assertEqual(observed[1][2], "Bearer opaque-1")
            self.assertEqual(observed[3][2:], ("Bearer opaque-2", b"<opaque-soap/>"))
            self.assertEqual(list(stream_report("https://unreachable.invalid/report?format=csv&key=value", config)), [{"id": "1"}])
            self.assertEqual(download_xsd("https://unreachable.invalid/report?key=value", config), "<schema/>")
            self.assertEqual([row[1] for row in observed[4:]], [
                "/token", "/api3/report?key=value&format=json",
                "/token", "/api4/report?key=value&format=json",
                "/token", "/api5/report?xsds",
            ])
        finally:
            server.shutdown()
            server.server_close()
            thread.join()


if __name__ == "__main__":
    unittest.main()
