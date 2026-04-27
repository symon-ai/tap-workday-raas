import importlib.util
import sys
import unittest
from pathlib import Path
from unittest import mock

import requests

# Load oauth_middleware without importing tap_workday_raas.__init__ (avoids ijson/yajl).
_OAUTH_PATH = Path(__file__).resolve().parents[2] / "tap_workday_raas" / "oauth_middleware.py"
_spec = importlib.util.spec_from_file_location("tap_workday_raas.oauth_middleware", _OAUTH_PATH)
_oauth_mod = importlib.util.module_from_spec(_spec)
sys.modules["tap_workday_raas.oauth_middleware"] = _oauth_mod
_spec.loader.exec_module(_oauth_mod)

WorkdayOAuthTokenProvider = _oauth_mod.WorkdayOAuthTokenProvider
raas_config_uses_oauth = _oauth_mod.raas_config_uses_oauth
validate_raas_tap_config = _oauth_mod.validate_raas_tap_config


class TestRaasConfig(unittest.TestCase):
    def test_basic_auth_detection(self):
        c = {"username": "u", "password": "p", "reports": [{}]}
        self.assertFalse(raas_config_uses_oauth(c))

    def test_oauth_detection(self):
        c = {
            "client_id": "a",
            "client_secret": "b",
            "token_url": "https://example.com/token",
            "refresh_token": "rt",
            "reports": [{}],
        }
        self.assertTrue(raas_config_uses_oauth(c))

    def test_validate_raas_basic(self):
        validate_raas_tap_config(
            {"username": "u", "password": "p", "reports": [{"report_name": "r"}]}
        )

    def test_validate_raas_oauth(self):
        validate_raas_tap_config(
            {
                "client_id": "a",
                "client_secret": "b",
                "token_url": "https://example.com/token",
                "oauth_grant_type": "refresh_token",
                "refresh_token": "rt",
                "reports": [{"report_name": "r"}],
            }
        )

    def test_validate_oauth_client_credentials_rejects_stray_refresh_token(self):
        with self.assertRaises(ValueError) as ctx:
            validate_raas_tap_config(
                {
                    "client_id": "a",
                    "client_secret": "b",
                    "token_url": "https://example.com/token",
                    "oauth_grant_type": "client_credentials",
                    "refresh_token": "should_not_be_here",
                    "reports": [{"report_name": "r"}],
                }
            )
        self.assertIn("refresh_token", str(ctx.exception).lower())

    def test_validate_oauth_client_credentials_ok_without_refresh(self):
        validate_raas_tap_config(
            {
                "client_id": "a",
                "client_secret": "b",
                "token_url": "https://example.com/token",
                "reports": [{"report_name": "r"}],
            }
        )

    def test_validate_missing_reports(self):
        with self.assertRaises(ValueError):
            validate_raas_tap_config({"username": "u", "password": "p"})


class TestTokenProvider(unittest.TestCase):
    def test_refresh_token_grant(self):
        mock_http = mock.Mock()
        mock_resp = mock.Mock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "access_token": "tok123",
            "expires_in": 3600,
        }
        mock_http.post.return_value = mock_resp

        p = WorkdayOAuthTokenProvider(
            client_id="id",
            client_secret="sec",
            token_url="https://wd.example.com/ccx/oauth2/t/token",
            grant_type="refresh_token",
            refresh_token="rt",
            session=mock_http,
        )
        sess = requests.Session()
        p.apply_to_session(sess)
        self.assertEqual(p.get_access_token(), "tok123")
        self.assertEqual(sess.headers["Authorization"], "Bearer tok123")
        mock_http.post.assert_called_once()
        call_kw = mock_http.post.call_args[1]
        self.assertIn("Authorization", call_kw["headers"])

    def test_post_body_client_auth(self):
        mock_http = mock.Mock()
        mock_resp = mock.Mock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"access_token": "tok456", "expires_in": 3600}
        mock_http.post.return_value = mock_resp

        p = WorkdayOAuthTokenProvider(
            client_id="id",
            client_secret="sec",
            token_url="https://wd.example.com/ccx/oauth2/t/token",
            grant_type="client_credentials",
            session=mock_http,
            token_client_auth="post_body",
        )
        self.assertEqual(p.get_access_token(), "tok456")
        call_kw = mock_http.post.call_args[1]
        self.assertNotIn("Authorization", call_kw["headers"])
        self.assertEqual(call_kw["data"]["client_id"], "id")
        self.assertEqual(call_kw["data"]["client_secret"], "sec")

    def test_refresh_token_invalid_client_retries_raw_id(self):
        import base64

        uuid_str = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        b64_id = base64.b64encode(uuid_str.encode("ascii")).decode("ascii")
        mock_http = mock.Mock()
        fail = mock.Mock()
        fail.status_code = 401
        fail.text = '{"error":"invalid_client"}'
        ok = mock.Mock()
        ok.status_code = 200
        ok.json.return_value = {"access_token": "tok789", "expires_in": 3600}
        mock_http.post.side_effect = [fail, ok]

        p = WorkdayOAuthTokenProvider(
            client_id=b64_id,
            client_secret="sec",
            token_url="https://wd.example.com/ccx/oauth2/t/token",
            grant_type="refresh_token",
            refresh_token="rt",
            session=mock_http,
            client_id_format="auto",
        )
        self.assertEqual(p.get_access_token(), "tok789")
        self.assertEqual(mock_http.post.call_count, 2)
