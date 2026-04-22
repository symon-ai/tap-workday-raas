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
                "refresh_token": "rt",
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
