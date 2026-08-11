"""Tests for optional api_base_url rewriting of RaaS report request URLs."""

import importlib.util
import sys
import types
import unittest
from pathlib import Path
from unittest import mock

_ROOT = Path(__file__).resolve().parents[2]


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


sys.modules.setdefault("tap_workday_raas", types.ModuleType("tap_workday_raas"))
_load_module("tap_workday_raas.symon_exception", _ROOT / "tap_workday_raas" / "symon_exception.py")
_load_module("tap_workday_raas.oauth_middleware", _ROOT / "tap_workday_raas" / "oauth_middleware.py")
_client_mod = _load_module("tap_workday_raas.client", _ROOT / "tap_workday_raas" / "client.py")

resolve_raas_request_url = _client_mod.resolve_raas_request_url
stream_report = _client_mod.stream_report
download_xsd = _client_mod.download_xsd

_REPORT_URL = (
    "https://wd2-impl-services1.workday.com/ccx/service/customreport2/"
    "talend_dpt1/lmcneil/Stitch_Testing_2"
)


class TestResolveRaasRequestUrl(unittest.TestCase):
    def test_absent_api_base_url_returns_original(self):
        self.assertEqual(
            resolve_raas_request_url(_REPORT_URL, {}),
            _REPORT_URL,
        )

    def test_blank_api_base_url_returns_original(self):
        self.assertEqual(
            resolve_raas_request_url(_REPORT_URL, {"api_base_url": "  "}),
            _REPORT_URL,
        )

    def test_native_rest_base_rewrites_origin(self):
        rewritten = resolve_raas_request_url(
            _REPORT_URL,
            {
                "api_base_url": "https://wd5-services1.myworkday.com/ccx/api/v1/talend_dpt1",
            },
        )
        self.assertEqual(
            rewritten,
            "https://wd5-services1.myworkday.com/ccx/service/customreport2/"
            "talend_dpt1/lmcneil/Stitch_Testing_2",
        )

    def test_native_rest_base_strips_trailing_slash(self):
        rewritten = resolve_raas_request_url(
            _REPORT_URL,
            {
                "api_base_url": "https://wd5-services1.myworkday.com/ccx/api/v1/talend_dpt1/",
            },
        )
        self.assertEqual(
            rewritten,
            "https://wd5-services1.myworkday.com/ccx/service/customreport2/"
            "talend_dpt1/lmcneil/Stitch_Testing_2",
        )

    def test_opaque_proxy_preserves_path_prefix(self):
        rewritten = resolve_raas_request_url(
            _REPORT_URL,
            {"api_base_url": "https://api-proxy.example.com/workday"},
        )
        self.assertEqual(
            rewritten,
            "https://api-proxy.example.com/workday/ccx/service/customreport2/"
            "talend_dpt1/lmcneil/Stitch_Testing_2",
        )

    def test_proxy_with_rest_path_keeps_prefix(self):
        rewritten = resolve_raas_request_url(
            _REPORT_URL,
            {
                "api_base_url": (
                    "https://api-proxy.example.com/workday/ccx/api/v1/talend_dpt1"
                ),
            },
        )
        self.assertEqual(
            rewritten,
            "https://api-proxy.example.com/workday/ccx/service/customreport2/"
            "talend_dpt1/lmcneil/Stitch_Testing_2",
        )

    def test_preserves_query_string(self):
        url = _REPORT_URL + "?Effective_as_of_Date=2024-01-01&format=xml"
        rewritten = resolve_raas_request_url(
            url,
            {"api_base_url": "https://api-proxy.example.com/workday"},
        )
        self.assertEqual(
            rewritten,
            "https://api-proxy.example.com/workday/ccx/service/customreport2/"
            "talend_dpt1/lmcneil/Stitch_Testing_2"
            "?Effective_as_of_Date=2024-01-01&format=xml",
        )

    def test_invalid_api_base_url_raises(self):
        with self.assertRaises(ValueError) as ctx:
            resolve_raas_request_url(_REPORT_URL, {"api_base_url": "not-a-url"})
        self.assertIn("api_base_url", str(ctx.exception))

    def test_ignores_report_url_host_when_api_base_url_set(self):
        rewritten = resolve_raas_request_url(
            "https://stale-host.example.com/ccx/service/customreport2/t/u/R",
            {"api_base_url": "https://api-proxy.example.com/workday"},
        )
        self.assertEqual(
            rewritten,
            "https://api-proxy.example.com/workday/ccx/service/customreport2/t/u/R",
        )
        self.assertNotIn("stale-host", rewritten)

    def test_requires_service_path_fragments_when_api_base_url_set(self):
        with self.assertRaises(ValueError) as ctx:
            resolve_raas_request_url(
                "https://wd2-impl-services1.workday.com/not-a-raas-path",
                {"api_base_url": "https://api-proxy.example.com/workday"},
            )
        self.assertIn("/ccx/service", str(ctx.exception))


class _StreamingReportResponse:
    def __init__(self, body: bytes, status_code: int = 200):
        self._body = body
        self.status_code = status_code

    def iter_content(self, chunk_size=512):
        yield self._body

    def raise_for_status(self):
        return None

    def close(self):
        return None


class _GetContext:
    def __init__(self, response):
        self._response = response

    def __enter__(self):
        return self._response

    def __exit__(self, *args):
        return False


class TestClientUsesApiBaseUrl(unittest.TestCase):
    @mock.patch.object(_client_mod, "_session_for_config")
    def test_stream_report_gets_rewritten_url(self, mock_session_for_config):
        session = mock.Mock()
        body = b'{"Report_Entry": [{"row_id": "1"}]}'
        session.get.return_value = _GetContext(_StreamingReportResponse(body))
        mock_session_for_config.return_value = (session, None)

        config = {
            "username": "u",
            "password": "p",
            "api_base_url": "https://api-proxy.example.com/workday",
        }
        list(stream_report(_REPORT_URL, config))

        session.get.assert_called_once()
        called_url = session.get.call_args[0][0]
        self.assertEqual(
            called_url,
            "https://api-proxy.example.com/workday/ccx/service/customreport2/"
            "talend_dpt1/lmcneil/Stitch_Testing_2?format=json",
        )

    @mock.patch.object(_client_mod, "_session_for_config")
    def test_download_xsd_gets_rewritten_url(self, mock_session_for_config):
        session = mock.Mock()
        response = mock.Mock()
        response.status_code = 200
        response.text = "<xsd/>"
        response.raise_for_status.return_value = None
        session.get.return_value = response
        mock_session_for_config.return_value = (session, None)

        config = {
            "username": "u",
            "password": "p",
            "api_base_url": "https://wd5-services1.myworkday.com/ccx/api/v1/talend_dpt1",
        }
        self.assertEqual(download_xsd(_REPORT_URL, config), "<xsd/>")

        session.get.assert_called_once()
        called_url = session.get.call_args[0][0]
        self.assertEqual(
            called_url,
            "https://wd5-services1.myworkday.com/ccx/service/customreport2/"
            "talend_dpt1/lmcneil/Stitch_Testing_2?xsds",
        )


if __name__ == "__main__":
    unittest.main()
