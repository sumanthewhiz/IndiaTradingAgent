from __future__ import annotations

import io
import json
import tempfile
import unittest
import urllib.error
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, patch

from india_trader.autonomy import PreparationBlocked, prepare_session
from india_trader.broker import BrokerReadUnavailable, BrokerRejected, KiteHTTP, SubmissionUnknown
from india_trader.core import Config, IST
from india_trader.dashboard import Controller


class BrokerErrorTests(unittest.TestCase):
    def setUp(self):
        self.http = KiteHTTP(Config(), False, api_key="test-only-app-key",
                             access_token="test-only-access-token")
        self.http.opener = Mock()

    def response(self, code=403, error_type="PermissionException",
                 message="Insufficient permission for that call.", body=None):
        content = json.dumps({"status": "error", "error_type": error_type,
                              "message": message}).encode() if body is None else body
        stream = io.BytesIO(content)
        error = urllib.error.HTTPError("https://api.kite.trade/quote?secret=never-print-query",
                                      code, "must-not-use-HTTP-reason", {}, stream)
        self.http.opener.open.side_effect = error
        return stream

    def test_quote_permission_names_the_data_entitlement_not_expired_login(self):
        stream = self.response()
        with self.assertRaises(BrokerRejected) as caught:
            self.http.request("GET", "/quote", query=[("i", "NSE:TEST")])
        error = caught.exception
        self.assertEqual(error.status_code, 403)
        self.assertEqual(error.error_type, "PermissionException")
        self.assertEqual(error.endpoint, "/quote")
        self.assertEqual(error.method, "GET")
        self.assertIn("Insufficient permission for that call.", str(error))
        self.assertIn("paid Connect subscription", str(error))
        self.assertIn("same API key", str(error))
        self.assertIn("IP whitelisting does not grant market-data access", str(error))
        self.assertNotIn("never-print-query", str(error))
        self.assertFalse(self.http.auth_expired)
        self.assertFalse(error.session_expired)
        self.assertTrue(stream.closed)
        self.http.opener.open.assert_called_once()

    def test_historical_permission_has_the_same_actionable_data_guidance(self):
        self.response()
        with self.assertRaises(BrokerRejected) as caught:
            self.http.request("GET", "/instruments/historical/123/day")
        self.assertIn("GET /instruments/historical/123/day", str(caught.exception))
        self.assertIn("live and historical data", str(caught.exception))
        self.assertFalse(caught.exception.session_expired)

    def test_token_exception_on_quote_is_not_mislabeled_as_a_subscription_problem(self):
        self.response(error_type="TokenException", message="Invalid api_key or access_token.")
        with self.assertRaises(BrokerRejected) as caught:
            self.http.request("GET", "/quote")
        self.assertTrue(self.http.auth_expired)
        self.assertTrue(caught.exception.session_expired)
        self.assertIn("sign in with Zerodha again", str(caught.exception))
        self.assertNotIn("paid Connect", str(caught.exception))

    def test_profile_permission_denial_does_not_expire_the_session(self):
        self.response()
        with self.assertRaises(BrokerRejected) as caught:
            self.http.request("GET", "/user/profile")
        self.assertIn("account/API permissions", str(caught.exception))
        self.assertFalse(caught.exception.session_expired)
        self.assertFalse(self.http.auth_expired)

    def test_http_401_without_json_still_requires_login(self):
        self.response(code=401, body=b"opaque response")
        with self.assertRaises(BrokerRejected) as caught:
            self.http.request("GET", "/user/profile")
        self.assertTrue(caught.exception.session_expired)
        self.assertTrue(self.http.auth_expired)
        self.assertIn("Broker error detail unavailable", str(caught.exception))
        self.assertNotIn("opaque response", str(caught.exception))

    def test_explicit_ip_error_points_to_the_whitelist(self):
        self.response(message="Request IP is not whitelisted.")
        with self.assertRaises(BrokerRejected) as caught:
            self.http.request("GET", "/quote")
        self.assertIn("outbound static IP", str(caught.exception))
        self.assertNotIn("paid Connect", str(caught.exception))
        self.assertFalse(self.http.auth_expired)

    def test_error_text_redacts_credentials_urls_and_secret_fields(self):
        self.response(message=(
            "Denied test-only-app-key / test-only-access-token. "
            "https://example.invalid/?access_token=other-secret "
            "api_secret=do-not-echo; Authorization: Bearer do-not-echo-either"
        ))
        with self.assertRaises(BrokerRejected) as caught:
            self.http.request("GET", "/quote")
        text = str(caught.exception)
        for secret in ("test-only-app-key", "test-only-access-token", "other-secret",
                       "do-not-echo", "never-print-query", "must-not-use-HTTP-reason"):
            self.assertNotIn(secret, text)
        self.assertIn("[redacted]", text)

    def test_non_json_html_is_not_exposed(self):
        self.response(body=b"<html>test-only-access-token<private-response/></html>")
        with self.assertRaises(BrokerRejected) as caught:
            self.http.request("GET", "/quote")
        self.assertNotIn("private-response", str(caught.exception))
        self.assertNotIn("test-only-access-token", str(caught.exception))
        self.assertIn("Broker error detail unavailable", str(caught.exception))

    def test_oversized_json_is_not_partially_logged(self):
        self.response(body=json.dumps({"message": "private-fragment " * 2000}).encode())
        with self.assertRaises(BrokerRejected) as caught:
            self.http.request("GET", "/quote")
        self.assertNotIn("private-fragment", str(caught.exception))
        self.assertLess(len(str(caught.exception)), 600)

    def test_invalid_error_field_shapes_are_not_trusted(self):
        self.response(error_type="PermissionException\ninjected-log", message={"access_token": "hidden"})
        with self.assertRaises(BrokerRejected) as caught:
            self.http.request("GET", "/quote")
        self.assertIsNone(caught.exception.error_type)
        self.assertNotIn("injected-log", str(caught.exception))
        self.assertNotIn("hidden", str(caught.exception))

    def test_long_messages_and_control_characters_are_bounded(self):
        self.response(message="start\x00\x1b[31m\n" + "repeated " * 1000)
        with self.assertRaises(BrokerRejected) as caught:
            self.http.request("GET", "/quote")
        text = str(caught.exception)
        self.assertNotIn("\x00", text)
        self.assertNotIn("\x1b", text)
        self.assertNotIn("\n", text)
        self.assertLess(len(text), 700)

    def test_write_timeout_status_remains_unknown_without_any_retry(self):
        self.http.allow_orders = True
        self.response(code=504, error_type="NetworkException", message="Gateway timeout.")
        with self.assertRaises(SubmissionUnknown) as caught:
            self.http.request("POST", "/orders/regular", data={"tag": "test1234"})
        self.assertFalse(caught.exception.session_expired)
        self.assertIn("reconcile", str(caught.exception))
        self.http.opener.open.assert_called_once()

    def test_rate_limit_is_not_retried_or_treated_as_expired_auth(self):
        self.response(code=429, error_type="GeneralException", message="Too many requests.")
        with self.assertRaises(BrokerReadUnavailable) as caught:
            self.http.request("GET", "/quote")
        self.assertEqual(caught.exception.status_code, 429)
        self.assertFalse(self.http.auth_expired)
        self.http.opener.open.assert_called_once()

    def test_unreadable_error_body_keeps_explicit_http_context(self):
        response = urllib.error.HTTPError("https://api.kite.trade/quote", 403, "Forbidden", {}, None)
        response.read = Mock(side_effect=OSError("private transport details"))
        self.http.opener.open.side_effect = response
        with self.assertRaises(BrokerRejected) as caught:
            self.http.request("GET", "/quote")
        self.assertIn("GET /quote rejected (HTTP 403)", str(caught.exception))
        self.assertNotIn("private transport details", str(caught.exception))

    def test_preparation_distinguishes_auth_failure_from_account_permission(self):
        vault = Mock()
        vault.load.return_value = {
            "auto_start": True, "consent_version": "auto-live-v1",
            "keys": {"ai_api_key": "test-only-ai", "broker_api_key": "test-only-key",
                     "broker_access_token": "test-only-token"},
        }
        with tempfile.TemporaryDirectory() as temporary:
            for error_type in ("TokenException", "PermissionException"):
                failure = BrokerRejected("Sanitized broker response.", 403, error_type=error_type)
                http = Mock()
                http.request.side_effect = failure
                with self.subTest(error_type=error_type), \
                     patch("india_trader.autonomy.now_ist", return_value=datetime(2026, 9, 24, 10, 0, tzinfo=IST)), \
                     patch("india_trader.autonomy.software_ready", return_value=True), \
                     patch("india_trader.autonomy.KiteHTTP", return_value=http):
                    expected = PreparationBlocked if error_type == "TokenException" else BrokerRejected
                    with self.assertRaises(expected) as caught:
                        prepare_session(Path(temporary), Path(temporary), vault)
                    if error_type == "TokenException":
                        self.assertEqual(caught.exception.state, "BROKER_LOGIN_REQUIRED")
                    http.request.assert_called_once_with("GET", "/user/profile")

    def test_controller_routes_token_failures_from_any_preparation_stage_to_login(self):
        vault = Mock()
        vault.load.return_value = {
            "auto_start": True, "keys": {"ai_api_key": "test-only-ai", "broker_api_key": "test-only-key"},
        }
        with tempfile.TemporaryDirectory() as temporary:
            for error_type, expected in (("TokenException", "BROKER_LOGIN_REQUIRED"),
                                         ("PermissionException", "BLOCKED")):
                error = BrokerRejected("Sanitized quote failure.", 403, error_type=error_type,
                                       endpoint="/quote", method="GET")
                controller = Controller(Path(temporary), Path(temporary), vault,
                                        prepare=Mock(side_effect=error))
                controller._software_check = Mock()
                controller.closed = Mock()
                controller.closed.is_set.side_effect = [False, True]
                controller.wake.set()
                controller._loop()
                self.assertEqual(controller.phase, expected)
                self.assertEqual(controller.reason, "Sanitized quote failure.")
                controller.prepare.assert_called_once()


if __name__ == "__main__":
    unittest.main()
