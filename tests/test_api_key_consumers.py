import io
import unittest
from unittest.mock import patch

from scripts import api_smoke_tests, eval_harness, kg_exact_drift_audit


class ConsumerKeyTests(unittest.TestCase):
    def test_local_http_consumers_send_environment_key(self):
        calls = [
            lambda: api_smoke_tests.get_json("http://synthetic.invalid", "/status"),
            lambda: api_smoke_tests.post_json("http://synthetic.invalid", "/query", {}),
            lambda: eval_harness.post_json("http://synthetic.invalid", "/query", {}, 1),
            lambda: kg_exact_drift_audit._request_json("http://synthetic.invalid/freshness"),
            lambda: kg_exact_drift_audit._request_json("http://synthetic.invalid/freshness/repair", "POST"),
        ]
        for key in ["", "synthetic-consumer-key"]:
            for call in calls:
                with self.subTest(key=key, call=call), patch.dict("os.environ", {"KG_API_KEY": key}), patch("urllib.request.urlopen") as opened:
                    opened.return_value = io.BytesIO(b'{}')
                    self.assertEqual(call(), {})
                    request = opened.call_args.args[0]
                    self.assertEqual(request.get_header("X-kg-api-key"), key)
                    self.assertNotIn("synthetic-consumer-key", request.full_url)
