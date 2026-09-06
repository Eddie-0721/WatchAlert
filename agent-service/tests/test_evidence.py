import unittest
from app.main import query_evidence


class EvidenceTests(unittest.TestCase):
    def test_prometheus_evidence_retains_query_not_credentials(self):
        evidence = query_evidence("prometheus.query_range", {"datasourceId": "shared", "password": "secret"}, {
            "datasource": {"id": "shared", "name": "Prometheus", "token": "secret"},
            "promql": 'up{env="test"}', "start": 1, "end": 100, "step": 10,
            "resultCount": 1000, "truncated": True,
        })
        self.assertTrue(evidence.truncated)
        self.assertNotIn("secret", evidence.model_dump_json())
        self.assertEqual(evidence.query["promql"], 'up{env="test"}')
        self.assertGreater(evidence.queriedAt, 0)

    def test_page_count_is_not_reported_as_complete(self):
        evidence = query_evidence("alerts.search", {"index": 1, "size": 20}, {"list": [{"fingerprint": "a", "labels": {"password": "secret"}}], "total": 125})
        self.assertIn("125", evidence.summary)
        self.assertEqual(evidence.source["records"], [{"fingerprint": "a"}])


if __name__ == "__main__":
    unittest.main()
