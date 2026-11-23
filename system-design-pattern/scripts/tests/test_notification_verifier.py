import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from verify_notifications import verify


class NotificationVerifierTest(unittest.TestCase):
    def check(self, records):
        sales = [{'transaction_id': 'tx', 'completed_at': '2026-11-16T12:00:01Z', 'sku': 'A', 'quantity': 3}]
        subscriptions = [{'subscriber_id': 'alice', 'sku': 'A', 'subscribed_at': '2026-11-16T12:00:00Z'}]
        with tempfile.TemporaryDirectory() as directory:
            for user, entries in records.items():
                Path(directory, user+'.jsonl').write_text(''.join(json.dumps(entry)+'\n' for entry in entries))
            with patch('verify_notifications.rows', side_effect=[sales, subscriptions]):
                return verify({'checkout': None, 'notification': None}, directory)

    def delivery(self, **overrides):
        return dict(transactionId='tx', subscriberId='alice', sku='A', quantity=3,
                    completedAt='2026-11-16T12:00:01Z', receivedAt='2026-11-16T12:00:01.250Z') | overrides

    def test_matching_delivery(self):
        result = self.check({'alice': [self.delivery()]})
        self.assertTrue(result['passed'])
        self.assertEqual(250, result['latency_ms']['p50'])

    def test_missing_user_log_is_failure(self):
        self.assertEqual(1, self.check({})['missing'])

    def test_wrong_quantity_duplicate_and_extra_are_detected(self):
        result = self.check({'alice': [self.delivery(quantity=4), self.delivery(quantity=4), self.delivery(sku='B')]})
        self.assertFalse(result['passed'])
        self.assertEqual(1, result['wrong_quantity'])
        self.assertEqual(1, result['duplicate_log_entries'])
        self.assertEqual(1, result['unexpected'])

    def test_log_identity_cannot_impersonate_another_subscriber(self):
        with self.assertRaises(ValueError):
            self.check({'bob': [self.delivery()]})


if __name__ == '__main__':
    unittest.main()
