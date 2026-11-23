from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from benchmark_runtime import seed_catalog, original_snapshot, source_hashes, instance_wal_compression
from system import Cluster


class BenchmarkRuntimeTest(unittest.TestCase):
    def test_notification_wal_compression_does_not_change_other_instances(self):
        self.assertEqual('lz4', instance_wal_compression('notification', 'lz4'))
        for service in ['catalog', 'checkout', 'analytics', 'inventory', 'shared']:
            self.assertEqual('off', instance_wal_compression(service, 'lz4'))
        for invalid in [None, 'invalid', 'lz4 -c fsync=off', True]:
            with self.assertRaises(ValueError):
                instance_wal_compression('notification', invalid)

    def test_seeds_matching_catalog_and_inventory(self):
        databases = {'catalog': Mock(), 'inventory': Mock()}
        result = seed_catalog(databases, 2000, 1000000)
        self.assertEqual(2000, result['sku_count'])
        for database in databases.values():
            database.sql.assert_called_once()
            self.assertIn('generate_series(1,2000)', database.sql.call_args.args[0])

    def test_invalid_seed_never_touches_database(self):
        databases = {'catalog': Mock(), 'inventory': Mock()}
        with self.assertRaises(ValueError):
            seed_catalog(databases, -1, 1000000)
        with self.assertRaises(ValueError):
            seed_catalog(databases, 2000, -1)
        for database in databases.values():
            database.sql.assert_not_called()

    def test_baseline_copy_excludes_data_and_keeps_sources_unchanged(self):
        with tempfile.TemporaryDirectory() as folder:
            source = Path(folder)/'source'
            source.mkdir()
            (source/'Main.java').write_text('class Main {}')
            (source/'target').mkdir()
            (source/'target/Secret.java').write_text('runtime')
            (source/'.env').write_text('password')
            destination = original_snapshot(source, Path(folder)/'copy')
            self.assertEqual(source_hashes(source), source_hashes(destination))
            self.assertFalse((destination/'target').exists())
            self.assertFalse((destination/'.env').exists())

    def test_cluster_supports_four_service_baseline_and_five_service_variant(self):
        for names in [('catalog', 'checkout', 'analytics', 'inventory'),
                      ('catalog', 'checkout', 'analytics', 'inventory', 'notification')]:
            cluster = Cluster(dict.fromkeys(names), '/tmp/test', source_root='/tmp/source')
            self.assertEqual(len(names)+1, len(cluster.ports))
            self.assertEqual(Path('/tmp/source/gateway/target/gateway-0.0.1-SNAPSHOT.jar'), cluster.jar('gateway'))


if __name__ == '__main__':
    unittest.main()
