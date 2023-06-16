import collections
import logging
import sys
import unittest
# import appsinstalled_pb2
import memc_load
import memcache

client_addr = '127.0.0.1:33013'  # test server address
AppsInstalled = collections.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])

# logger = logging.getLogger()
# logger.level = logging.DEBUG

class InsertAppsinstalledTest(unittest.TestCase):

    def setUp(self) -> None:
        self.memc = memcache.Client((client_addr,))

    def tearDown(self) -> None:
        self.memc.delete('test_key')
        self.memc.delete('somedev:someid')
        self.memc.disconnect_all()

    def test_set_get(self):
        key = 'test_key'
        val = b'test_value'
        result = self.memc.set(key, val)
        self.assertTrue(result)
        val_read = self.memc.get(key)
        self.assertEqual(val, val_read)

    def test_insert(self):
        appinstalled = AppsInstalled('somedev', 'someid', 55.1, 55.1, [1, 2, 3])
        result = memc_load.insert_appsinstalled(client_addr, appinstalled, False)
        self.assertTrue(result)
        val = self.memc.get('somedev:someid')
        self.assertEqual(b'\x08\x01\x08\x02\x08\x03\x11\xcd\xcc\xcc\xcc\xcc\x8cK@\x19\xcd\xcc\xcc\xcc\xcc\x8cK@', val)

    def test_insert_fail_nonexistent_instance(self):
        appinstalled = AppsInstalled('somedev', 'someid', 55.1, 55.1, [1, 2, 3])
        result = memc_load.insert_appsinstalled('127.0.0.1:35004', appinstalled, False)
        self.assertFalse(result)

    def test_insert_fail_bad_address(self):
        appinstalled = AppsInstalled('somedev', 'someid', 55.1, 55.1, [1, 2, 3])
        result = memc_load.insert_appsinstalled(0, appinstalled, False)
        self.assertFalse(result)

    def test_insert_dry_run(self):
        appinstalled = AppsInstalled('somedev', 'someid', 55.1, 55.1, [1, 2, 3])
        # stream_handler = logging.StreamHandler(sys.stdout)
        # logger.addHandler(stream_handler)
        with self.assertLogs(level='DEBUG') as cm:
            memc_load.insert_appsinstalled(client_addr, appinstalled, True)
        self.assertIn('DEBUG:root:127.0.0.1:33013 - somedev:someid -> apps: 1 apps: 2 apps: 3 lat: 55.1 lon: 55.1 ',
                      cm.output)


class ParseAppinstalledTest(unittest.TestCase):
    def test_line_completeness(self):
        sample = "idfa\t55.55\t42.42\t1423,3,7,23\n"
        result = memc_load.parse_appsinstalled(sample)
        self.assertEqual(None, result)  # less than 5 parts

    def test_empty_ids(self):
        sample = "d\t\t55.55\t42.42\t1423,3,7,23\n"
        result = memc_load.parse_appsinstalled(sample)
        self.assertEqual(None, result)  # empty dev_id

    def test_not_all_digits(self):
        sample = "idfa\tdevid\t55.55\t42.42\t1423,3,7,a,23\n"
        with self.assertLogs(level='DEBUG') as cm:
            memc_load.parse_appsinstalled(sample)
        self.assertIn(f"INFO:root:Not all user apps are digits: `{sample}`", cm.output)

    def test_invalid_geo(self):
        sample = "idfa\tdevid\t55.55\tabc\t1423,3,7,23\n"
        with self.assertLogs(level='DEBUG') as cm:
            memc_load.parse_appsinstalled(sample)
        self.assertIn(f"INFO:root:Invalid geo coords: `{sample}`", cm.output)

    def test_ok(self):
        sample = "idfa\tdevid\t55.55\t42.42\t1423,3,7,23\n"
        result = memc_load.parse_appsinstalled(sample)
        self.assertEqual(AppsInstalled(dev_type='idfa', dev_id='devid', lat=55.55, lon=42.42, apps=[1423, 3, 7, 23]), result)


if __name__ == "__main__":
    unittest.main()
