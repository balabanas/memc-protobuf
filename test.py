import collections
import gzip
import os
import unittest
from pathlib import Path

import memcache

import memc_load

client_addr = '127.0.0.1:33013'  # test server address
AppsInstalled = collections.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])


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
        appsinstalled = AppsInstalled('somedev', 'someid', 55.1, 55.1, [1, 2, 3])
        key, packed = memc_load.protobuf_serilalize(appsinstalled)
        result = memc_load.insert_appsinstalled_multi(self.memc, {key: packed}, False)
        self.assertEqual((0, 1), result)
        val = self.memc.get('somedev:someid')
        self.assertEqual(b'\x08\x01\x08\x02\x08\x03\x11\xcd\xcc\xcc\xcc\xcc\x8cK@\x19\xcd\xcc\xcc\xcc\xcc\x8cK@', val)

    def test_insert_fail_nonexistent_instance(self):
        appsinstalled = AppsInstalled('somedev', 'someid', 55.1, 55.1, [1, 2, 3])
        key, packed = memc_load.protobuf_serilalize(appsinstalled)
        nonex_memc = memcache.Client(['127.0.0.1:35004', ])
        result = memc_load.insert_appsinstalled_multi(nonex_memc, {key: packed}, False)
        self.assertEqual((1, 1), result)

    def test_insert_dry_run(self):
        appsinstalled = AppsInstalled('somedev', 'someid', 55.1, 55.1, [1, 2, 3])
        key, packed = memc_load.protobuf_serilalize(appsinstalled)
        with self.assertLogs(level='DEBUG') as cm:
            memc_load.insert_appsinstalled_multi(self.memc, {key: packed}, True)
        self.assertIn(
            'DEBUG:root:inet:127.0.0.1:33013 - somedev:someid -> apps: 1 apps: 2 apps: 3 lat: 55.1 lon: 55.1 ',
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
        self.assertEqual(AppsInstalled(dev_type='idfa', dev_id='devid', lat=55.55, lon=42.42, apps=[1423, 3, 7, 23]),
                         result)


class FilesTest(unittest.TestCase):
    def setUp(self) -> None:
        self.memc = memcache.Client((client_addr,))
        # write a gunzipped file with some contents
        content = "somedev\tsomeid\t55.55\t42.42\t1423,43,567,3,7,23\nsomedev1\tsomeid1\t55.55\t42.42\t7423,424"
        os.makedirs('test', exist_ok=True)
        self.file_path = Path('test/test.tsv')
        with open(self.file_path, 'w') as file:
            file.write(content)
        self.compressed_file_path = Path('test/test.tsv.gz')
        with open(self.file_path, 'rb') as file_in:
            with gzip.open(self.compressed_file_path, 'wb') as file_out:
                file_out.writelines(file_in)
        os.remove(self.file_path)

    def tearDown(self) -> None:
        self.memc.delete('somedev:someid')
        self.memc.delete('somedev1:someid1')
        self.memc.disconnect_all()

        os.remove(self.compressed_file_path)
        os.rmdir('test')

    def test_dot_rename(self):
        self.assertTrue(self.compressed_file_path.name in os.listdir('test'))
        memc_load.dot_rename(self.compressed_file_path)
        self.assertTrue('.' + self.compressed_file_path.name in os.listdir('test'))
        self.assertFalse(self.compressed_file_path.name in os.listdir('test'))
        os.rename(str(self.compressed_file_path.parent) + '/.' + str(self.compressed_file_path.name),
                  self.compressed_file_path)

    def test_main(self):
        from memc_load import opts, device_memc
        opts.pattern = 'test/*.tsv.gz'
        device_memc['somedev'] = client_addr
        memc_load.main()
        os.rename(str(self.compressed_file_path.parent) + '/.' + str(self.compressed_file_path.name),
                  self.compressed_file_path)
        self.assertTrue(self.memc.get('somedev:someid'))


class SplitByDevTest(unittest.TestCase):
    def setUp(self) -> None:
        self.content = [
            'somedev\tsomeid\t55.55\t42.42\t1423,43,567,3,7,23',
            'somedev1\tsomeid1\t55.55\t42.42\t7423,424',
            'errdev\t...',
            '  ',
        ]

    def test_split(self):
        batch_by_dev, errors = memc_load.split_by_dev(self.content)
        self.assertEqual(1, len(batch_by_dev['somedev']))
        self.assertEqual(1, len(batch_by_dev['somedev1']))
        self.assertEqual(2, errors)


if __name__ == "__main__":
    unittest.main()
