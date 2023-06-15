#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import gzip
import sys
import glob
import logging
import collections
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor
from itertools import islice
from optparse import OptionParser
# brew install protobuf
# protoc  --python_out=. ./appsinstalled.proto
# pip install protobuf
import appsinstalled_pb2
# pip install python-memcached
import memcache
import threading


NORMAL_ERR_RATE = 0.01
AppsInstalled = collections.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])


def dot_rename(path):
    head, fn = os.path.split(path)
    # atomic in most cases
    os.rename(path, os.path.join(head, "." + fn))


def insert_appsinstalled(memc_addr, appsinstalled, dry_run=False):
    ua = appsinstalled_pb2.UserApps()
    ua.lat = appsinstalled.lat
    ua.lon = appsinstalled.lon
    key = "%s:%s" % (appsinstalled.dev_type, appsinstalled.dev_id)
    ua.apps.extend(appsinstalled.apps)
    packed = ua.SerializeToString()
    # @TODO persistent connection
    # @TODO retry and timeouts!
    try:
        if dry_run:
            logging.debug("%s - %s -> %s" % (memc_addr, key, str(ua).replace("\n", " ")))
        else:
            memc = memcache.Client([memc_addr])
            memc.set(key, packed)
    except Exception as e:
        logging.exception("Cannot write to memc %s: %s" % (memc_addr, e))
        return False
    return True


def parse_appsinstalled(line):
    line_parts = line.strip().split("\t")
    if len(line_parts) < 5:
        return
    dev_type, dev_id, lat, lon, raw_apps = line_parts
    if not dev_type or not dev_id:
        return
    try:
        apps = [int(a.strip()) for a in raw_apps.split(",")]
    except ValueError:
        apps = [int(a.strip()) for a in raw_apps.split(",") if a.isidigit()]
        logging.info("Not all user apps are digits: `%s`" % line)
    try:
        lat, lon = float(lat), float(lon)
    except ValueError:
        logging.info("Invalid geo coords: `%s`" % line)
    return AppsInstalled(dev_type, dev_id, lat, lon, apps)


def main(options):
    device_memc = {
        "idfa": options.idfa,
        "gaid": options.gaid,
        "adid": options.adid,
        "dvid": options.dvid,
    }
    with ProcessPoolExecutor(max_workers=2) as pexecutor:
        future_to_line = [pexecutor.submit(process_line, (n, line)) for n, line in enumerate(batch)]
    

def process_file(fn)
        processed = errors = 0
        logging.info('Processing %s' % fn)
        with gzip.open(fn, 'rt') as f:
            nlines = sum(1 for i in f)
            print(f"Got nlines: {nlines}")


        with gzip.open(fn, 'rt') as fd:
            batch_n = 0
            while True:
                batch_n += 1
                logging.info(f"Working on batch {batch_n}")
                batch = list(islice(fd, 1000))
                if not batch:
                    break

                    for future in as_completed(future_to_line):
                        try:
                            data = future.result()
                        except Exception as exc:
                            print(f'err {exc}')
                        else:
                            errors += not data[0]
                            processed += data[0]
                            if not data[1] % 100:
                                print(f"Line {data[1]} is done.")

                # with ThreadPoolExecutor(max_workers=4) as executor:
                #     # for n, line in enumerate(fd):
                #     #     submits = executor.submit(process_line, (n, line))
                #     future_to_line = [executor.submit(process_line, (n, line)) for n, line in enumerate(batch)]
                #     for future in as_completed(future_to_line):
                #         try:
                #             data = future.result()
                #         except Exception as exc:
                #             print(f'err {exc}')
                #         else:
                #             errors += not data[0]
                #             processed += data[0]
                #             if not data[1] % 100:
                #                 print(f"Line {data[1]} is done.")

        # if not processed:
        #     # dot_rename(fn)
        #     continue

        err_rate = float(errors) / processed
        if err_rate < NORMAL_ERR_RATE:
            logging.info(f"Processed: {processed}. Acceptable error rate {err_rate}. Successfull load")
        else:
            logging.error(f"Processed: {processed}. High error rate ({err_rate} > {NORMAL_ERR_RATE}). Failed load")
        # dot_rename(fn)



def process_line(nline):
    n, line = nline
    # if not n % 100:
    #     print(f"Line {n} is processed...")
    errors = 0
    processed = 0
    # for line in batch:
    line = line.strip()
    if not line:
        return False, n
    appsinstalled = parse_appsinstalled(line)
    if not appsinstalled:
        return False, n
    device_memc = {
        "idfa": '127.0.0.1:33013',
        "gaid": '127.0.0.1:33014',
        "adid": '127.0.0.1:33015',
        "dvid": '127.0.0.1:33016',
    }
    memc_addr = device_memc.get(appsinstalled.dev_type)
    if not memc_addr:
        errors += 1
        logging.error("Unknow device type: %s" % appsinstalled.dev_type)
        return False, n
    ok = insert_appsinstalled(memc_addr, appsinstalled, False)
    return ok, n


def prototest():
    sample = "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"
    for line in sample.splitlines():
        dev_type, dev_id, lat, lon, raw_apps = line.strip().split("\t")
        apps = [int(a) for a in raw_apps.split(",") if a.isdigit()]
        lat, lon = float(lat), float(lon)
        ua = appsinstalled_pb2.UserApps()
        ua.lat = lat
        ua.lon = lon
        ua.apps.extend(apps)
        packed = ua.SerializeToString()
        unpacked = appsinstalled_pb2.UserApps()
        unpacked.ParseFromString(packed)
        assert ua == unpacked


if __name__ == '__main__':
    op = OptionParser()
    op.add_option("-t", "--test", action="store_true", default=False)
    op.add_option("-l", "--log", action="store", default=False)
    op.add_option("--dry", action="store_true", default=False)
    op.add_option("--pattern", action="store", default="data/appsinstalled/*.tsv.gz")
    op.add_option("--idfa", action="store", default="127.0.0.1:33013")
    op.add_option("--gaid", action="store", default="127.0.0.1:33014")
    op.add_option("--adid", action="store", default="127.0.0.1:33015")
    op.add_option("--dvid", action="store", default="127.0.0.1:33016")
    (opts, args) = op.parse_args()
    logging.basicConfig(filename=opts.log, level=logging.INFO if not opts.dry else logging.DEBUG,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')

    if opts.test:
        prototest()
        sys.exit(0)

    logging.info("Memc loader started with options: %s" % opts)

    try:
        main(opts)
    except Exception as e:
        logging.exception("Unexpected error: %s" % e)
        sys.exit(1)
