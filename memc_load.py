#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import gzip
import sys
import glob
import logging
import collections
import time
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor, wait, FIRST_COMPLETED
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
    # head, fn = os.path.split(path)
    # atomic in most cases
    # os.rename(path, os.path.join(head, "." + fn))
    pass


def insert_appsinstalled(memc_addr, appsinstalled, dry_run=False):
    ua = appsinstalled_pb2.UserApps()
    ua.lat = appsinstalled.lat
    ua.lon = appsinstalled.lon
    key = f"{appsinstalled.dev_type}:{appsinstalled.dev_id}"
    ua.apps.extend(appsinstalled.apps)
    packed = ua.SerializeToString()
    # @TODO persistent connection
    # @TODO retry and timeouts!
    try:
        if dry_run:
            ua_cr_replaced = str(ua).replace('\n', ' ')
            logging.debug(f"{memc_addr} - {key} -> {ua_cr_replaced}")
        else:
            result, i = False, 0
            while i < 50:
                memc = memcache.Client((memc_addr,), debug=0)  # will not return error if no connection
                result = memc.set(key, packed)
                if result:
                    return result
                time.sleep(0.01)
                i += 1
            logging.error('Insert unsuccessful')
            return False
    except Exception as e:
        logging.exception(f"Cannot write to memc {memc_addr}: {e}")
        return False



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
        logging.info(f"Not all user apps are digits: `{line}`")
    try:
        lat, lon = float(lat), float(lon)
    except ValueError:
        logging.info(f"Invalid geo coords: `{line}`")
    return AppsInstalled(dev_type, dev_id, lat, lon, apps)



def main(options):
    device_memc = {
        "idfa": options.idfa,
        "gaid": options.gaid,
        "adid": options.adid,
        "dvid": options.dvid,
    }
    files = sorted(list(glob.iglob(options.pattern)))  # to prefix processed files chronologically
    with ProcessPoolExecutor(max_workers=3) as pexecutor:
        iterator = {pexecutor.submit(process_file, fn, device_memc): fn for fn in files}
        files.reverse()  # to pop from the end
        while files:
            dones = wait(iterator, return_when=FIRST_COMPLETED).done
            for done in dones:
                if files[-1] == iterator[done]:
                    dot_rename(files[-1])
                    iterator.pop(done)
                    logging.info(f"File {files.pop()} has been renamed")


def process_file(fn, device_memc):
    processed = errors = 0
    logging.info(f'Processing file {fn}')
    with gzip.open(fn, 'rt') as f:
        nlines = sum(1 for i in f)
        f.seek(0)
        logging.info(f"File {fn}. Total lines to process: {nlines}")
        batch_n = 1
        while True:
            batch = list(islice(f, 20000))
            if not batch:
                break
            logging.info(f"File {fn}: batch {batch_n} / {int(nlines / 20000) +1}")
            with ThreadPoolExecutor(max_workers=100) as executor:
                future_to_line = [executor.submit(process_line, n, line, device_memc, fn) for n, line in enumerate(batch)]
                logging.info(f"File {fn}. Created thread pool")
                for future in as_completed(future_to_line):
                    # logging.info(f"File {fn}. Analysing future: {future}")
                    # try:
                    data = future.result()
                    # except Exception as exc:
                    # logging.info(f'File {f.name}: error, {exc}')
                    # else:
                    errors += not data[0]
                    processed += data[0]
                        # if not data[1] % 10000:
                        #     logging.info(f"Line {batch_n * 20000 + data[1] } of {fd.name} is done.")
            batch_n += 1
    logging.info(f"File {fn}. {processed} {errors}")
    err_rate = float(errors) / (errors + processed)
    if err_rate < NORMAL_ERR_RATE:
        logging.info(f"File {fn}. Processed: {processed}. Acceptable error rate {err_rate}. Successfull load")
    else:
        logging.error(f"File {fn}. Processed: {processed}. \
        High error rate ({err_rate} > {NORMAL_ERR_RATE}). Failed load")



def process_line(n, line, device_memc, fn):
    line = line.strip()
    if not line:
        logging.error(f"File {fn}. No line")
        return False, n
    appsinstalled = parse_appsinstalled(line)
    if not appsinstalled:
        logging.error(f"File {fn}. No appsinstalled")
        return False, n
    memc_addr = device_memc.get(appsinstalled.dev_type)
    if not memc_addr:
        logging.error(f"File {fn}. Unknown device type: {appsinstalled.dev_type}")
        return False, n
    ok = insert_appsinstalled(memc_addr, appsinstalled, False)
    if not ok:
        logging.error(f"File {fn}. Memc insertion error for {appsinstalled.dev_type}, line {n}")
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


if __name__ == '__main__':

    if opts.test:
        prototest()
        sys.exit(0)

    logging.info("Memc loader started with options: %s" % opts)

    try:
        main(opts)
    except Exception as e:
        logging.exception("Unexpected error: %s" % e)
        sys.exit(1)
