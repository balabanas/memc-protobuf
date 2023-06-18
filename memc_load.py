#!/usr/bin/env python
# -*- coding: utf-8 -*-
import glob
import gzip
import logging
import os
import sys
import time
from collections import defaultdict, namedtuple
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor, wait, FIRST_COMPLETED
from itertools import islice
from optparse import OptionParser

import memcache

import appsinstalled_pb2

N_RETRY_ON_ERROR: int = 2  # number of retries in case inserting is unsuccessful
BATCH_SIZE: int = 20000
N_PROCESSES: int = 3
N_THREADS: int = 4  # as we split each batch by 4 device-type block of records, and store each block as set_multi
NORMAL_ERR_RATE: float = 0.01
AppsInstalled = namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])


def dot_rename(path):
    head, fn = os.path.split(path)
    os.rename(path, os.path.join(head, "." + fn))


def protobuf_serilalize(appsinstalled):
    ua = appsinstalled_pb2.UserApps()
    ua.lat = appsinstalled.lat
    ua.lon = appsinstalled.lon
    key = f"{appsinstalled.dev_type}:{appsinstalled.dev_id}"
    ua.apps.extend(appsinstalled.apps)
    packed = ua.SerializeToString()
    return key, packed


def insert_appsinstalled_multi(memc: memcache.Client, data, dry_run=False):
    # @TODO timeouts!
    total_records = len(data)
    try:
        if dry_run:
            ua = appsinstalled_pb2.UserApps()
            for key, value in data.items():
                ua.ParseFromString(value)
                ua_cr_replaced = str(ua).replace('\n', ' ')
                logging.debug(f"{memc.servers[0]} - {key} -> {ua_cr_replaced}")
        else:
            result, i = False, 0
            while i < N_RETRY_ON_ERROR:
                result = memc.set_multi(data)
                if not result:  # if ok, result is an empty list, if not - failed keys
                    return 0, total_records
                time.sleep(0.02)
                i += 1
            return len(result), total_records  # n of errors, total
    except Exception as exc:
        logging.exception(f"Cannot write to memc {memc.servers[0]}: {exc}")
        return total_records, total_records  # assume all data is not stored


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
        apps = [int(a.strip()) for a in raw_apps.split(",") if a.isdigit()]
        logging.info(f"Not all user apps are digits: `{line}`")
    try:
        lat, lon = float(lat), float(lon)
    except ValueError:
        logging.info(f"Invalid geo coords: `{line}`")
    return AppsInstalled(dev_type, dev_id, lat, lon, apps)


def main():
    files = sorted(list(glob.iglob(opts.pattern)))  # to prefix processed files chronologically
    with ProcessPoolExecutor(max_workers=N_PROCESSES) as pexecutor:
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
    """
    :param fn:
    :param device_memc: despite it is global, we pass it to this function in order
    to be able to modify device_memc in tests, and be sure modified version is available in a forked process
    :return:
    """

    processed = errors = 0
    logging.info(f'Processing file {fn}')
    with gzip.open(fn, 'rt') as f:
        nlines = sum(1 for _ in f)
        f.seek(0)
        logging.info(f"File {fn}. Total lines to process: {nlines}")
        batch_n = 1
        while True:
            batch = list(islice(f, BATCH_SIZE))
            if not batch:
                break
            logging.info(f"File {fn}: batch {batch_n} / {int(nlines / BATCH_SIZE) + 1}")
            batch_by_dev, batch_errors = split_by_dev(batch)
            with ThreadPoolExecutor(max_workers=N_THREADS) as executor:
                future_to_line = [
                    executor.submit(insert_appsinstalled_multi, conns[device_memc[dev_type]], data, opts.dry) for
                    dev_type, data in batch_by_dev.items()]
                logging.info(f"File {fn}. Created thread pool")
                for future in as_completed(future_to_line):
                    data = future.result()
                    errors += data[0]
                    processed += data[1] - data[0]
                # add number of lines which were not parsed and as such were not included into batches_by_dev for insert
                errors += batch_errors
            batch_n += 1
    logging.info(f"File {fn}. {processed} {errors}")
    err_rate = float(errors) / (errors + processed)
    if err_rate < NORMAL_ERR_RATE:
        logging.info(f"File {fn}. Processed: {processed}. Acceptable error rate {err_rate}. Successfull load")
    else:
        logging.error(f"File {fn}. Processed: {processed}. \
        High error rate ({err_rate} > {NORMAL_ERR_RATE}). Failed load")


def split_by_dev(batch):
    splitted_batch = defaultdict(dict)
    batch_errors = 0
    for line in batch:
        line = line.strip()
        if not line:
            batch_errors += 1
            continue
        appsinstalled = parse_appsinstalled(line)
        if not appsinstalled:
            batch_errors += 1
            continue
        key, packed = protobuf_serilalize(appsinstalled)
        splitted_batch[appsinstalled.dev_type][key] = packed
    return splitted_batch, batch_errors


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
op.add_option("--pattern", action="store", default="/data/appsinstalled/*.tsv.gz")
op.add_option("--idfa", action="store", default="127.0.0.1:33013")
op.add_option("--gaid", action="store", default="127.0.0.1:33014")
op.add_option("--adid", action="store", default="127.0.0.1:33015")
op.add_option("--dvid", action="store", default="127.0.0.1:33016")
opts, args = op.parse_args()
logging.basicConfig(filename=opts.log, level=logging.DEBUG if not opts.dry else logging.DEBUG,
                    format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
device_memc = {
    "idfa": opts.idfa,
    "gaid": opts.gaid,
    "adid": opts.adid,
    "dvid": opts.dvid,
}
conns = {addr: memcache.Client((addr,), debug=0) for devtype, addr in device_memc.items()}

if __name__ == '__main__':
    if opts.test:
        prototest()
        sys.exit(0)
    logging.info("Memc loader started with options: %s" % opts)
    try:
        main()
        for server in conns.values():
            server.disconnect_all()
    except Exception as e:
        logging.exception("Unexpected error: %s" % e)
        sys.exit(1)
