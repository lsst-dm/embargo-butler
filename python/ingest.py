import logging
import os
import redis
import socket
import time

from lsst.daf.butler import Butler
from lsst.utils import doImportType
from lsst.resources import ResourcePath
from exposure_info import ExposureInfo

logger = logging.Logger(__name__)

r = redis.Redis(host=os.environ["REDIS_HOST"])
r.auth(os.environ["REDIS_PASSWORD"])
redis_queue = os.environ["REDIS_QUEUE"]
butler_repo = os.environ["BUTLER_REPO"]

worker_name = socket.gethostname()
worker_queue = f"WORKER:{worker_name}"


def on_success(datasets):
    for dataset in datasets:
        logger.info(f"Ingested {dataset.geturl()}")
        e = ExposureInfo(dataset.path)
        r.lrem(worker_queue, 0, e.path)
        r.incr(f"INGEST:{e.bucket}:{e.instrument}:{e.obs_date}")
        r.hset(f"FILE:{e.path}", "ingest_time", str(time.time()))


def on_ingest_failure(dataset, exc):
    logger.error(f"Failed to ingest {dataset.geturl()}: {exc}")
    e = ExposureInfo(dataset.path)
    r.inc(f"FAIL:{e.bucket}:{e.instrument}:{e.obs_date}")
    r.hset(f"FILE:{e.path}", "last_ing_fail_exc", str(exc))
    r.hincrby(f"FILE:{e.path}", "ing_fail_count", 1)
    if int(r.hget(f"FILE:{e.path}", "ing_fail_count")) > 2:
        r.lrem(worker_queue, 0, e.path)


def on_metadata_failure(dataset, exc):
    logger.error(f"Failed to translate metadata for {dataset.geturl()}: {exc}")
    e = ExposureInfo(dataset.path)
    r.inc(f"FAIL:{e.bucket}:{e.instrument}:{e.obs_date}")
    r.hset(f"FILE:{e.path}", "last_md_fail_exc", str(exc))
    r.lrem(worker_queue, 0, e.path)


butler = Butler(butler_repo, writeable=True)
TaskClass = doImportType("lsst.obs.base.RawIngestTask")
ingestConfig = TaskClass.ConfigClass()
ingestConfig.transfer = "direct"
ingester = TaskClass(
    config=ingestConfig,
    butler=butler,
    on_success=on_success,
    on_ingest_failure=on_ingest_failure,
    on_metadata_failure=on_metadata_failure,
)

while True:
    if r.llen(worker_queue) > 0:
        blobs = r.lrange(worker_queue, 0, -1)
        resources = [ResourcePath(f"s3://{b.decode()}") for b in blobs
            if b.endswith(b".fits")]
        print(resources)
        ingester.run(resources)
    r.blmove(redis_queue, worker_queue, 0, "RIGHT", "LEFT")
