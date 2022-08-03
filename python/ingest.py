import logging
import os
import redis
import socket
import sys
import time

from lsst.daf.butler import Butler
from lsst.utils import doImportType
from lsst.resources import ResourcePath
from exposure_info import ExposureInfo

logging.basicConfig(
    level=logging.INFO,
    format="{levelname} {asctime} {name} ({filename}:{lineno}) - {message}",
    style="{",
    force=True,
)
logger = logging.Logger(__name__)
logger.setLevel(logging.DEBUG)

r = redis.Redis(host=os.environ["REDIS_HOST"])
r.auth(os.environ["REDIS_PASSWORD"])
redis_queue = os.environ["REDIS_QUEUE"]
butler_repo = os.environ["BUTLER_REPO"]

worker_name = socket.gethostname()
worker_queue = f"WORKER:{worker_name}"


def on_success(datasets):
    for dataset in datasets:
        logger.info(f"Ingested {dataset}")
        print(f"*** Ingested {dataset}")
        e = ExposureInfo(dataset.path)
        print(f"*** {e}")
        r.lrem(worker_queue, 0, e.path)
        # r.incr(f"INGEST:{e.bucket}:{e.instrument}:{e.obs_day}")
        # r.hset(f"FILE:{e.path}", "ingest_time", str(time.time()))


def on_ingest_failure(dataset, exc):
    logger.error(f"Failed to ingest {dataset}: {exc}")
    print(f"*** Failed to ingest {type(dataset)}({dataset}): {exc}", file=sys.stderr)
    e = ExposureInfo(dataset.geturl())
    print(f"*** {e}", file=sys.stderr)
    # r.incr(f"FAIL:{e.bucket}:{e.instrument}:{e.obs_day}")
    # r.hset(f"FILE:{e.path}", "last_ing_fail_exc", str(exc))
    # r.hincrby(f"FILE:{e.path}", "ing_fail_count", 1)
    # if int(r.hget(f"FILE:{e.path}", "ing_fail_count")) > 2:
    r.lrem(worker_queue, 0, e.path)


def on_metadata_failure(dataset, exc):
    logger.error(f"Failed to translate metadata for {dataset}: {exc}")
    e = ExposureInfo(dataset.geturl())
    print(f"*** {e}", file=sys.stderr)
    # r.incr(f"FAIL:{e.bucket}:{e.instrument}:{e.obs_day}")
    # r.hset(f"FILE:{e.path}", "last_md_fail_exc", str(exc))
    r.lrem(worker_queue, 0, e.path)


logger.info(f"Initializing Butler from {butler_repo}")
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

logger.info(f"Waiting on {worker_queue}")
while True:
    if r.llen(worker_queue) > 0:
        blobs = r.lrange(worker_queue, 0, -1)
        resources = []
        for b in blobs:
            # Wait for FITS files
            if b.endswith(b".fits"):
                resources.append(ResourcePath(f"s3://{b.decode()}"))
            else:
                r.lrem(worker_queue, 0, b)
        if resources:
            logger.info(f"Ingesting {resources}")
            print(f"*** Ingesting {resources}", file=sys.stderr)
            try:
                ingester.run(resources)
            except Exception as e:
                logger.error(f"Error while ingesting {resources}", exc_info=e)
    r.blmove(redis_queue, worker_queue, 0, "RIGHT", "LEFT")
