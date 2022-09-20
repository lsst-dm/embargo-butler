# This file is part of oga_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""
Service to ingest images into per-bucket Butler repos.
"""
import logging
import os
import socket
import sys
import time

import redis
from exposure_info import ExposureInfo
from lsst.daf.butler import Butler
from lsst.resources import ResourcePath
from lsst.utils import doImportType

MAX_FAILURES: int = 3
"""Retry ingests until this many failures (`int`)."""

logging.basicConfig(
    level=logging.INFO,
    format="{levelname} {asctime} {name} ({filename}:{lineno}) - {message}",
    style="{",
    stream=sys.stderr,
    force=True,
)
logger = logging.Logger(__name__)
logger.setLevel(logging.DEBUG)

r = redis.Redis(host=os.environ["REDIS_HOST"])
r.auth(os.environ["REDIS_PASSWORD"])
bucket = os.environ["BUCKET"]
redis_queue = f"QUEUE:{bucket}"
butler_repo = os.environ["BUTLER_REPO"]

worker_name = socket.gethostname()
worker_queue = f"WORKER:{bucket}:{worker_name}"


def on_success(datasets):
    """Callback for successful ingest.

    Record ingest time and statistics.

    Parameter
    ---------
    datasets: `list` [`lsst.daf.butler.FileDataset`]
        The successfully-ingested datasets.
    """
    for dataset in datasets:
        logger.info(f"Ingested {dataset}")
        print(f"*** Ingested {dataset}", file=sys.stderr)
        e = ExposureInfo(dataset.path.geturl())
        print(f"*** {e}", file=sys.stderr)
        with r.pipeline() as pipe:
            pipe.lrem(worker_queue, 0, e.path)
            pipe.hset(f"FILE:{e.path}", "ingest_time", str(time.time()))
            pipe.hincrby(f"INGEST:{e.bucket}:{e.instrument}", f"{e.obs_day}", 1)
            pipe.execute()


def on_ingest_failure(dataset, exc):
    """Callback for ingest failure.

    Record statistics; give up on the dataset if it fails 3 times.

    Parameters
    ----------
    dataset: `lsst.obs.base.ingest.RawFileData`
        Raw dataset that failed ingest.
    exc: `Exception`
        Exception raised by the ingest failure.
    """
    logger.error(f"Failed to ingest {dataset}: {exc}")
    print(f"*** Failed to ingest {type(dataset)}({dataset}): {exc}", file=sys.stderr)
    e = ExposureInfo(dataset.files[0].filename.geturl())
    print(f"*** {e}", file=sys.stderr)
    with r.pipeline() as pipe:
        pipe.hincrby(f"FAIL:{e.bucket}:{e.instrument}", f"{e.obs_day}", 1)
        pipe.hset(f"FILE:{e.path}", "ing_fail_exc", str(exc))
        pipe.hincrby(f"FILE:{e.path}", "ing_fail_count", 1)
        pipe.execute()
    if int(r.hget(f"FILE:{e.path}", "ing_fail_count")) >= MAX_FAILURES:
        r.lrem(worker_queue, 0, e.path)


def on_metadata_failure(dataset, exc):
    """Callback for metadata parsing failure.

    Record statistics.  Since this is always unrecoverable, don't retry.

    Parameters
    ----------
    dataset: `lsst.resources.ResourcePath`
        Path that failed metadata parsing.
    exc: `Exception`
        Exception raised by the ingest failure.
    """

    logger.error(f"Failed to translate metadata for {dataset}: {exc}")
    e = ExposureInfo(dataset.geturl())
    print(f"*** {e}", file=sys.stderr)
    with r.pipeline() as pipe:
        pipe.incr(f"FAIL:{e.bucket}:{e.instrument}", f"{e.obs_day}", 1)
        pipe.hset(f"FILE:{e.path}", "last_md_fail_exc", str(exc))
        pipe.lrem(worker_queue, 0, e.path)
        pipe.execute()


def main():
    """Ingest FITS files from a Redis queue."""
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
        # Process any entries on the worker queue.
        if r.llen(worker_queue) > 0:
            blobs = r.lrange(worker_queue, 0, -1)
            resources = []
            for b in blobs:
                if b.endswith(b".fits"):
                    # Should always be the case
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
        # Atomically grab the next entry from the bucket queue, blocking until
        # one exists.
        r.blmove(redis_queue, worker_queue, 0, "RIGHT", "LEFT")


if __name__ == "__main__":
    main()
