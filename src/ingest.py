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
import os
import socket
import time

from lsst.daf.butler import Butler
from lsst.obs.base import DefineVisitsTask, RawIngestTask
from lsst.resources import ResourcePath

from exposure_info import ExposureInfo
from utils import setup_logging, setup_redis

MAX_FAILURES: int = 3
"""Retry ingests until this many failures (`int`)."""

logger = setup_logging(__name__)
r = setup_redis()
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
        logger.info("Ingested %s", dataset)
        e = ExposureInfo(dataset.path.geturl())
        logger.debug("Exposure %s", e)
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
    logger.error("Failed to ingest %s: %s", dataset, exc)
    e = ExposureInfo(dataset.files[0].filename.geturl())
    logger.debug("Exposure %s", e)
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

    logger.error("Failed to translate metadata for %s: %s", dataset, exc)
    e = ExposureInfo(dataset.geturl())
    logger.debug("Exposure %s", e)
    with r.pipeline() as pipe:
        pipe.hincrby(f"FAIL:{e.bucket}:{e.instrument}", f"{e.obs_day}", 1)
        pipe.hset(f"FILE:{e.path}", "md_fail_exc", str(exc))
        pipe.lrem(worker_queue, 0, e.path)
        pipe.execute()


def main():
    """Ingest FITS files from a Redis queue."""
    logger.info("Initializing Butler from %s", butler_repo)
    butler = Butler(butler_repo, writeable=True)
    ingest_config = RawIngestTask.ConfigClass()
    ingest_config.transfer = "direct"
    ingester = RawIngestTask(
        config=ingest_config,
        butler=butler,
        on_success=on_success,
        on_ingest_failure=on_ingest_failure,
        on_metadata_failure=on_metadata_failure,
    )

    define_visits_config = DefineVisitsTask.ConfigClass()
    define_visits_config.groupExposures = "one-to-one"
    visit_definer = DefineVisitsTask(config=define_visits_config, butler=butler)

    logger.info("Waiting on %s", worker_queue)
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
                logger.info("Ingesting %s", resources)
                refs = None
                try:
                    refs = ingester.run(resources)
                except Exception:
                    logger.exception("Error while ingesting %s", resources)
                if refs:
                    try:
                        ids = [ref.dataId for ref in refs]
                        visit_definer.run(ids)
                        logger.info("Defined visits for %s", ids)
                    except Exception:
                        logger.exception("Error while defining visits for %s", refs)
        # Atomically grab the next entry from the bucket queue, blocking until
        # one exists.
        r.blmove(redis_queue, worker_queue, 0, "RIGHT", "LEFT")


if __name__ == "__main__":
    main()
