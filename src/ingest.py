# This file is part of embargo_butler.
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
Service to ingest images or LFA objects into per-bucket Butler repos.
"""
import os
import socket
import time

import requests
from lsst.daf.butler import Butler
from lsst.obs.base import DefineVisitsTask, RawIngestTask
from lsst.resources import ResourcePath

from info import Info
from rucio_interface import RucioInterface
from utils import setup_logging, setup_redis

MAX_FAILURES: int = 3
"""Retry ingests until this many failures (`int`)."""

max_ingests = int(os.environ.get("MAX_INGESTS", "10"))
"""Accept up to this many files to ingest at once."""

logger = setup_logging(__name__)
r = setup_redis()
bucket = os.environ["BUCKET"]
if bucket.startswith("rubin:"):
    os.environ["LSST_DISABLE_BUCKET_VALIDATION"] = 1
redis_queue = f"QUEUE:{bucket}"
butler_repo = os.environ["BUTLER_REPO"]
webhook_uri = os.environ.get("WEBHOOK_URI", None)
is_lfa = "-lfa-" in bucket

worker_name = socket.gethostname()
worker_queue = f"WORKER:{bucket}:{worker_name}"

if not is_lfa:
    rucio_rse = os.environ.get("RUCIO_RSE", None)
    if rucio_rse:
        dtn_url = os.environ["RUCIO_DTN"]
        if not dtn_url.endswith("/"):
            dtn_url += "/"
        rucio_interface = RucioInterface(rucio_rse, dtn_url, bucket, os.environ["RUCIO_SCOPE"])


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
        info = Info.from_path(dataset.path.geturl())
        logger.debug("%s", info)
        with r.pipeline() as pipe:
            pipe.lrem(worker_queue, 0, info.path)
            pipe.hset(f"FILE:{info.path}", "ingest_time", str(time.time()))
            pipe.hincrby(f"INGEST:{info.bucket}:{info.instrument}", f"{info.obs_day}", 1)
            pipe.execute()
        if webhook_uri:
            resp = requests.post(webhook_uri, json=info.__dict__, timeout=0.5)
            logger.info("Webhook response: %s", resp)


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
    info = Info(dataset.files[0].filename.geturl())
    logger.debug("%s", info)
    with r.pipeline() as pipe:
        pipe.hincrby(f"FAIL:{info.bucket}:{info.instrument}", f"{info.obs_day}", 1)
        pipe.hset(f"FILE:{info.path}", "ing_fail_exc", str(exc))
        pipe.hincrby(f"FILE:{info.path}", "ing_fail_count", 1)
        pipe.execute()
    if int(r.hget(f"FILE:{info.path}", "ing_fail_count")) >= MAX_FAILURES:
        r.lrem(worker_queue, 0, info.path)


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
    info = Info(dataset.geturl())
    logger.debug("%s", info)
    with r.pipeline() as pipe:
        pipe.hincrby(f"FAIL:{info.bucket}:{info.instrument}", f"{info.obs_day}", 1)
        pipe.hset(f"FILE:{info.path}", "md_fail_exc", str(exc))
        pipe.lrem(worker_queue, 0, info.path)
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

    if not is_lfa:
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

            # Ingest if we have resources
            if resources:
                logger.info("Ingesting %s", resources)
                refs = None
                try:
                    refs = ingester.run(resources)
                except Exception:
                    logger.exception("Error while ingesting %s", resources)

                # Define visits if we ingested anything
                if not is_lfa and refs:
                    try:
                        ids = [ref.dataId for ref in refs]
                        visit_definer.run(ids)
                        logger.info("Defined visits for %s", ids)
                    except Exception:
                        logger.exception("Error while defining visits for %s", refs)
                if not is_lfa and rucio_rse:
                    # Register with Rucio if we ingested anything
                    try:
                        rucio_interface.register(resources)
                    except Exception:
                        logger.exception("Rucio registration failed for %s", resources)

        # Atomically grab the next entry from the bucket queue, blocking until
        # one exists.
        r.blmove(redis_queue, worker_queue, 0, "RIGHT", "LEFT")
        # Be greedy and take as many entries as exist up to max
        n = r.llen(worker_queue)
        while n < max_ingests and r.lmove(redis_queue, worker_queue, "RIGHT", "LEFT"):
            n += 1


if __name__ == "__main__":
    main()
