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
import json
import os
import re
import socket
import time

import astropy.io.fits
import requests
from lsst.daf.butler import Butler
from lsst.pipe.base import Instrument
from lsst.obs.base import DefineVisitsTask, RawIngestTask
from lsst.obs.lsst import PhotodiodeIngestTask
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
    os.environ["LSST_DISABLE_BUCKET_VALIDATION"] = "1"
redis_queue = f"QUEUE:{bucket}"
butler_repo = os.environ["BUTLER_REPO"]
webhook_uri = os.environ.get("WEBHOOK_URI", None)
is_lfa = "rubinobs-lfa-" in bucket
group_lifetime = int(os.environ.get("GROUP_LIFETIME", 86400))

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
    info = Info.from_path(dataset.files[0].filename.geturl())
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
    info = Info.from_path(dataset.geturl())
    logger.debug("%s", info)
    with r.pipeline() as pipe:
        pipe.hincrby(f"FAIL:{info.bucket}:{info.instrument}", f"{info.obs_day}", 1)
        pipe.hset(f"FILE:{info.path}", "md_fail_exc", str(exc))
        pipe.lrem(worker_queue, 0, info.path)
        pipe.execute()


def record_groups(resources: list[ResourcePath]) -> None:
    """Record the group ids from received FITS files in Redis.

    Parameters
    ----------
    resources: `list` [`ResourcePath`]
        The resources to record group ids from.
    """

    global r, group_lifetime, logger
    with r.pipeline() as pipe:
        for res in resources:
            if not res.exists():
                continue
            json_file = res.updatedExtension("json")
            header = {}
            try:
                with json_file.open("rb") as f:
                    header = json.load(f)
            except Exception:
                try:
                    with res.open("rb") as f:
                        header = astropy.io.fits.open(f)[0].header
                except Exception:
                    logger.exception("Error reading header for %s", res)
            try:
                instrument = header["INSTRUME"]
                # Canonicalize instrument name
                match instrument.lower():
                    case "lsstcomcamsim" | "comcamsim":
                        instrument = "LSSTComCamSim"
                    case "lsstcomcam" | "comcam":
                        instrument = "LSSTComCam"
                    case "lsstcam":
                        instrument = "LSSTCam"
                    case "latiss":
                        instrument = "LATISS"
                if "GROUPID" in header:
                    groupid = header["GROUPID"]
                    snap_number = int(header["CURINDEX"]) - 1
                    detector = header["RAFTBAY"] + "_" + header["CCDSLOT"]
                    key = f"GROUP:{instrument}:{groupid}:{snap_number}:{detector}"
                    pipe.set(key, str(res))
                    pipe.expire(key, group_lifetime)
            except Exception:
                logger.exception("Error reading snap/detector for %s", res)
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

    if is_lfa:
        # LSSTCam photodiode is copy mode only.
        instrument = Instrument.from_string("LSSTCam", butler.registry)
        lsstcam_photodiode_ingester = PhotodiodeIngestTask(
            config=PhotodiodeIngestTask.ConfigClass(),
            butler=butler,
            instrument=instrument,
            on_success=on_success,
            on_ingest_failure=on_ingest_failure,
            on_metadata_failure=on_metadata_failure,
        )
    else:
        define_visits_config = DefineVisitsTask.ConfigClass()
        define_visits_config.groupExposures = "one-to-one"
        visit_definer = DefineVisitsTask(config=define_visits_config, butler=butler)

    logger.info("Waiting on %s", worker_queue)
    while True:
        # Process any entries on the worker queue.
        if r.llen(worker_queue) > 0:
            blobs = r.lrange(worker_queue, 0, -1)
            resources = [ResourcePath(f"s3://{b.decode()}") for b in blobs]

            # Ingest if we have resources
            if resources:

                if not is_lfa:
                    record_groups(resources)

                logger.info("Ingesting %s", resources)
                refs = None
                if is_lfa:
                    resources_photodiode = []
                    resources_others = []
                    for resource in resources:
                        if re.search(r"MTCamera/photodiode.*_photodiode.ecsv", resource):
                            resources_photodiode.append(resource)
                        else:
                            resources_others.append(resource)
                    resources = resources_others
                try:
                    refs = ingester.run(resources)
                except Exception:
                    logger.exception("Error while ingesting %s, retrying one by one", resources)
                    refs = []
                    for resource in resources:
                        try:
                            refs.extend(ingester.run([resource]))
                        except Exception:
                            logger.exception("Error while ingesting %s", resource)
                            info = Info.from_path(resource.geturl())
                            r.lrem(worker_queue, 0, info.path)

                if is_lfa and resources_photodiode:
                    try:
                        refs = lsstcam_photodiode_ingester.run(resources_photodiode)
                    except Exception:
                        logger.exception(
                            "Error while ingesting %s, retrying one by one", resources_photodiode
                        )
                        refs = []
                        for resource in resources_photodiode:
                            try:
                                refs.extend(lsstcam_photodiode_ingester.run([resource]))
                            except Exception:
                                logger.exception("Error while ingesting %s", resource)
                                info = Info.from_path(resource.geturl())
                                r.lrem(worker_queue, 0, info.path)

                # Define visits if we ingested anything
                if not is_lfa and refs:
                    ids = [ref.dataId for ref in refs]
                    try:
                        visit_definer.run(ids, incremental=True)
                        logger.info("Defined visits for %s", ids)
                    except Exception:
                        logger.exception("Error while defining visits for %s, retrying one by one", refs)
                        for id in ids:
                            try:
                                visit_definer.run([id], incremental=True)
                                logger.info("Defined visit for %s", id)
                            except Exception:
                                logger.exception("Error while defining visits for %s", id)
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
