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
import socket
import time
from collections import defaultdict

import astropy.io.fits
import requests
from lsst.daf.butler import Butler
from lsst.obs.base import DefineVisitsTask, RawIngestTask
from lsst.obs.lsst import ingest_guider
from lsst.resources import ResourcePath

from info import Info
from utils import setup_logging, setup_redis

max_failures = int(os.environ.get("MAX_FAILURES", "3"))
"""Retry ingests until this many failures."""

max_guider_wait = float(os.environ.get("MAX_GUIDER_WAIT", "5.0"))
"""Wait this long for science images to arrive after guiders."""

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

success_refs = []
retry_guider_obsids = dict()


class _DuplicateIngestError(RuntimeError):
    pass


class _UndefinedExposureError(RuntimeError):
    pass


def on_success(datasets):
    """Callback for successful ingest.

    Record ingest time and statistics.

    Parameter
    ---------
    datasets: `list` [`lsst.daf.butler.FileDataset`]
        The successfully-ingested datasets.
    """

    webhook_filenames = dict()
    for dataset in datasets:
        logger.info("Ingested %s", dataset)
        info = Info.from_path(dataset.path.geturl())
        logger.debug("%s", info)
        with r.pipeline() as pipe:
            pipe.lrem(worker_queue, 0, info.path)
            pipe.hset(f"FILE:{info.path}", "ingest_time", str(time.time()))
            pipe.hincrby(f"INGEST:{info.bucket}:{info.instrument}", f"{info.obs_day}", 1)
            pipe.execute()
        if not is_lfa:
            webhook_filenames.setdefault(info.exp_id, []).append(info.filename)
        # If we ingested something, guiders should now be able to be ingested.
        if info.exp_id in retry_guider_obsids:
            del retry_guider_obsids[info.exp_id]
    if webhook_uri:
        for exp_id in webhook_filenames:
            info_dict = {"exp_id": exp_id, "filenames": webhook_filenames[exp_id]}
            try:
                resp = requests.post(webhook_uri, json=info_dict, timeout=0.5)
                logger.info("Webhook response %s: %s", info_dict, resp)
            except Exception:
                # Ignore webhook exceptions
                logger.exception("Webhook exception for %s", info_dict)


def on_ingest_failure(exposure_data, exc):
    """Callback for ingest failure.

    Record statistics; give up on the dataset if it fails 3 times.

    Parameters
    ----------
    exposure_data: `lsst.obs.base.ingest.RawExposureData`
        Information about raw datasets that failed ingest.
    exc: `Exception`
        Exception raised by the ingest failure.
    """
    assert len(exposure_data.files) == 1
    logger.info("Failed to ingest %s: %s", exposure_data, exc)
    f = exposure_data.files[0]
    info = Info.from_path(f.filename.geturl())
    logger.debug("%s", info)
    if "Datastore already contains" in str(exc):
        logger.info("Already ingested %s", info.path)
        # Don't retry these
        r.lrem(worker_queue, 0, info.path)
        raise _DuplicateIngestError
    logger.warning("Marking for retry %s", info.path)
    with r.pipeline() as pipe:
        pipe.hincrby(f"FAIL:{info.bucket}:{info.instrument}", f"{info.obs_day}", 1)
        pipe.hset(f"FILE:{info.path}", "ing_fail_exc", str(exc))
        pipe.hincrby(f"FILE:{info.path}", "ing_fail_count", 1)
        pipe.execute()
    if int(r.hget(f"FILE:{info.path}", "ing_fail_count")) >= max_failures:
        logger.error("Giving up on %s", info.path)
        r.lrem(worker_queue, 0, info.path)


def on_guider_ingest_failure(datasets, exc):
    """Callback for guider ingest failure.

    Record statistics; give up on the dataset if it fails 3 times.

    Parameters
    ----------
    datasets: `list` [ `lsst.daf.butler.FileDataset` ]
        Raw guider datasets that failed ingest.
    exc: `Exception`
        Exception raised by the ingest failure.
    """
    assert len(datasets) == 1
    dataset = datasets[0]
    logger.info("Failed to ingest %s: %s", dataset, exc)
    info = Info.from_path(dataset.path.geturl())
    logger.debug("%s", info)
    if "Datastore already contains" in str(exc):
        logger.info("Already ingested %s", info.path)
        # Don't retry these
        r.lrem(worker_queue, 0, info.path)
        raise _DuplicateIngestError
    logger.warning("Marking for retry %s", info.path)
    with r.pipeline() as pipe:
        pipe.hincrby(f"FAIL:{info.bucket}:{info.instrument}", f"{info.obs_day}", 1)
        pipe.hset(f"FILE:{info.path}", "ing_fail_exc", str(exc))
        pipe.hincrby(f"FILE:{info.path}", "ing_fail_count", 1)
        pipe.execute()
    if int(r.hget(f"FILE:{info.path}", "ing_fail_count")) >= max_failures:
        logger.error("Giving up on %s", info.path)
        r.lrem(worker_queue, 0, info.path)


def on_undefined_exposure(resource, obs_id):
    """Callback for undefined exposure while ingesting guiders.

    Don't do normal retry processing for these

    Parameters
    ----------
    resource: `lsst.resources.ResourcePath`
        Raw guider dataset that failed ingest.
    obs_id: `str`
        Observation id for which no exposure record was found.
    """
    # No need to log; ingest_guider does that already
    if obs_id not in retry_guider_obsids:
        retry_guider_obsids[obs_id] = time.time()
    if retry_guider_obsids[obs_id] + max_guider_wait <= time.time():
        info = Info.from_path(resource.geturl())
        with r.pipeline() as pipe:
            pipe.hincrby(f"FAIL:{info.bucket}:{info.instrument}", f"{info.obs_day}", 1)
            pipe.hset(f"FILE:{info.path}", "ing_fail_exc", "Max wait")
            pipe.execute()
        logger.error("Giving up on %s", info.path)
        r.lrem(worker_queue, 0, info.path)
        del retry_guider_obsids[obs_id]
    raise _UndefinedExposureError


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
    batch_ingester = RawIngestTask(
        config=ingest_config,
        butler=butler,
        on_success=on_success,
        on_metadata_failure=on_metadata_failure,
    )
    one_by_one_ingester = RawIngestTask(
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
            resources = [ResourcePath(f"s3://{b.decode()}") for b in blobs if b"_guider" not in b]
            guiders = [ResourcePath(f"s3://{b.decode()}") for b in blobs if b"_guider" in b]

            # Ingest if we have resources
            if resources:
                if not is_lfa:
                    record_groups(resources)

                logger.info("Ingesting %s", resources)
                success_refs = []
                try:
                    success_refs = batch_ingester.run(resources)
                except RuntimeError:
                    # Retry one by one
                    for resource in resources:
                        try:
                            success_refs.extend(one_by_one_ingester.run([resource]))
                        except _DuplicateIngestError:
                            pass
                        except Exception:
                            logger.exception("Error while ingesting %s", resource)
                except Exception:
                    logger.exception("Error while ingesting %s", resources)

                # Define visits if we ingested anything
                if not is_lfa and success_refs:
                    id_dict = defaultdict(list)
                    for ref in success_refs:
                        data_id = ref.dataId
                        id_dict[data_id["instrument"]].append(data_id)
                    for ids in id_dict.values():
                        try:
                            visit_definer.run(ids, incremental=True)
                            logger.info("Defined visits for %s", ids)
                        except Exception:
                            logger.exception("Error while defining visits for %s", success_refs)

            # Ingest if we have guiders
            if guiders:
                logger.info("Ingesting %s", guiders)
                try:
                    ingest_guider(
                        butler,
                        guiders,
                        transfer="direct",
                        on_success=on_success,
                        on_metadata_failure=on_metadata_failure,
                    )
                except RuntimeError:
                    # Retry one by one
                    retries = False
                    for guider in guiders:
                        try:
                            ingest_guider(
                                butler,
                                [guider],
                                transfer="direct",
                                on_success=on_success,
                                on_ingest_failure=on_guider_ingest_failure,
                                on_undefined_exposure=on_undefined_exposure,
                                on_metadata_failure=on_metadata_failure,
                            )
                        except _DuplicateIngestError:
                            pass
                        except _UndefinedExposureError:
                            retries = True
                        except Exception:
                            logger.exception("Error while ingesting %s", guider)
                    if retries:
                        # Wait for a science image to show up
                        time.sleep(0.5)
                except Exception:
                    logger.exception("Error while ingesting %s", guiders)

        # If we have any retries, don't wait for new images
        n = r.llen(worker_queue)
        if n == 0:
            # Atomically grab the next entry from the bucket queue, blocking
            # until one exists.
            r.blmove(redis_queue, worker_queue, 0, "RIGHT", "LEFT")
            n = 1

        # Be greedy and take as many entries as exist up to max
        while n < max_ingests and r.lmove(redis_queue, worker_queue, "RIGHT", "LEFT"):
            n += 1


if __name__ == "__main__":
    main()
