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
Service to determine when complete exposures have been ingested.
"""
import logging
import os
import socket
import sys
import time

import redis
from lsst.resources import ResourcePath

from exposure_info import ExposureInfo

logging.basicConfig(
    level=logging.INFO,
    format="{levelname} {asctime} {name} ({filename}:{lineno}) - {message}",
    style="{",
    stream=sys.stderr,
    force=True,
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

r = redis.Redis(host=os.environ["REDIS_HOST"])
r.auth(os.environ["REDIS_PASSWORD"])
bucket = os.environ["BUCKET"]
redis_queue = f"DETQUEUE:{bucket}"

worker_name = socket.gethostname()
worker_queue = f"DETWORKER:{bucket}:{worker_name}"


def main():
    """Track detector ingests for an exposure."""
    logger.info("Waiting on %s", worker_queue)
    while True:
        # Process any entries on the worker queue.
        if r.llen(worker_queue) > 0:
            blobs = r.lrange(worker_queue, 0, -1)
            resources = []
            for b in blobs:
                if b.endswith(b"_detector.json"):
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
