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
Enqueue service to post notifications to per-bucket queues.
"""
import logging
import os
import sys
import time

import redis
from exposure_info import ExposureInfo

SLEEP_INTERVAL: float = 0.2
"""Interval in seconds between checks for new objects if none were found
(`float`)."""

FILE_RETENTION: float = 7 * 24 * 60 * 60
"""Time in seconds to remember information about specific FITS files."""

logging.basicConfig(
    level=logging.DEBUG,
    format="{levelname} {asctime} {name} ({filename}:{lineno}) - {message}",
    style="{",
    stream=sys.stderr,
    force=True,
)
logger = logging.Logger(__name__)

r = redis.Redis(host=os.environ["REDIS_HOST"])
r.auth(os.environ["REDIS_PASSWORD"])
redis_key = os.environ["REDIS_KEY"]
# It's possible for more than one enqueue service to be running, especially
# during upgrades, so define a lock, but use a rapid timeout in case of
# problems.
lock = r.lock("ENQUEUE_LOCK", timeout=1)


def enqueue_objects():
    """Enqueue FITS objects from a Redis hash key onto per-bucket queues.

    Compute the `ExposureInfo` for each FITS object and return the list.

    Returns
    -------
    info_list: `list` [`ExposureInfo`]
    """
    objects = r.hkeys(redis_key)
    info_list = []
    # Use a pipeline for efficiency.
    with r.pipeline() as pipe:
        for o in objects:
            path = o.decode()
            if path.endswith(".fits"):
                e = ExposureInfo(path)
                pipe.lpush(f"QUEUE:{e.bucket}", o)
                pipe.hdel(redis_key, o)
                print(f"*** Enqueued {path} to {e.bucket}", file=sys.stderr)
                logger.info(f"Enqueued {path} to {e.bucket}")
                info_list.append(e)
        pipe.execute()
    return info_list


def update_stats(info_list):
    """Update statistics and monitoring information for each exposure.

    Parameters
    ----------
    info_list: `list` [`ExposureInfo`]
    """
    with r.pipeline() as pipe:
        max_seqnum = {}
        for e in info_list:
            pipe.hincrby(f"REC:{e.bucket}", e.obs_day, 1)
            bucket_instrument = f"{e.bucket}:{e.instrument}"
            pipe.hincrby(f"RECINSTR:{bucket_instrument}", e.obs_day, 1)
            pipe.hset(f"FILE:{e.path}", "recv_time", str(time.time()))
            pipe.expire(f"FILE:{e.path}", FILE_RETENTION)
            seqnum_key = f"MAXSEQ:{bucket_instrument}:{e.obs_day}"
            max_seqnum[seqnum_key] = max(e.seq_num, max_seqnum.get(seqnum_key, 0))
        pipe.execute()

    for seqnum_key in max_seqnum:
        with r.pipeline() as pipe:
            # Retry if max sequence number key was updated before we set it.
            while True:
                try:
                    pipe.watch(seqnum_key)
                    current = pipe.get(seqnum_key)
                    pipe.multi()
                    pipe.set(seqnum_key, max(current, max_seqnum[seqnum_key]))
                    pipe.execute()
                    break
                except redis.WatchError:
                    continue


def main():
    """Enqueue objects while updating statistics and checking for idle
    workers."""
    logger.info(f"Waiting on {redis_key}")
    while True:
        while not lock.acquire():
            pass
        info_list = enqueue_objects()
        lock.release()
        if info_list:
            # Statistics updates can wait until after we have dispatched.
            update_stats(info_list)
        else:
            time.sleep(SLEEP_INTERVAL)


if __name__ == "__main__":
    main()
