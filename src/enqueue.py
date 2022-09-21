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
import urllib.parse

from flask import Flask, request
import redis
from exposure_info import ExposureInfo


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
notification_secret = os.environ["NOTIFICATION_SECRET"]


def enqueue_objects(objects):
    """Enqueue FITS objects onto per-bucket queues.

    Compute the `ExposureInfo` for each FITS object and return the list.

    Parameters
    ----------
    objects: `list` [`str`]

    Returns
    -------
    info_list: `list` [`ExposureInfo`]
    """
    info_list = []
    # Use a pipeline for efficiency.
    with r.pipeline() as pipe:
        for o in objects:
            if o.endswith(".fits"):
                e = ExposureInfo(o)
                pipe.lpush(f"QUEUE:{e.bucket}", o)
                print(f"*** Enqueued {o} to {e.bucket}", file=sys.stderr)
                logger.info(f"Enqueued {o} to {e.bucket}")
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
            max_seqnum[seqnum_key] = max(int(e.seq_num), max_seqnum.get(seqnum_key, 0))
        pipe.execute()

    for seqnum_key in max_seqnum:
        with r.pipeline() as pipe:
            # Retry if max sequence number key was updated before we set it.
            while True:
                try:
                    pipe.watch(seqnum_key)
                    current = pipe.get(seqnum_key)
                    if current is None:
                        value = max_seqnum[seqnum_key]
                    else:
                        value = max(int(current), max_seqnum[seqnum_key])
                    pipe.multi()
                    pipe.set(seqnum_key, value)
                    pipe.execute()
                    break
                except redis.WatchError:
                    continue


app = Flask(__name__)


@app.post("/notify")
def notify():
    object_names = []
    for r in request.json["Records"]:
        if r["opaqueData"] != notification_secret:
            print(f"*** Unrecognized secret {r['opaqueData']}", file=sys.stderr)
            logger.info(f"Unrecognized secret {r['opaqueData']}")
            continue
        object_names.append(
            r["s3"]["bucket"]["name"] + "/" + urllib.parse.unquote_plus(r["s3"]["object"]["key"])
        )
    info_list = enqueue_objects(object_names)
    update_stats(info_list)
    return info_list
