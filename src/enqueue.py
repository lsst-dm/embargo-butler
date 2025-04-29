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
Enqueue service to post notifications to per-bucket queues.
"""
import os
import re
import time
import urllib.parse

import redis
from flask import Flask, request

from info import ExposureInfo, Info
from utils import setup_logging, setup_redis

FILE_RETENTION: float = 7 * 24 * 60 * 60
"""Time in seconds to remember information about specific FITS files."""

logger = setup_logging(__name__)
r = setup_redis()
notification_secret = os.environ["NOTIFICATION_SECRET"]
regexp = re.compile(os.environ.get("DATASET_REGEXP", r"fits$"))
profile = os.environ.get("PROFILE", "")
if profile != "":
    profile += "@"


def enqueue_objects(objects):
    """Enqueue objects onto per-bucket queues.

    Compute the `Info` for each object with a selected extension and return
    the list.

    Parameters
    ----------
    objects: `list` [`str`]

    Returns
    -------
    info_list: `list` [`Info`]
    """
    info_list = []
    # Use a pipeline for efficiency.
    with r.pipeline() as pipe:
        for o in objects:
            if regexp.search(o):
                info = Info.from_path(o)
                if info.needs_exposure():
                    if not r.hexists("EXPSEEN", info.exp_id):
                        pipe.lpush(f"EXPWAIT:{info.exp_id}", o)
                        logger.info("Wait for exposure %s: %s", info.exp_id, o)
                else:
                    pipe.lpush(f"QUEUE:{info.bucket}", o)
                    logger.info("Enqueued %s to %s", o, info.bucket)
                info_list.append(info)
        pipe.execute()
    return info_list


def update_stats(info_list):
    """Update statistics and monitoring information for each exposure.

    Parameters
    ----------
    info_list: `list` [`Info`]
    """
    max_seqnum = {}
    with r.pipeline() as pipe:
        for info in info_list:
            pipe.hincrby(f"REC:{info.bucket}", info.obs_day, 1)
            bucket_instrument = f"{info.bucket}:{info.instrument}"
            pipe.hincrby(f"RECINSTR:{bucket_instrument}", info.obs_day, 1)
            pipe.hset(f"FILE:{info.path}", "recv_time", str(time.time()))
            pipe.expire(f"FILE:{info.path}", FILE_RETENTION)
            if isinstance(info, ExposureInfo):
                seqnum_key = f"MAXSEQ:{bucket_instrument}:{info.obs_day}"
                max_seqnum[seqnum_key] = max(int(info.seq_num), max_seqnum.get(seqnum_key, 0))
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
            logger.info("Unrecognized secret %s", r["opaqueData"])
            continue
        object_names.append(
            profile + r["s3"]["bucket"]["name"] + "/" + urllib.parse.unquote_plus(r["s3"]["object"]["key"])
        )
    info_list = enqueue_objects(object_names)
    update_stats(info_list)
    return info_list
