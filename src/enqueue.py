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

Consumes S3 ObjectCreated notifications that Ceph RGW publishes (as plain
JSON) to a Kafka topic, and pushes the resulting object paths onto per-bucket
Redis queues for the ingest workers.
"""
import asyncio
import json
import os
import re
import time
import urllib.parse

import aiokafka
import redis

from info import ExposureInfo, Info
from utils import setup_logging, setup_redis

FILE_RETENTION: float = 7 * 24 * 60 * 60
"""Time in seconds to remember information about specific FITS files."""

logger = setup_logging(__name__)
r = setup_redis()
regexp = re.compile(os.environ.get("DATASET_REGEXP", r"fits$"))
profile = os.environ.get("PROFILE", "")
if profile != "":
    profile += "@"


def enqueue_objects(objects):
    """Enqueue objects onto per-bucket queues.

    Compute the `Info` for each object with a selected extension and return
    the list.  Paths that have already been enqueued within the
    ``FILE_RETENTION`` window are skipped, so Kafka redeliveries (rebalance,
    pod restart) and operator re-trigger tooling do not double-enqueue.

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
            if not regexp.search(o):
                continue
            # SET NX EX is the dedupe guard; pipeline can't conditionally
            # enqueue, so this is a separate round trip per matched object.
            if not r.set(f"ENQ:{o}", "1", nx=True, ex=int(FILE_RETENTION)):
                logger.info("Skipping duplicate enqueue %s", o)
                continue
            info = Info.from_path(o)
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


def object_names_from_notification(payload):
    """Extract object names from an S3 bucket notification message.

    Ceph RGW publishes notifications to Kafka as plain JSON in the standard
    S3 event-notification schema.  Records that aren't ``ObjectCreated*`` are
    skipped, as are records that are missing required fields.

    Parameters
    ----------
    payload: `bytes` or `str`
        The raw Kafka message value.

    Returns
    -------
    object_names: `list` [`str`]
        ``profile + bucket + "/" + key`` strings ready for `Info.from_path`.
    """
    object_names = []
    msg = json.loads(payload)
    for record in msg.get("Records", []):
        if not record.get("eventName", "").startswith("ObjectCreated"):
            continue
        try:
            bucket = record["s3"]["bucket"]["name"]
            key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
        except (KeyError, TypeError):
            logger.exception("Invalid S3 notification record: %s", record)
            continue
        object_names.append(profile + bucket + "/" + key)
    return object_names


async def main():
    """Consume S3 notifications from Kafka and enqueue objects on Redis.

    Offsets are committed manually after a message has been successfully
    processed (parse + ``enqueue_objects`` + ``update_stats``).  Parse-stage
    failures (malformed JSON, ``None`` tombstone) are treated as poison pills
    and committed so they don't block the partition; process-stage failures
    (e.g. Redis errors) deliberately leave the offset uncommitted so the
    message is redelivered on the next poll or after a pod restart.
    """
    consumer = aiokafka.AIOKafkaConsumer(
        os.environ["KAFKA_TOPIC"],
        bootstrap_servers=os.environ["KAFKA_CLUSTER"],
        group_id=os.environ.get("KAFKA_GROUP_ID", "embargo-butler-enqueue"),
        auto_offset_reset=os.environ.get("KAFKA_OFFSET_RESET", "latest"),
        enable_auto_commit=False,
    )
    await consumer.start()
    logger.info(
        "Kafka consumer started: topic=%s cluster=%s group=%s",
        os.environ["KAFKA_TOPIC"],
        os.environ["KAFKA_CLUSTER"],
        os.environ.get("KAFKA_GROUP_ID", "embargo-butler-enqueue"),
    )
    try:
        async for msg in consumer:
            try:
                object_names = object_names_from_notification(msg.value)
            except (AttributeError, TypeError, ValueError, UnicodeDecodeError):
                logger.exception(
                    "Skipping malformed Kafka message topic=%s partition=%s offset=%s",
                    msg.topic,
                    msg.partition,
                    msg.offset,
                )
                await consumer.commit()
                continue

            try:
                if object_names:
                    info_list = enqueue_objects(object_names)
                    update_stats(info_list)
            except Exception:
                logger.exception(
                    "Failed to enqueue Kafka message topic=%s partition=%s offset=%s; "
                    "leaving offset uncommitted for redelivery",
                    msg.topic,
                    msg.partition,
                    msg.offset,
                )
                continue

            await consumer.commit()
    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())
