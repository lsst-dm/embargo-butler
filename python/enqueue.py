import logging
import os
import redis
import time
from exposure_info import ExposureInfo

logging.basicConfig(
        level=logging.DEBUG,
        format="{levelname} {asctime} {name} ({filename}:{lineno}) - {message}",
        style="{")
logger = logging.Logger(__name__)

r = redis.Redis(host=os.environ["REDIS_HOST"])
r.auth(os.environ["REDIS_PASSWORD"])
redis_key = os.environ["REDIS_KEY"]

logger.info(f"Checking for idle queues")
for queue in r.scan_iter("WORKER:*"):
    idle = r.object("idletime", queue)
    if idle > 10:
        logger.info(f"Restoring idle queue {queue}")
        while r.lmove(queue, redis_key) is not None:
            continue
    else:
        logger.info(f"Leaving queue {queue}")

logger.info(f"Waiting on {redis_key}")
while True:
    objects = r.hkeys(redis_key)
    if len(objects) == 0:
        time.sleep(0.5)
    else:
        bucket = None
        object_list = []
        for o in objects:
            print(f"*** Processing {o}")
            e = ExposureInfo(o.decode())
            print(f"*** {e}")
            # Future optimization: gather all objects in the same bucket
            r.lpush(f"QUEUE:{e.bucket}", o)
        r.hdel(redis_key, *objects)
        # Other stuff can wait until after we have dispatched
        for o in objects:
            path = o.decode()
            e = ExposureInfo(path)
            logger.info(f"Enqueued {path}")
            r.hincrby(f"RECEIVED:{e.bucket}:{e.instrument}", e.obs_day, 1)
            r.zadd(f"MAXSEQ:{e.bucket}:{e.instrument}",
                {e.obs_day: int(e.seq_num)}, gt=True)
            r.hset(f"FILE:{path}", "recv_time", str(time.time()))
            r.expire(f"FILE:{path}", 7 * 24 * 60 * 60)
