import logging
import os
import redis
import time
from exposure_info import ExposureInfo

logging.basicConfig(
        level=logging.DEBUG,
        format="{levelname} {asctime} {name}"
            "({MDC[LABEL]})({filename}:{lineno}) - {message}",
        style="{")
logger = logging.Logger(__name__)

r = redis.Redis(host=os.environ["REDIS_HOST"])
r.auth(os.environ["REDIS_PASSWORD"])
redis_key = os.environ["REDIS_KEY"]

logger.info(f"Waiting on {redis_key}")
while True:
    objects = r.hkeys(redis_key)
    if len(objects) == 0:
        time.sleep(0.5)
    else:
        bucket = None
        object_list = []
        for o in objects:
            e = ExposureInfo(o.decode())
            # Future optimization: gather all objects in the same bucket
            r.rpush(f"QUEUE:{e.bucket}", o)
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
