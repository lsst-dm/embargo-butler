import os
import redis
import time
from exposure_info import ExposureInfo

r = redis.Redis(host=os.environ["REDIS_HOST"])
r.auth(os.environ["REDIS_PASSWORD"])
redis_key = os.environ["REDIS_KEY"]

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
            r.rpush(f"QUEUE:{bucket}", o)
        r.hdel(redis_key, *objects)
        # Other stuff can wait until after we have dispatched
        for o in objects:
            e = ExposureInfo(o.decode())
            r.hincrby("RECEIVED", e.obs_day, 1)
            r.zadd("MAXSEQ", {e.obs_day: int(e.seq)}, gt=True)
            r.hset(f"FILE:{o}", "recv_time", str(time.time()))
            r.expire(f"FILE:{file}", 7 * 24 * 60 * 60)
