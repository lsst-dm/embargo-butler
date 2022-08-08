import logging
import os
import sys
import time

import redis

IDLE_MAX: float = 10
"""Max idle time in seconds for worker queues before requeueing them
(`float`)"""

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


def main():
    """Requeue items from idle worker queues."""
    while True:
        time.sleep(IDLE_MAX)
        logger.info("Checking for idle queues")
        for queue in r.scan_iter("WORKER:*"):
            idle = r.object("idletime", queue)
            if idle > IDLE_MAX:
                logger.info(f"Restoring idle queue {queue} ({idle} sec)")
                bucket = queue.split(":")[1]
                dest = f"QUEUE:{bucket}"
                # Since the lmove is atomic, no need to lock.
                while r.lmove(queue, dest, 0, "RIGHT", "LEFT") is not None:
                    continue


if __name__ == "__main__":
    main()
