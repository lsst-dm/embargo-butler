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
Idle worker cleanup service.

After a configurable number of seconds, move all work items from an idle
worker's queue back to the main per-bucket queue.  This avoids losing work
items when a deployment is restarted.
"""
import time

from utils import setup_logging, setup_redis

IDLE_MAX: float = 10
"""Max idle time in seconds for worker queues before requeueing them
(`float`)"""

logger = setup_logging(__name__)
r = setup_redis()


def main():
    """Requeue items from idle worker queues."""
    while True:
        time.sleep(IDLE_MAX)
        logger.debug("Checking for idle queues")
        for queue in r.scan_iter("WORKER:*"):
            idle = r.object("idletime", queue)
            if idle > IDLE_MAX:
                logger.info("Restoring idle queue %s (%f sec)", queue, idle)
                bucket = queue.decode().split(":")[1]
                dest = f"QUEUE:{bucket}"
                # Since the lmove is atomic, no need to lock.
                while r.lmove(queue, dest, "RIGHT", "LEFT") is not None:
                    pass
                logger.info("Done restoring idle queue %s", queue)


if __name__ == "__main__":
    main()
