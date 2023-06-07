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
Utility functions for embargo_butler services.
"""
import logging
import os
import re
import sys

import redis

__all__ = ["setup_logging", "setup_redis"]


def setup_logging(module: str):
    """Set up logging for a service.

    Levels for any logger component may be set via the LOG_CONFIG environment
    variable as a comma-separated list of "component=LEVEL" settings.  The
    special "." component refers to the root level of the LSST Science
    Pipelines loggers.

    Parameter
    ---------
    module: `str`
        The ``__name__`` of the calling module.

    Returns
    -------
    logger: `logging.Logger`
        The logger to use for the module.
    """

    logging.basicConfig(
        level=logging.INFO,
        format="{levelname} {asctime} {name} ({filename}:{lineno}) - {message}",
        style="{",
        stream=sys.stderr,
        force=True,
    )

    logger = logging.getLogger(module)

    logspec = os.environ.get("LOG_CONFIG")
    if logspec:
        # One-line "component=LEVEL" logging specification parser.
        for component, level in re.findall(r"(?:([\w.]*)=)?(\w+)", logspec):
            if component == ".":
                # Specially handle "." as a component to mean the lsst root
                component = "lsst"
            logging.getLogger(component).setLevel(level)

    return logger


def setup_redis():
    """Set up Redis server connection, including authentication.

    REDIS_HOST and REDIS_PASSWORD environment variables must be set.

    Returns
    -------
    redis: `redis.Redis`
        The Redis connection.
    """

    r = redis.Redis(host=os.environ["REDIS_HOST"], password=os.environ["REDIS_PASSWORD"])
    return r
