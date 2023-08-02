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
ExposureInfo class used to extract information from notification messages.
"""
import logging
from dataclasses import dataclass

__all__ = ("ExposureInfo",)

logger = logging.getLogger(__name__)


@dataclass
class ExposureInfo:
    path: str
    bucket: str
    instrument: str
    filename: str
    exp_id: str
    instrument_code: str
    controller: str
    obs_day: str
    seq_num: str

    def __init__(self, path):
        try:
            if path.startswith("s3://"):
                path = path[len("s3://") :]
            self.path = path
            (
                self.bucket,
                self.instrument,
                self.obs_day,
                self.exp_id,
                self.filename,
            ) = path.split("/")
            (
                self.instrument_code,
                self.controller,
                obs_day,
                self.seq_num,
            ) = self.exp_id.split("_")
            if obs_day != self.obs_day:
                logger.warn("Mismatched observation dates: %s", path)
        except Exception:
            logger.exception("Unable to parse: %s", path)
            raise
