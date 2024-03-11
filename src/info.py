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

import logging
from dataclasses import dataclass

__all__ = ("Info", "ExposureInfo", "LfaInfo")

logger = logging.getLogger(__name__)


@dataclass
class Info:
    path: str
    bucket: str
    instrument: str
    filename: str
    obs_day: str

    @staticmethod
    def from_path(path: str) -> Info:
        if path.startswith("s3://"):
            path = path[len("s3://") :]
        if path.startswith("rubinobs-lfa-"):
            return LfaInfo(path)
        else:
            return ExposureInfo(path)


@dataclass
class ExposureInfo(Info):
    """Class used to extract exposure information from notification messages."""

    exp_id: str
    instrument_code: str
    controller: str
    seq_num: str

    def __init__(self, path):
        try:
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


@dataclass
class LfaInfo(Info):
    """Class used to extract LFA information from notification messages."""

    def __init__(self, path):
        try:
            self.path = path
            components = path.split("/")
            if len(components) == 7:
                self.bucket, csc, generator, year, month, day, self.filename = components
                self.instrument = f"{csc}/{generator}"
            elif len(components) == 6:
                self.bucket, self.instrument, year, month, day, self.filename = components
            else:
                raise ValueError(f"Unrecognized number of components: {len(components)}")
            self.obs_day = f"{year}{month}{day}"
        except Exception:
            logger.exception("Unable to parse: %s", path)
            raise
