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
import re
from dataclasses import dataclass
from typing import Self

__all__ = ("Info", "ExposureInfo", "LfaInfo")

logger = logging.getLogger(__name__)


@dataclass
class Info:
    """Base class for information extracted from notification messages."""

    path: str
    """Path component of the S3 URL.
    """

    bucket: str
    """Bucket component of the S3 URL.
    """

    instrument: str
    """Instrument name.
    """

    filename: str
    """Filename component of the S3 URL.
    """

    obs_day: str
    """Observation day (in timezone UTC-12).
    """

    @staticmethod
    def from_path(url: str) -> Self:
        """Create an `Info` of the proper subclass from an S3 URL.

        Parameters
        ----------
        url: `str`
            S3 URL (including bucket).

        Returns
        -------
        info: `Info`
            Either an `LfaInfo` or an `ExposureInfo` as appropriate.
        """

        if url.startswith("s3://"):
            url = url[len("s3://") :]
        if "rubinobs-lfa-" in url:
            return LfaInfo(url)
        else:
            return ExposureInfo(url)

    def needs_exposure(self) -> bool:
        return False

    def is_raw(self) -> bool:
        return False


@dataclass
class ExposureInfo(Info):
    """Class used to extract exposure information from notification messages.

    Parameters
    ----------
    path: `str`
        Path portion of S3 URL (after bucket).
    """

    exp_id: str
    """Exposure ID.
    """

    instrument_code: str
    """Instrument code (two characters).
    """

    controller: str
    """Controller code (one character).
    """

    seq_num: str
    """Sequence number within an observation day.
    """

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

    def needs_exposure(self) -> bool:
        return self.filename.endswith("_guider.fits")

    def is_raw(self) -> bool:
        return bool(re.search(r"\d\.fits", self.filename))


@dataclass
class LfaInfo(Info):
    """Class used to extract LFA information from notification messages.

    Parameters
    ----------
    path: `str`
        Path portion of S3 URL (after bucket).
    """

    def __init__(self, path):
        try:
            self.path = path
            components = path.split("/")
            if len(components) == 8:
                self.bucket, csc, generator, year, month, day, directory, self.filename = components
                self.instrument = f"{csc}/{generator}"
            elif len(components) == 7:
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
