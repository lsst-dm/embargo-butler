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
Presence service to translate group names to image URIs.
"""
import os
import re

from flask import Flask

from utils import setup_logging, setup_redis

logger = setup_logging(__name__)
r = setup_redis()

# Delete image key when seen, before it expires, to save space
delete_seen = os.environ.get("DELETE_SEEN") is not None

app = Flask(__name__)


@app.get("/presence/<instrument>/<group_name>/<int:snap_index>/<detector_name>")
def presence(instrument: str, group_name: str, snap_index: int, detector_name: str) -> dict | tuple:
    """Return the presence and URI of an image matching the parameters.

    Parameters
    ----------
    instrument: `str`
        Name of the instrument taking the image.
    group_name: `str`
        Name of the group (from the GROUPID FITS header).
    snap_index: `int`
        Number of the snap (zero-based).
    detector_name: `str`
        Name of the detector ("RNN_SNN").

    Returns
    -------
    json: `dict`
        JSON with "error", "present", "uri", and/or "message" keys.
    """

    try:
        if instrument not in ("LATISS", "LSSTComCam", "LSSTComCamSim", "LSSTCam", "LSST-TS8"):
            return ({"error": True, "message": f"Unknown instrument {instrument}"}, 400)
        if not re.match(r"R\d\d_S\d\d", detector_name):
            return ({"error": True, "message": f"Unrecognized detector name {detector_name}"}, 400)
        key = f"GROUP:{instrument}:{group_name}:{snap_index}:{detector_name}"
        result = r.get(key)
        if result:
            logger.info(f"Found key {key}")
            if delete_seen:
                r.delete(key)
            return {"error": False, "present": True, "uri": result.decode()}
        else:
            logger.debug(f"No key {key}")
            return {"error": False, "present": False}
    except Exception as e:
        return ({"error": True, "message": str(e)}, 500)
