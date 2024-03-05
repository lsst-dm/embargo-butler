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

import hashlib
import logging
import random
import re
import time
from typing import Any
import zlib

from lsst.daf.butler import DatasetRef
from lsst.resources import ResourcePath
from rucio.client.didclient import DIDClient
from rucio.client.replicaclient import ReplicaClient
import rucio.common.exception

__all__ = ["RucioInterface"]

logger = logging.getLogger(__name__)


class RucioInterface:
    """Register files in Rucio and attach them to datasets.

    Parameters
    ----------
    rucio_rse: `str`
        Name of the RSE that the files live in.
    dtn_url: `str`
        Base URL of the data transfer node for the Rucio physical filename.
    bucket: `str`
        Name of the S3 bucket that the files live in.
    scope: `str`
        Rucio scope to register the files in.
    """

    def __init__(self, rucio_rse: str, dtn_url: str, bucket: str, scope: str):
        self.rucio_rse = rucio_rse
        self.dtn_url = dtn_url
        self.pfn_base = f"{dtn_url}{bucket}/"
        self.scope = scope

        self.replica_client = ReplicaClient()
        self.did_client = DIDClient()

    def _make_did(self, res: ResourcePath) -> dict[str, str | int]:
        """Make a Rucio data identifier dictionary from a resource.

        Parameters
        ----------
        res: `lsst.resources.ResourcePath`
            Path to the file.

        Returns
        -------
        did: `dict [ str, str|int ]`
            Rucio data identifier including physical and logical names,
            byte length, adler32 and MD5 checksums, and scope.
        """
        with res.open("rb") as f:
            contents = f.read()
            size = len(contents)
            md5 = hashlib.md5(contents).hexdigest()
            adler32 = f"{zlib.adler32(contents):08x}"
        path = res.path.removeprefix("/")
        pfn = self.pfn_base + path
        return dict(pfn=pfn, bytes=size, adler32=adler32, md5=md5, name=path, scope=self.scope)

    def _add_files_to_dataset(self, dids: list[dict], dataset_id: str) -> None:
        """Attach a list of files specified by Rucio DIDs to a Rucio dataset.

        Ignores already-attached files for idempotency.

        Parameters
        ----------
        dids: `list [ dict [ str, str|int ] ]`
            List of Rucio data identifiers.
        dataset_id: `str`
            Logical name of the Rucio dataset.
        """
        retries = 0
        max_retries = 2
        while True:
            try:
                self.did_client.add_files_to_dataset(
                    scope=self.scope,
                    name=dataset_id,
                    files=dids,
                    rse=self.rucio_rse,
                )
                return
            except rucio.common.exception.FileAlreadyExists:
                # At least one already is in the dataset.
                # This shouldn't happen, but if it does,
                # we have to retry each individually.
                for did in dids:
                    try:
                        self.did_client.add_files_to_dataset(
                            scope=self.scope,
                            name=dataset_id,
                            files=[did],
                            rse=self.rucio_rse,
                        )
                    except rucio.common.exception.FileAlreadyExists:
                        pass
                return
            except rucio.common.exception.DatabaseException:
                retries += 1
                if retries < max_retries:
                    time.sleep(random.uniform(0.5, 2))
                    continue
                else:
                    raise

    def register(
        self,
        resources: list[ResourcePath],
        refs: list[DatasetRef] = [],
    ) -> None:
        """Register a list of files in Rucio.

        Parameters
        ----------
        resources: `list [ lsst.resources.ResourcePath ]`
            List of resource paths to files.
        refs: `list [ lsst.daf.butler.DatasetRef ]`
            Corresponding list of DatasetRefs for datasets (optional).
        """
        if refs:
            data = []
            for r, path in zip(refs, resources):
                did = self._make_did(path)
                did["meta"] = {
                    "rubin_butler": 1,
                    "rubin_metadata": r.to_json(),
                }
                data.append(did)
        else:
            data = [self._make_did(path) for path in resources]
        self._register(data)
        logger.info("Done with Rucio for %s", resources)

    def _register(self, dids: list[dict[str, Any]]) -> None:
        datasets = dict()
        for did in dids:
            # For raw images, use a dataset per 100 exposures
            dataset_id = re.sub(
                r"(.+?)/(\d+)/[A-Z]{2}_[A-Z]_\2_(\d{4})\d{2}/.*",
                r"Dataset/\1/\2/\3",
                did["name"],
            )
            datasets.setdefault(dataset_id, []).append(did)

        for dataset_id, dids in datasets.items():
            try:
                logger.info("Registering %s in dataset %s, RSE %s", dids, dataset_id, self.rucio_rse)
                self._add_files_to_dataset(dids, dataset_id)
            except rucio.common.exception.DataIdentifierNotFound:
                # No such dataset, so create it
                try:
                    logger.info("Creating Rucio dataset %s", dataset_id)
                    self.did_client.add_dataset(
                        scope=self.scope,
                        name=dataset_id,
                        statuses={"monotonic": True},
                        rse=self.rucio_rse,
                    )
                except rucio.common.exception.DataIdentifierAlreadyExists:
                    # If someone else created it in the meantime
                    pass
                # And then retry adding DIDs
                self._add_files_to_dataset(dids, dataset_id)
