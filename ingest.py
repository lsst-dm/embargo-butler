import os
import redis
import time

from lsst.daf.butler import Butler
from lsst.utils import doImportType
from lsst.resources import ResourcePath

r = redis.Redis(host=os.environ["REDIS_HOST"])
r.auth(os.environ["REDIS_PASSWORD"])
redis_key = os.environ["REDIS_KEY"]

def on_success(datasets):
    for dataset in datasets:
        print(dataset.path)
        r.hdel(redis_key, dataset.path[len("s3://"):])

def on_ingest_failure(path, exc):
    print(path, exc)

def on_metadata_failure(path, exc):
    print(path, exc)
    # r.hdel(redis_key, dataset.path[len("s3://"):])

butler = Butler(os.environ["BUTLER_REPO"], writeable=True)
TaskClass = doImportType("lsst.obs.base.RawIngestTask")
ingestConfig = TaskClass.ConfigClass()
ingestConfig.transfer = "direct"
ingester = TaskClass(
    config=ingestConfig,
    butler=butler,
    on_success=on_success,
    on_ingest_failure=on_ingest_failure,
    on_metadata_failure=on_metadata_failure,
)

while True:
    objects = r.hkeys(redis_key)
    resources = [ResourcePath(f"s3://{o.decode()}") for o in objects]
    print(resources)
    ingester.run(resources)
    time.sleep(0.5)
    break
