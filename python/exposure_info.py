from dataclasses import dataclass
import logging

__all__ = ("ExposureInfo",)

log = logging.Logger(__name__)


@dataclass
class ExposureInfo:
    path: str
    bucket: str
    instrument: str
    obs_day: str
    exp_id: str
    filename: str
    instrument_code: str
    controller: str
    seq_num: str

    def __init__(self, path):
        if path.startswith("s3://"):
            path = path[len("s3://"):]
        self.path = path
        (
            self.bucket,
            self.instrument,
            obs_day,
            self.exp_id,
            self.filename,
        ) = path.split("/")
        (
            self.instrument_code,
            self.controller,
            self.obs_day,
            self.seq_num,
        ) = exp_id.split("_")
        if obs_day != self.obs_day:
            log.warn(f"Mismatched observation dates: {path}")
