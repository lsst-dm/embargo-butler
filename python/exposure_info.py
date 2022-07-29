from dataclasses import dataclass
import logging

__all__ = ("ExposureInfo",)

log = logging.Logger(__name__)


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
    detector_full_name: str
    detector_raft_name: str
    detector_name_in_raft: str

    def __init__(self, path):
        try:
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
                instrument_code,
                controller,
                obs_day2,
                seq_num,
            ) = self.exp_id.split("_")
            (
                self.instrument_code,
                self.controller,
                self.obs_day,
                self.seq_num,
                self.detector_raft_name,
                self.detector_name_in_raft,
            ) = self.filename.split("_")
            self.detector_full_name = (f"{self.detector_raft_name}"
                f"_{self.detector_name_in_raft}")
            if obs_day != self.obs_day or obs_day2 != self.obs_day:
                log.warn(f"Mismatched observation dates: {path}")
            if seq_num != self.seq_num:
                log.warn(f"Mismatched sequence numbers: {path}")
        except Exception as e:
            log.error(f"Unable to parse: {path}")
            raise
