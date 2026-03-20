from enum import Enum


class GraphLabel(Enum):
    NORMAL = "normal"
    ANOMALY_SOURCE = "anomaly_source"
    ANOMALY_OFFCHAIN = "anomaly_offchain"
    ANOMALY_DESTINATION = "anomaly_destination"