from enum import Enum


class GraphLabel(Enum):
    NORMAL = "normal"
    ANOMALY_SOURCE = "anomaly_source"
    ANOMALY_OFFCHAIN = "anomaly_offchain"
    ANOMALY_DESTINATION = "anomaly_destination"

class GraphCompletion(Enum):
    COMPLETE = "complete"
    SOURCE_ONLY = "source_only"
    DESTINATION_ONLY = "destination_only"