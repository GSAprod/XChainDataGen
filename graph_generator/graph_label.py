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

class BlockchainType(Enum):
    SOURCE = "source"
    DESTINATION = "destination"
    OFFCHAIN = "offchain"

class GraphNodeType(Enum):
    USER = "user"
    ROUTER = "router"
    TOKEN = "token"
    OTHER_ACCOUNT = "other_account"
    LOG_EVENT = "log_event"
    VALIDATOR = "validator"

class GraphEdgeType(Enum):
    TRANSACTION = "transaction"
    TOKEN_TRANSFER = "token_transfer"
    TOKEN_AUTH = "token_auth"
    FUNCTION_CALL = "function_call"
    LOG_RELATION = "log_relation"
    CROSS_CHAIN_RELATION = "cross_chain_relation"