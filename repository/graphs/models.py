from enum import Enum

from sqlalchemy import JSON, BigInteger, Column, Integer, Numeric, String

from repository.database import Base


class GraphMappingBlockchain(Base):
    __tablename__ = "graph_mapping_blockchain"

    graph_id = Column(Integer, nullable=False, autoincrement=True, primary_key=True)
    cctx_graph_id = Column(Integer, nullable=True) # Can be null for transactions that are not linked to a cross-chain transaction
    bridge = Column(String(20), nullable=False)
    blockchain = Column(String(20), nullable=False)
    tx_hash = Column(String(66), nullable=False)
    block_number = Column(Integer, nullable=False)
    label = Column(String(20), nullable=False)

class GraphMappingCrossChain(Base):
    __tablename__ = "graph_mapping_cross_chain"

    cctx_graph_id = Column(Integer, nullable=False, autoincrement=True, primary_key=True)
    cctx_id = Column(Integer, nullable=True)
    bridge = Column(String(20), nullable=False)
    source_chain = Column(String(20), nullable=False)
    target_chain = Column(String(20), nullable=False)
    source_tx_hash = Column(String(66), nullable=False)
    destination_tx_hash = Column(String(66), nullable=False)
    label = Column(String(20), nullable=False)

class GraphNodeType(Enum):
    USER = "user"
    ROUTER = "router"
    TOKEN = "token"
    OTHER_ACCOUNT = "other_account"
    LOG_EVENT = "log_event"
    VALIDATOR = "validator"

class GraphNode(Base):
    __tablename__ = "graph_nodes"

    node_id = Column(Integer, nullable=False, autoincrement=True, primary_key=True)
    node_type = Column(String(50), nullable=False)
    chain_graph_id = Column(Integer, nullable=False)
    cctx_graph_id = Column(Integer, nullable=True) # Can be null for nodes that are not linked to a cross-chain transaction
    bridge = Column(String(255), nullable=False)
    blockchain = Column(String(255), nullable=True) # Can be none for validator nodes
    address = Column(String(255), nullable=True)  # Can be none for relays and logs
    attributes = Column(JSON, nullable=True) # JSON object of attributes
    attributes_text = Column(String, nullable=True) # Text description of attributes for LLM input
    timestamp = Column(BigInteger, nullable=True)

class GraphEdgeType(Enum):
    TRANSACTION = "transaction"
    TOKEN_TRANSFER = "token_transfer"
    TOKEN_AUTH = "token_auth"
    FUNCTION_CALL = "function_call"
    LOG_RELATION = "log_relation"
    CROSS_CHAIN_RELATION = "cross_chain_relation"

class GraphEdge(Base):
    __tablename__ = "graph_edges"

    edge_id = Column(Integer, nullable=False, autoincrement=True, primary_key=True)
    edge_type = Column(String(50), nullable=False)
    chain_graph_id = Column(Integer, nullable=False)
    cctx_graph_id = Column(Integer, nullable=True) # Can be null for edges that are not linked to a cross-chain transaction
    bridge = Column(String(255), nullable=False)
    blockchain = Column(String(255), nullable=True) # Can be none for edges that are not linked to a specific blockchain
    source_id = Column(Integer, nullable=False)
    target_id = Column(Integer, nullable=False)
    attributes = Column(JSON, nullable=True) # JSON object of attributes
    attributes_text = Column(String, nullable=True) # Text description of attributes for LLM input
    tx_hash = Column(String(255), nullable=True)
    amount = Column(Numeric(30), nullable=True)
    deposit_id = Column(String(255), nullable=True)
    withdrawal_id = Column(String(255), nullable=True)
    timestamp = Column(BigInteger, nullable=True)