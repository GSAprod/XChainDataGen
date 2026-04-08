
from sqlalchemy import JSON, BigInteger, Column, Float, Integer, Numeric, String

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

class GraphNode(Base):
    __tablename__ = "graph_nodes"

    node_id = Column(Integer, nullable=False, autoincrement=True, primary_key=True)
    node_type = Column(String(50), nullable=False)
    chain_graph_id = Column(Integer, nullable=False)
    cctx_graph_id = Column(Integer, nullable=True) # Can be null for nodes that are not linked to a cross-chain transaction
    bridge = Column(String(255), nullable=False)
    blockchain = Column(String(255), nullable=True) # Can be none for validator nodes
    blockchain_type = Column(String(255), nullable=True) # If a node is part of the source, target chain, or in an offchain process
    address = Column(String(255), nullable=True)  # Can be none for relays and logs
    attributes = Column(JSON, nullable=True) # JSON object of attributes
    attributes_text = Column(String, nullable=True) # Text description of attributes for LLM input
    amount = Column(Numeric(80), nullable=True)
    amount_usd = Column(Numeric(80), nullable=True)
    event_order = Column(Integer, nullable=True) # Order of events in the same transaction, starting from 0
    timestamp = Column(BigInteger, nullable=True) # Timestamp of the transaction

class GraphEdge(Base):
    __tablename__ = "graph_edges"

    edge_id = Column(Integer, nullable=False, autoincrement=True, primary_key=True)
    edge_type = Column(String(50), nullable=False)
    chain_graph_id = Column(Integer, nullable=False)
    cctx_graph_id = Column(Integer, nullable=True) # Can be null for edges that are not linked to a cross-chain transaction
    bridge = Column(String(255), nullable=False)
    blockchain = Column(String(255), nullable=True) # Can be none for edges that are not linked to a specific blockchain
    blockchain_type = Column(String(255), nullable=True) # If a node is part of the source, target chain, or in an offchain process
    source_id = Column(Integer, nullable=False)
    target_id = Column(Integer, nullable=False)
    attributes = Column(JSON, nullable=True) # JSON object of attributes
    attributes_text = Column(String, nullable=True) # Text description of attributes for LLM input
    tx_hash = Column(String(255), nullable=True)
    amount = Column(Numeric(80), nullable=True)
    amount_usd = Column(Numeric(80), nullable=True)
    deposit_id = Column(String(255), nullable=True)
    withdrawal_id = Column(String(255), nullable=True)
    event_order = Column(Integer, nullable=True) # Order of events in the same transaction, starting from 0
    timestamp = Column(BigInteger, nullable=True)