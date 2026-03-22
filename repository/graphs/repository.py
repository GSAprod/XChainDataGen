from repository.base import BaseRepository

from .models import BlockchainGraphMapping, GraphEdge, CrossChainGraphMapping, GraphNode

class BlockchainGraphMappingRepository(BaseRepository):
    def __init__(self, session_factory):
        super().__init__(BlockchainGraphMapping, session_factory)

    def get_by_id(self, graph_id: int):
        with self.get_session() as session:
            return session.query(BlockchainGraphMapping).filter(BlockchainGraphMapping.graph_id == graph_id).first()

    def graph_exists(self, bridge: str, blockchain: str, tx_hash: str):
        with self.get_session() as session:
            return session.query(BlockchainGraphMapping).filter(BlockchainGraphMapping.bridge == bridge, BlockchainGraphMapping.blockchain == blockchain, BlockchainGraphMapping.tx_hash == tx_hash).first()

class CrossChainGraphMappingRepository(BaseRepository):
    def __init__(self, session_factory):
        super().__init__(CrossChainGraphMapping, session_factory)

    def get_by_id(self, graph_id: int):
        with self.get_session() as session:
            return session.query(CrossChainGraphMapping).filter(CrossChainGraphMapping.graph_id == graph_id).first()

    def graph_exists(self, bridge: str, cctx_id: int):
        with self.get_session() as session:
            return session.query(CrossChainGraphMapping).filter(CrossChainGraphMapping.bridge == bridge, CrossChainGraphMapping.cctx_id == cctx_id).first()
        
class GraphNodeRepository(BaseRepository):
    def __init__(self, session_factory):
        super().__init__(GraphNode, session_factory)

    def get_by_address(self, graph_id: int, address: str):
        with self.get_session() as session:
            return session.query(GraphNode).filter(GraphNode.graph_id == graph_id, GraphNode.address == address).first()

    def update_node_type(self, node_id: int, new_type: str):
        with self.get_session() as session:
            node = session.query(GraphNode).filter(GraphNode.node_id == node_id).first()
            if node:
                node.node_type = new_type
                session.commit()
                return node
            return None

class GraphEdgeRepository(BaseRepository):
    def __init__(self, session_factory):
        super().__init__(GraphEdge, session_factory)

    def get_by_connections(self, graph_id: int, source_id: int, target_id: int):
        with self.get_session() as session:
            return session.query(GraphEdge).filter(GraphEdge.chain_graph_id == graph_id, GraphEdge.source_id == source_id, GraphEdge.target_id == target_id).first()