from graph_generator.graph_label import BlockchainType, GraphNodeType
from repository.base import BaseRepository

from .models import (
    GraphEdge,
    GraphMappingBlockchain,
    GraphMappingCrossChain,
    GraphNode,
)


class GraphMappingBlockchainRepository(BaseRepository):
    def __init__(self, session_factory):
        super().__init__(GraphMappingBlockchain, session_factory)

    def get_by_id(self, graph_id: int):
        with self.get_session() as session:
            return session.query(GraphMappingBlockchain).filter(GraphMappingBlockchain.graph_id == graph_id).first()

    def graph_exists(self, bridge: str, blockchain: str, tx_hash: str):
        with self.get_session() as session:
            return session.query(GraphMappingBlockchain).filter(GraphMappingBlockchain.bridge == bridge, GraphMappingBlockchain.blockchain == blockchain, GraphMappingBlockchain.tx_hash == tx_hash).first()

    def assign_cctx_id(self, graph_id: int, cctx_graph_id: int):
        with self.get_session() as session:
            graph_mapping = session.query(GraphMappingBlockchain).filter(GraphMappingBlockchain.graph_id == graph_id).first()
            if graph_mapping:
                graph_mapping.cctx_graph_id = cctx_graph_id
                session.commit()
                return graph_mapping
            return None

class GraphMappingCrossChainRepository(BaseRepository):
    def __init__(self, session_factory):
        super().__init__(GraphMappingCrossChain, session_factory)

    def get_by_id(self, cctx_graph_id: int):
        with self.get_session() as session:
            return session.query(GraphMappingCrossChain).filter(GraphMappingCrossChain.cctx_graph_id == cctx_graph_id).first()

    def graph_exists(self, bridge: str, cctx_id: int):
        with self.get_session() as session:
            return session.query(GraphMappingCrossChain).filter(GraphMappingCrossChain.bridge == bridge, GraphMappingCrossChain.cctx_id == cctx_id).first()

    def get_by_chain_tx_hash(self, bridge: str, chain: str, tx_hash: str):
        with self.get_session() as session:
            source = session.query(GraphMappingCrossChain).filter(
                GraphMappingCrossChain.bridge == bridge,
                GraphMappingCrossChain.source_chain == chain, 
                GraphMappingCrossChain.source_tx_hash == tx_hash
            ).first()
            if source is not None:
                return source
            else:
                return session.query(GraphMappingCrossChain).filter(
                    GraphMappingCrossChain.bridge == bridge,
                    GraphMappingCrossChain.target_chain == chain, 
                    GraphMappingCrossChain.destination_tx_hash == tx_hash
                ).first()
            
class GraphNodeRepository(BaseRepository):
    def __init__(self, session_factory):
        super().__init__(GraphNode, session_factory)

    def get_by_address(self, graph_id: int, address: str):
        with self.get_session() as session:
            return session.query(GraphNode).filter(GraphNode.chain_graph_id == graph_id, GraphNode.address == address).first()

    def update_node_type(self, node_id: int, new_type: str):
        with self.get_session() as session:
            node = session.query(GraphNode).filter(GraphNode.node_id == node_id).first()
            if node:
                node.node_type = new_type
                session.commit()
                return node
            return None
    
    def get_by_chain_graph_id(self, graph_id: int):
        with self.get_session() as session:
            return session.query(GraphNode).filter(GraphNode.chain_graph_id == graph_id).all()

    def assign_cctx_id(self, graph_id: int, cctx_id: int, blockchain_type: BlockchainType = None):
        with self.get_session() as session:
            nodes = session.query(GraphNode).filter(GraphNode.chain_graph_id == graph_id).all()
            for node in nodes:
                node.cctx_graph_id = cctx_id
                if blockchain_type is not None:
                    node.blockchain_type = blockchain_type.value
            session.commit()
            return nodes
        
    def get_router_node_by_graph_id(self, graph_id: int):
        with self.get_session() as session:
            return session.query(GraphNode).filter(GraphNode.chain_graph_id == graph_id, GraphNode.node_type == GraphNodeType.ROUTER.value).first()

class GraphEdgeRepository(BaseRepository):
    def __init__(self, session_factory):
        super().__init__(GraphEdge, session_factory)

    def get_by_connections(self, graph_id: int, source_id: int, target_id: int):
        with self.get_session() as session:
            return session.query(GraphEdge).filter(GraphEdge.chain_graph_id == graph_id, GraphEdge.source_id == source_id, GraphEdge.target_id == target_id).first()
        
    def get_by_chain_graph_id(self, graph_id: int):
        with self.get_session() as session:
            return session.query(GraphEdge).filter(GraphEdge.chain_graph_id == graph_id).all()

    def assign_cctx_id(self, graph_id: int, cctx_id: int, blockchain_type: BlockchainType = None):
        with self.get_session() as session:
            edges = session.query(GraphEdge).filter(GraphEdge.chain_graph_id == graph_id).all()
            for edge in edges:
                edge.cctx_graph_id = cctx_id
                if blockchain_type is not None:
                    edge.blockchain_type = blockchain_type.value
            session.commit()
            return edges