from repository.base import BaseRepository

from .models import GraphEdge, GraphMapping, GraphNode


class GraphNodeRepository(BaseRepository):
    def __init__(self, session_factory):
        super().__init__(GraphNode, session_factory)

    def get_by_address(self, graph_id: int, address: str):
        with self.get_session() as session:
            return session.query(GraphNode).filter(GraphNode.graph_id == graph_id, GraphNode.address == address).first()

class GraphEdgeRepository(BaseRepository):
    def __init__(self, session_factory):
        super().__init__(GraphEdge, session_factory)

    def get_by_connections(self, graph_id: int, source_id: int, target_id: int):
        with self.get_session() as session:
            return session.query(GraphEdge).filter(GraphEdge.graph_id == graph_id, GraphEdge.source_id == source_id, GraphEdge.target_id == target_id).first()

class GraphMappingRepository(BaseRepository):
    def __init__(self, session_factory):
        super().__init__(GraphMapping, session_factory)

    def get_by_id(self, graph_id: int):
        with self.get_session() as session:
            return session.query(GraphMapping).filter(GraphMapping.graph_id == graph_id).first()

    def graph_exists(self, bridge: str, cctx_id: int):
        with self.get_session() as session:
            return session.query(GraphMapping).filter(GraphMapping.bridge == bridge, GraphMapping.cctx_id == cctx_id).first()