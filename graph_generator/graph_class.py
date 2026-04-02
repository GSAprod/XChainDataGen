

from config.constants import Bridge
from graph_generator.graph_label import GraphLabel, GraphNodeType
from repository.graphs.models import GraphMappingBlockchain


class GraphObject:
    def __init__(self, graph_mapping_repo, node_repo, edge_repo, token_metadata_repo):
        self.graph_mapping_repo = graph_mapping_repo
        self.node_repo = node_repo
        self.edge_repo = edge_repo
        self.token_metadata_repo = token_metadata_repo

        self.graph_mapping = None
        self.nodes = []
        self.edges = []

    def create_graph_mapping(self, bridge: Bridge, blockchain: str, tx_hash: str, block_number: int, label: GraphLabel) -> GraphMappingBlockchain:
        self.graph_mapping = self.graph_mapping_repo.create({
            "bridge": bridge.value,
            "blockchain": blockchain,
            "tx_hash": tx_hash,
            "block_number": block_number,
            "label": label.value
        })
        return self.graph_mapping
    
    def attach_graph_mapping(self, graph_mapping: GraphMappingBlockchain):
        self.graph_mapping = graph_mapping

    def attach_nodes(self, nodes):
        self.nodes = nodes
    
    def create_node(self, node_data):
        node = self.node_repo.create(node_data)
        self.nodes.append(node)
        return node
    
    def fetch_or_create_token_node(self, address: str):
        for node in self.nodes:
            if node.address.lower() == address.lower():
                return node
        
        token_metadata = self.token_metadata_repo.get_token_metadata_by_contract_and_blockchain(address, self.graph_mapping.blockchain)
        new_node_data = {
            "chain_graph_id": self.graph_mapping.graph_id,
            "node_type": GraphNodeType.TOKEN.value,
            "bridge": self.graph_mapping.bridge,
            "blockchain": self.graph_mapping.blockchain,
            "address": address,
            "attributes": {
                "symbol": token_metadata.symbol,
                "name": token_metadata.name,
                "decimals": token_metadata.decimals
            } if token_metadata else None,
            "attributes_text": f"type = token; blockchain = {self.graph_mapping.blockchain}; symbol = {token_metadata.symbol}; name = {token_metadata.name}; decimals = {token_metadata.decimals}" if token_metadata else None
        }
        return self.create_node(new_node_data)

    def fetch_or_create_node(self, address, attributes=None, attributes_text=None, timestamp=None, node_type_if_missing=GraphNodeType.OTHER_ACCOUNT.value):
        for node in self.nodes:
            if node.address.lower() == address.lower():
                return node
        # If not found, create a new node with the provided type
        new_node_data = {
            "chain_graph_id": self.graph_mapping.graph_id,
            "node_type": node_type_if_missing,
            "bridge": self.graph_mapping.bridge,
            "blockchain": self.graph_mapping.blockchain,
            "address": address,
        }
        if attributes is not None:
            new_node_data["attributes"] = attributes
        if attributes_text is not None:
            new_node_data["attributes_text"] = attributes_text
        if timestamp is not None:
            new_node_data["timestamp"] = timestamp
        return self.create_node(new_node_data)

    def create_log_node(self, topic, event_signature, event_args, attributes_text=None, amount=None, amount_usd=None):
        log_node_data = {
            "chain_graph_id": self.graph_mapping.graph_id,
            "node_type": GraphNodeType.LOG_EVENT.value,
            "bridge": self.graph_mapping.bridge,
            "blockchain": self.graph_mapping.blockchain,
            "address": topic,       # NOTE: There can be multiple events with the same topic.
            "attributes": {
                "event_signature": event_signature,
                "event_args": event_args,
            }
        }
        if attributes_text is not None:
            log_node_data["attributes_text"] = attributes_text
        if amount is not None:
            log_node_data["amount"] = amount
        if amount_usd is not None:
            log_node_data["amount_usd"] = amount_usd
        return self.create_node(log_node_data)

    def update_node_type(self, node_id: int, new_type: str):
        updated_node = self.node_repo.update_node_type(node_id, new_type)
        if updated_node:
            # Update the node in the local list as well
            for i, node in enumerate(self.nodes):
                if node.node_id == node_id:
                    self.nodes[i] = updated_node
                    break
        return updated_node

    def create_edge(self, source_id, target_id, edge_type, attributes=None, attributes_text=None):
        edge_data = {
            "chain_graph_id": self.graph_mapping.graph_id,
            "bridge": self.graph_mapping.bridge,
            "blockchain": self.graph_mapping.blockchain,
            "source_id": source_id,
            "target_id": target_id,
            "edge_type": edge_type,
        }
        if attributes is not None:
            edge_data["attributes"] = attributes
        if attributes_text is not None:
            edge_data["attributes_text"] = attributes_text
        edge = self.edge_repo.create(edge_data)
        self.edges.append(edge)
        return edge

    def find_or_create_edge(self, source_id, target_id, edge_type, attributes=None, attributes_text=None):
        for edge in self.edges:
            if edge.source_id == source_id and edge.target_id == target_id and edge.edge_type == edge_type:
                return edge
        return self.create_edge(source_id, target_id, edge_type, attributes, attributes_text)

    def fetch_node(self, node_id):
        for node in self.nodes:
            if node.node_id == node_id:
                return node
        return None
    

    def fetch_node_by_address(self, address, create_if_not_exists=False):
        for node in self.nodes:
            if node.address.lower() == address.lower():
                return node
        return None
    
    def fetch_edge(self, edge_id):
        for edge in self.edges:
            if edge.edge_id == edge_id:
                return edge
        return None
