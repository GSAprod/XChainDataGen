

from config.constants import Bridge
from graph_generator.graph_label import CrossChainGraphLabel, GraphNodeType
from repository.graphs.models import GraphMappingBlockchain


class GraphObject:
    def __init__(self, graph_mapping_repo, node_repo, edge_repo, token_metadata_repo):
        self.graph_mapping_repo = graph_mapping_repo
        self.node_repo = node_repo
        self.edge_repo = edge_repo
        self.token_metadata_repo = token_metadata_repo

        self.graph_mapping = None
        self.tx_timestamp = None
        self.nodes = []
        self.edges = []

    def load_from_db(self, bridge: Bridge, blockchain: str, tx_hash: str):
        self.graph_mapping = self.graph_mapping_repo.graph_exists(bridge.value, blockchain, tx_hash)
        if not self.graph_mapping:
            raise Exception(f"No graph mapping found for bridge {bridge.value}, blockchain {blockchain}, tx_hash {tx_hash}")

        self.nodes = self.node_repo.get_by_chain_graph_id(self.graph_mapping.graph_id)
        self.edges = self.edge_repo.get_by_chain_graph_id(self.graph_mapping.graph_id)
        
        # If possible, set the graph's timestamp based on the nodes
        for node in self.nodes:
            if node.timestamp is not None:
                self.tx_timestamp = node.timestamp
                break

        return self

    def create_graph_mapping(self, bridge: Bridge, blockchain: str, tx_hash: str, block_number: int, timestamp: int, label: CrossChainGraphLabel) -> GraphMappingBlockchain:
        self.graph_mapping = self.graph_mapping_repo.create({
            "bridge": bridge.value,
            "blockchain": blockchain,
            "tx_hash": tx_hash,
            "block_number": block_number,
            "timestamp": timestamp,
            "label": label.value,
        })
        self.tx_timestamp = timestamp
        return self.graph_mapping
    
    def attach_graph_mapping(self, graph_mapping: GraphMappingBlockchain):
        self.graph_mapping = graph_mapping

    def attach_nodes(self, nodes):
        self.nodes = nodes
    
    def create_node(self, node_data):
        node = self.node_repo.create(node_data)
        self.nodes.append(node)
        return node
    
    def fetch_or_create_token_node(self, address: str, timestamp: int):
        for node in self.nodes:
            if node.address.lower() == address.lower():
                return node
        
        token_metadata = self.token_metadata_repo.get_token_metadata_by_contract_and_blockchain(address, self.graph_mapping.blockchain)
        event_list = "event Transfer(address from, address to, uint256 value), " + \
                     "event Approval(address _owner, address _spender, uint256 _value)"
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
            "attributes_text": f"""type = token;
blockchain = {self.graph_mapping.blockchain};
symbol = {token_metadata.symbol};
name = {token_metadata.name};
decimals = {token_metadata.decimals};
event_list = {event_list}""" if token_metadata else None,
            "token_symbol": token_metadata.symbol if token_metadata else None,
            "timestamp": timestamp
        }
        return self.create_node(new_node_data)

    def fetch_or_create_node(self, address, timestamp, attributes=None, attributes_text=None, node_type_if_missing=GraphNodeType.OTHER_ACCOUNT.value):
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

    def create_log_node(self, event_index, topic, event_type, event_signature, event_args, event_input, timestamp, attributes_text=None, amount=None, amount_usd=None, token_symbol=None):
        log_node_data = {
            "chain_graph_id": self.graph_mapping.graph_id,
            "node_type": GraphNodeType.LOG_EVENT.value,
            "bridge": self.graph_mapping.bridge,
            "blockchain": self.graph_mapping.blockchain,
            "address": topic,       # NOTE: There can be multiple events with the same topic.
            "attributes": {
                "event_signature": event_signature,
                "event_type": event_type,
                "event_args": event_args,
                "event_input": event_input,
                "num_args": len(event_args),
                "input_size": len(event_input[2:]) // 32 if event_input and event_input.startswith("0x") else 0
            },
            "event_order": event_index,
            "timestamp": timestamp
        }
        if attributes_text is not None:
            log_node_data["attributes_text"] = attributes_text
        if amount is not None:
            log_node_data["amount"] = amount
        if amount_usd is not None:
            log_node_data["amount_usd"] = amount_usd
        if token_symbol is not None:
            log_node_data["token_symbol"] = token_symbol
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

    def create_edge(self, source_id, target_id, edge_type, event_index, attributes=None, attributes_text=None):
        edge_data = {
            "chain_graph_id": self.graph_mapping.graph_id,
            "bridge": self.graph_mapping.bridge,
            "blockchain": self.graph_mapping.blockchain,
            "source_id": source_id,
            "target_id": target_id,
            "edge_type": edge_type,
            "event_order": event_index
        }
        if attributes is not None:
            edge_data["attributes"] = attributes
        if attributes_text is not None:
            edge_data["attributes_text"] = attributes_text
        edge = self.edge_repo.create(edge_data)
        self.edges.append(edge)
        return edge

    def find_or_create_edge(self, source_id, target_id, edge_type, event_index, attributes=None, attributes_text=None):
        for edge in self.edges:
            if edge.source_id == source_id and edge.target_id == target_id and edge.edge_type == edge_type:
                return edge
        return self.create_edge(source_id, target_id, edge_type, event_index, attributes, attributes_text)

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
