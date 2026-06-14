import os
import sys
import types

import yaml
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

from sqlalchemy import create_engine  # noqa: E402
from sqlalchemy.orm import sessionmaker  # noqa: E402

from graph_generator.graph_class import GraphObject  # noqa: E402
from graph_generator.graph_label import (  # noqa: E402
    BlockchainGraphLabel,
    EventType,
    GraphEdgeType,
    GraphNodeType,
)
from repository.graphs.repository import (  # noqa: E402
    GraphEdgeRepository,
    GraphMappingBlockchainRepository,
    GraphMappingCrossChainRepository,
    GraphNodeRepository,
)

_NODE_TYPE_MAP = {
    "user": GraphNodeType.USER.value,
    "token": GraphNodeType.TOKEN.value,
    "router": GraphNodeType.ROUTER.value,
    "log_event": GraphNodeType.LOG_EVENT.value,
    "other_account": GraphNodeType.OTHER_ACCOUNT.value,
    "validator": GraphNodeType.VALIDATOR.value,
}

_EDGE_TYPE_MAP = {
    "token_transfer": GraphEdgeType.TOKEN_TRANSFER.value,
    "token_auth": GraphEdgeType.TOKEN_AUTH.value,
    "function_call": GraphEdgeType.FUNCTION_CALL.value,
    "log_relation": GraphEdgeType.LOG_RELATION.value,
}

_EMITTER_FALLBACK_EVENT_TYPE = {
    "router": EventType.ROUTER_UNKNOWN.value,
    "token": EventType.TOKEN_UNKNOWN.value,
}


def _map_node_type(yaml_type: str) -> str:
    return _NODE_TYPE_MAP.get(yaml_type, GraphNodeType.OTHER_ACCOUNT.value)


def _map_edge_type(yaml_relation_type: str) -> str:
    return _EDGE_TYPE_MAP.get(yaml_relation_type, GraphEdgeType.FUNCTION_CALL.value)


def _resolve_event_type(yaml_event_type: str, emitter_type_str: str) -> str:
    """Use explicit event_type from YAML; fall back to emitter-based heuristic."""
    if yaml_event_type:
        return yaml_event_type
    return _EMITTER_FALLBACK_EVENT_TYPE.get(emitter_type_str, EventType.UNKNOWN.value)


def _resolve_address(address: str, node_type_str: str, router_replace: str) -> str:
    """Collapse all router addresses to a single canonical placeholder when specified."""
    if router_replace and node_type_str == "router":
        return router_replace
    return address.lower()


def connect_to_db():
    DATABASE_URL = os.getenv("DATABASE_URL")
    engine = create_engine(DATABASE_URL)
    return sessionmaker(bind=engine)


class AddNewAttacksScript:
    def __init__(self):
        self.session = connect_to_db()
        self.load_repos()

    def load_repos(self):
        self.cctx_graph_repo = GraphMappingCrossChainRepository(self.session)
        self.chain_graph_repo = GraphMappingBlockchainRepository(self.session)
        self.graph_nodes_repo = GraphNodeRepository(self.session)
        self.graph_edges_repo = GraphEdgeRepository(self.session)

    def import_attacks_from_file(self, yaml_file_path: str):
        with open(yaml_file_path, "r") as f:
            data = yaml.safe_load(f)

        bridge_name = data.get("bridge")
        router_address_replace = data.get("replace_router_address_with", None)

        for blockchain in data.get("blockchains", []):
            chain_name = blockchain.get("blockchain")
            for tx in blockchain.get("txs") or []:
                self.add_attack_to_db(bridge_name, chain_name, router_address_replace, tx)

        self.graph_nodes_repo.refresh_degrees()

    def add_attack_to_db(self, bridge_name: str, chain_name: str, router_address_replace: str, tx: dict):
        tx_hash = tx.get("tx_hash")
        block_number = tx.get("block_number")
        timestamp = tx.get("timestamp")

        if not tx_hash:
            return

        if self.chain_graph_repo.graph_exists(bridge_name, chain_name, tx_hash) is not None:
            print("  Attack tx already added:", chain_name, tx_hash)
            return

        print("Adding attack transaction:", tx_hash, "- chain:", chain_name)
        label = BlockchainGraphLabel.ANOMALY if tx.get("attack") else BlockchainGraphLabel.NORMAL

        # Wrap the bridge string so GraphObject can call .value on it
        bridge_ref = types.SimpleNamespace(value=bridge_name)
        graph_obj = GraphObject(
            self.chain_graph_repo, self.graph_nodes_repo, self.graph_edges_repo,
            token_metadata_repo=None,
        )
        graph_obj.create_graph_mapping(
            bridge_ref, chain_name, tx_hash, block_number, timestamp, label, set()
        )

        op_index = 0

        # Internal transactions represent native-token transfers not captured by log events
        for internal_tx in tx.get("internal_txs") or []:
            from_addr = internal_tx.get("from")
            to_addr = internal_tx.get("to")
            from_type = internal_tx.get("from_type", "other_account")
            to_type = internal_tx.get("to_type", "other_account")
            value = internal_tx.get("value", 0)
            tx_input = internal_tx.get("input") or "0x"
            token_symbol = internal_tx.get("token_symbol")

            from_node = graph_obj.fetch_or_create_node(
                _resolve_address(from_addr, from_type, router_address_replace),
                timestamp, node_type_if_missing=_map_node_type(from_type),
            )
            to_node = graph_obj.fetch_or_create_node(
                _resolve_address(to_addr, to_type, router_address_replace),
                timestamp, node_type_if_missing=_map_node_type(to_type),
            )
            graph_obj.create_edge(
                from_node.node_id, to_node.node_id, GraphEdgeType.TOKEN_TRANSFER.value, op_index
            )

            native_token_node = graph_obj.fetch_or_create_node(
                "token_native", timestamp,
                attributes={"symbol": "ETH", "name": "Native Currency", "decimals": 18},
                node_type_if_missing=GraphNodeType.TOKEN.value,
            )
            log_node = graph_obj.create_log_node(
                op_index,
                topic=f"internal_{op_index}",
                event_type=EventType.TRANSFER.value,
                event_signature="Transfer(address from, address to, uint256 value)",
                event_args={"from": from_addr, "to": to_addr, "value": value},
                event_input=tx_input,
                timestamp=timestamp,
                amount=value,
                token_symbol=token_symbol
            )
            graph_obj.create_edge(
                native_token_node.node_id, log_node.node_id,
                GraphEdgeType.LOG_RELATION.value, op_index
            )
            op_index += 1

        for event in tx.get("events") or []:
            emitted_by = event.get("emitted_by")
            emitter_type = event.get("emitter_type", "other_account")
            event_signature = event.get("event_signature") or ""
            num_args = event.get("num_args", 0)
            event_input = event.get("input") or "0x"
            amount = event.get("amount")
            token_symbol = event.get("token_symbol")
            yaml_event_type = event.get("event_type") or ""

            resolved_emitter = _resolve_address(emitted_by, emitter_type, router_address_replace)
            emitter_node = graph_obj.fetch_or_create_node(
                resolved_emitter, timestamp, node_type_if_missing=_map_node_type(emitter_type),
            )

            event_type = _resolve_event_type(yaml_event_type, emitter_type)
            log_node = graph_obj.create_log_node(
                op_index,
                topic=event_signature or emitted_by,
                event_type=event_type,
                event_signature=event_signature,
                event_args=[None] * num_args,
                event_input=event_input,
                timestamp=timestamp,
                amount=amount,
                token_symbol=token_symbol,
            )
            graph_obj.create_edge(emitter_node.node_id, log_node.node_id, GraphEdgeType.LOG_RELATION.value, op_index)

            for relation in event.get("relations") or []:
                rel_from_addr = relation.get("from", None)
                rel_from_type = relation.get("from_type", "other_account")
                rel_to_addr = relation.get("to", None)
                rel_to_type = relation.get("to_type", "other_account")
                edge_type = _map_edge_type(relation.get("relation_type", ""))

                rel_from_node = graph_obj.fetch_or_create_node(
                    _resolve_address(rel_from_addr, rel_from_type, router_address_replace),
                    timestamp, node_type_if_missing=_map_node_type(rel_from_type),
                ) if rel_from_addr else emitter_node
                rel_to_node = graph_obj.fetch_or_create_node(
                    _resolve_address(rel_to_addr, rel_to_type, router_address_replace),
                    timestamp, node_type_if_missing=_map_node_type(rel_to_type),
                ) if rel_to_addr else emitter_node
                graph_obj.create_edge(rel_from_node.node_id, rel_to_node.node_id, edge_type, op_index)

            op_index += 1


if __name__ == "__main__":
    yaml_file_path = sys.argv[1]
    script = AddNewAttacksScript()
    script.import_attacks_from_file(yaml_file_path)
