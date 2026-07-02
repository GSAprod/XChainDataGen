import os
import sys
import types

import yaml
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

from sqlalchemy import create_engine  # noqa: E402
from sqlalchemy.orm import sessionmaker  # noqa: E402

from dune.dune_client import DuneClient  # noqa: E402
from graph_generator.graph_class import GraphObject  # noqa: E402
from graph_generator.graph_label import (  # noqa: E402
    BlockchainGraphLabel,
    BlockchainType,
    CrossChainGraphLabel,
    EventType,
    GraphEdgeType,
    GraphNodeType,
)
from graph_generator.pricing import TokenPricingService  # noqa: E402
from repository.common.models import TokenMetadata  # noqa: E402
from repository.common.repository import (  # noqa: E402
    TokenMetadataRepository,
    TokenPriceRepository,
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
    "transaction": GraphEdgeType.TRANSACTION.value,
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
        self.pricing = None
        self.bridge_name = None
        self.cctx_id_linking = {
            #"cctx_id": [(blockchain, tx_hash1), (blockchain, tx_hash2)]
        }

    def load_repos(self):
        self.cctx_graph_repo = GraphMappingCrossChainRepository(self.session)
        self.chain_graph_repo = GraphMappingBlockchainRepository(self.session)
        self.graph_nodes_repo = GraphNodeRepository(self.session)
        self.graph_edges_repo = GraphEdgeRepository(self.session)
        self.token_metadata_repo = TokenMetadataRepository(self.session)
        self.token_price_repo = TokenPriceRepository(self.session)

    def import_attacks_from_file(self, yaml_file_path: str):
        with open(yaml_file_path, "r") as f:
            data = yaml.safe_load(f)

        bridge_name = data.get("bridge")
        self.bridge_name = bridge_name
        router_address_replace = data.get("replace_router_address_with", None)
        bridge_ref = types.SimpleNamespace(value=bridge_name)

        # Compute timestamp window across all txs for price fetching
        all_ts = [
            tx.get("timestamp")
            for bc in data.get("blockchains", [])
            for tx in bc.get("txs") or []
            if tx.get("timestamp")
        ]
        min_ts, max_ts = (min(all_ts), max(all_ts)) if all_ts else (0, 0)

        try:
            dune_client = DuneClient(bridge_ref)
        except Exception:
            dune_client = None

        self.pricing = TokenPricingService(
            bridge_ref, self.token_metadata_repo, self.token_price_repo,
            dune_client, lambda: (min_ts, max_ts),
        )
        self.pricing.reset()

        for blockchain in data.get("blockchains", []):
            chain_name = blockchain.get("blockchain")
            for tx in blockchain.get("txs") or []:
                self.add_attack_to_db(bridge_name, chain_name, router_address_replace, tx)

        # Backfill any USD amounts that couldn't be resolved during processing
        self.pricing.batch_resolve_pending(self.graph_nodes_repo)

        self.link_cross_chain_txs()
        self.graph_nodes_repo.refresh_degrees()

    def _create_native_value_transfer(
        self,
        graph_obj,
        chain_name: str,
        from_node,
        to_node,
        from_addr: str,
        to_addr: str,
        value,
        tx_input: str,
        token_symbol,
        timestamp,
        op_index: int,
    ) -> int:
        graph_obj.create_edge(
            from_node.node_id, to_node.node_id, GraphEdgeType.TOKEN_TRANSFER.value, op_index
        )
        native_token_node = graph_obj.fetch_or_create_node(
            "token_native", timestamp,
            attributes={"symbol": "ETH", "name": "Native Currency", "decimals": 18},
            node_type_if_missing=GraphNodeType.TOKEN.value,
        )
        amount_float, amount_usd = self.pricing.resolve_native_amount(chain_name, value, timestamp)
        log_node = graph_obj.create_log_node(
            op_index,
            topic=f"internal_{op_index}",
            event_type=EventType.TRANSFER.value,
            event_signature="Transfer(address from, address to, uint256 value)",
            event_args={"from": from_addr, "to": to_addr, "value": value},
            event_input=tx_input,
            timestamp=timestamp,
            amount=value,
            amount_usd=amount_usd,
            token_symbol=token_symbol,
        )
        if amount_usd is None:
            self.pricing.record_missing_price(log_node.node_id, "ETH", timestamp, amount_float)
        graph_obj.create_edge(
            native_token_node.node_id, log_node.node_id, GraphEdgeType.LOG_RELATION.value, op_index
        )
        return op_index + 1

    def _process_main_interaction(
        self, tx: dict, graph_obj, chain_name: str, router_replace: str, timestamp, op_index: int
    ) -> int:
        main_interaction = tx.get("main_interaction")
        if not main_interaction:
            return op_index

        from_addr = main_interaction.get("from")
        to_addr = main_interaction.get("to")
        from_type = main_interaction.get("from_type", "other_account")
        to_type = main_interaction.get("to_type", "other_account")
        value = main_interaction.get("value")
        tx_input = main_interaction.get("input") or "0x"
        token_symbol = main_interaction.get("token_symbol")

        from_node = graph_obj.fetch_or_create_node(
            _resolve_address(from_addr, from_type, router_replace),
            timestamp, node_type_if_missing=_map_node_type(from_type),
        )
        to_node = graph_obj.fetch_or_create_node(
            _resolve_address(to_addr, to_type, router_replace),
            timestamp, node_type_if_missing=_map_node_type(to_type),
        )
        if from_node.node_id == to_node.node_id:
            return op_index

        graph_obj.create_edge(
            from_node.node_id, to_node.node_id, GraphEdgeType.TRANSACTION.value, op_index
        )
        if value is not None:
            op_index = self._create_native_value_transfer(
                graph_obj, chain_name, from_node, to_node,
                from_addr, to_addr, value, tx_input, token_symbol, timestamp, op_index,
            )
        return op_index

    def _process_internal_txs(
        self, tx: dict, graph_obj, chain_name: str, router_replace: str, timestamp, op_index: int
    ) -> int:
        # Internal transactions represent either:
        # - native-token transfers not captured by log events
        # - internal function calls between contracts
        for internal_tx in tx.get("internal_txs") or []:
            from_addr = internal_tx.get("from")
            to_addr = internal_tx.get("to")
            from_type = internal_tx.get("from_type", "other_account")
            to_type = internal_tx.get("to_type", "other_account")
            value = internal_tx.get("value")
            tx_input = internal_tx.get("input") or "0x"
            token_symbol = internal_tx.get("token_symbol")

            from_node = graph_obj.fetch_or_create_node(
                _resolve_address(from_addr, from_type, router_replace),
                timestamp, node_type_if_missing=_map_node_type(from_type),
            )
            to_node = graph_obj.fetch_or_create_node(
                _resolve_address(to_addr, to_type, router_replace),
                timestamp, node_type_if_missing=_map_node_type(to_type),
            )
            if from_node.node_id == to_node.node_id:
                continue

            if value is not None:
                op_index = self._create_native_value_transfer(
                    graph_obj, chain_name, from_node, to_node,
                    from_addr, to_addr, value, tx_input, token_symbol, timestamp, op_index,
                )
            else:
                graph_obj.create_edge(
                    from_node.node_id, to_node.node_id, GraphEdgeType.FUNCTION_CALL.value
                )
        return op_index

    def _process_events(
        self, tx: dict, graph_obj, chain_name: str, router_replace: str, timestamp, op_index: int
    ) -> int:
        for event in tx.get("events") or []:
            emitted_by = event.get("emitted_by")
            emitter_type = event.get("emitter_type", "other_account")
            event_signature = event.get("event_signature") or ""
            num_args = event.get("num_args", 0)
            event_input = event.get("input") or "0x"
            amount = event.get("amount")
            token_symbol = event.get("token_symbol")
            token_address = event.get("token_address")
            token_decimals = event.get("token_decimals")
            yaml_event_type = event.get("event_type") or ""

            resolved_emitter = _resolve_address(emitted_by, emitter_type, router_replace)
            emitter_node = graph_obj.fetch_or_create_node(
                resolved_emitter, timestamp, node_type_if_missing=_map_node_type(emitter_type),
            )

            # Resolve USD amount when a raw amount is provided
            amount_usd = None
            amount_float = None
            if amount is not None:
                if not token_address:
                    # If no token address is provided, use the emitter address as a fallback
                    token_address = emitted_by

                metadata = self.token_metadata_repo \
                    .get_token_metadata_by_contract_and_blockchain(token_address, chain_name)
                if metadata is None and token_symbol:
                    metadata = self.token_metadata_repo \
                        .get_token_metadata_by_symbol_and_blockchain(token_symbol, chain_name)
                # If still not found but decimals were provided in the YAML, build a minimal object
                if metadata is None and token_decimals is not None and token_symbol:
                    metadata = TokenMetadata(
                        symbol=token_symbol,
                        name=token_symbol,
                        decimals=token_decimals,
                        blockchain=chain_name,
                        address=token_address,
                    )
                if metadata is not None:
                    amount_float, amount_usd = self.pricing.resolve_token_amount(
                        metadata, amount, timestamp
                    )

            event_type = _resolve_event_type(yaml_event_type, emitter_type)
            log_node = graph_obj.create_log_node(
                op_index,
                topic=f"{emitted_by}_{op_index}_{event_type}",
                event_type=event_type,
                event_signature=event_signature,
                event_args=[None] * num_args,
                event_input=event_input,
                timestamp=timestamp,
                amount=amount,
                amount_usd=amount_usd,
                token_symbol=token_symbol,
            )
            if amount_usd is None and amount_float is not None:
                # Price date missing; queue for Dune backfill
                self.pricing.record_missing_price(
                    log_node.node_id, token_symbol, timestamp, amount_float
                )
            graph_obj.create_edge(
                emitter_node.node_id, log_node.node_id, GraphEdgeType.LOG_RELATION.value, op_index
            )

            for relation in event.get("relations") or []:
                rel_from_addr = relation.get("from", None)
                rel_from_type = relation.get("from_type", "other_account")
                rel_to_addr = relation.get("to", None)
                rel_to_type = relation.get("to_type", "other_account")
                edge_type = _map_edge_type(relation.get("relation_type", ""))

                rel_from_node = graph_obj.fetch_or_create_node(
                    _resolve_address(rel_from_addr, rel_from_type, router_replace),
                    timestamp, node_type_if_missing=_map_node_type(rel_from_type),
                ) if rel_from_addr else emitter_node
                rel_to_node = graph_obj.fetch_or_create_node(
                    _resolve_address(rel_to_addr, rel_to_type, router_replace),
                    timestamp, node_type_if_missing=_map_node_type(rel_to_type),
                ) if rel_to_addr else emitter_node
                graph_obj.create_edge(
                    rel_from_node.node_id, rel_to_node.node_id, edge_type, op_index
                )

            op_index += 1
        return op_index

    def add_attack_to_db(
        self, bridge_name: str, chain_name: str, router_address_replace: str, tx: dict
    ):
        tx_hash = tx.get("tx_hash")
        block_number = tx.get("block_number")
        timestamp = tx.get("timestamp")

        if not tx_hash:
            return

        if self.chain_graph_repo.graph_exists(bridge_name, chain_name, tx_hash) is not None:
            print("  Attack tx already added:", chain_name, tx_hash)
            return

        cctx_id = tx.get("cctx_id")
        if cctx_id is not None:
            self.cctx_id_linking.setdefault(cctx_id, []).append((chain_name, tx_hash))

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
        op_index = self._process_main_interaction(
            tx, graph_obj, chain_name, router_address_replace, timestamp, op_index
        )
        op_index = self._process_internal_txs(
            tx, graph_obj, chain_name, router_address_replace, timestamp, op_index
        )
        self._process_events(
            tx, graph_obj, chain_name, router_address_replace, timestamp, op_index
        )

    def link_cross_chain_txs(self): #! TO BE TESTED
        # Filter out cctx_linking entries whose value lists don't have 2 entries
        # (i.e., missing one side of the cross-chain relation)
        valid_links = {
            cctx_id: links for cctx_id, links in self.cctx_id_linking.items() if len(links) == 2
        }

        for cctx_id, links in valid_links.items():
            (chain_a, tx_hash_a), (chain_b, tx_hash_b) = links
            graph_mapping_a = self.chain_graph_repo.graph_exists(self.bridge_name, chain_a, tx_hash_a)
            graph_mapping_b = self.chain_graph_repo.graph_exists(self.bridge_name, chain_b, tx_hash_b)

            if graph_mapping_a is None or graph_mapping_b is None:
                print(f"Warning: Missing graph mapping for cctx_id {cctx_id} - skipping link")
                continue

            # Skip if a cctx mapping already exists for either side
            if self.cctx_graph_repo.get_by_chain_tx_hash(self.bridge_name, chain_a, tx_hash_a):
                continue
            if self.cctx_graph_repo.get_by_chain_tx_hash(self.bridge_name, chain_b, tx_hash_b):
                continue

            # Ensure source is the earlier transaction (lower timestamp)
            if graph_mapping_b.timestamp < graph_mapping_a.timestamp:
                graph_mapping_a, graph_mapping_b = graph_mapping_b, graph_mapping_a
                chain_a, chain_b = chain_b, chain_a
                tx_hash_a, tx_hash_b = tx_hash_b, tx_hash_a
            src_mapping, dst_mapping = graph_mapping_a, graph_mapping_b

            # Determine cross-chain label from the individual chain labels
            if src_mapping.label == BlockchainGraphLabel.ANOMALY.value:
                cctx_label = CrossChainGraphLabel.ANOMALY_SOURCE
            elif dst_mapping.label == BlockchainGraphLabel.ANOMALY.value:
                cctx_label = CrossChainGraphLabel.ANOMALY_DESTINATION
            else:
                cctx_label = CrossChainGraphLabel.NORMAL

            print(f"Linking cctx_id {cctx_id}: {chain_a}:{tx_hash_a} → {chain_b}:{tx_hash_b}")

            cctx_graph_mapping = self.cctx_graph_repo.create({
                "cctx_id": str(cctx_id),
                "bridge": self.bridge_name,
                "source_chain": chain_a,
                "target_chain": chain_b,
                "source_tx_hash": tx_hash_a,
                "destination_tx_hash": tx_hash_b,
                "label": cctx_label.value,
            })

            # Backfill cctx_graph_id on both chain graph mappings
            cctx_gid = cctx_graph_mapping.cctx_graph_id
            self.chain_graph_repo.assign_cctx_id(src_mapping.graph_id, cctx_gid)
            self.chain_graph_repo.assign_cctx_id(dst_mapping.graph_id, cctx_gid)

            # Backfill cctx_graph_id + blockchain_type on all nodes and edges
            self.graph_nodes_repo.assign_cctx_id(
                src_mapping.graph_id, cctx_gid, blockchain_type=BlockchainType.SOURCE,
            )
            self.graph_nodes_repo.assign_cctx_id(
                dst_mapping.graph_id, cctx_gid, blockchain_type=BlockchainType.DESTINATION,
            )
            self.graph_edges_repo.assign_cctx_id(
                src_mapping.graph_id, cctx_gid, blockchain_type=BlockchainType.SOURCE,
            )
            self.graph_edges_repo.assign_cctx_id(
                dst_mapping.graph_id, cctx_gid, blockchain_type=BlockchainType.DESTINATION,
            )

            # Create a validator node (offchain relay) bridging the two router nodes
            src_router = self.graph_nodes_repo.get_router_node_by_graph_id(src_mapping.graph_id)
            dst_router = self.graph_nodes_repo.get_router_node_by_graph_id(dst_mapping.graph_id)
            if src_router is None or dst_router is None:
                print(f"  No router nodes found for cctx_id {cctx_id} - skipping validator node")
                continue

            validator_address = f"validator_{cctx_id}"
            if self.graph_nodes_repo.get_by_address(dst_mapping.graph_id, validator_address) is None:  # noqa: E501
                validator_node = self.graph_nodes_repo.create({
                    "node_type": GraphNodeType.VALIDATOR.value,
                    "chain_graph_id": dst_mapping.graph_id,
                    "cctx_graph_id": cctx_graph_mapping.cctx_graph_id,
                    "bridge": self.bridge_name,
                    "blockchain": None,
                    "blockchain_type": BlockchainType.OFFCHAIN.value,
                    "address": validator_address,
                    "attributes": {
                        "cctx_id": cctx_id,
                        "source_chain": chain_a,
                        "source_tx": tx_hash_a,
                        "source_timestamp": src_mapping.timestamp,
                        "target_chain": chain_b,
                        "destination_tx": tx_hash_b,
                        "destination_timestamp": dst_mapping.timestamp,
                    },
                    "attributes_text": (
                        f"type = validator; cctx_id = {cctx_id}; "
                        f"src_blockchain = {chain_a}; dst_blockchain = {chain_b}"
                    ),
                })

                self.graph_edges_repo.create({
                    "edge_type": GraphEdgeType.CROSS_CHAIN_RELATION.value,
                    "chain_graph_id": src_mapping.graph_id,
                    "cctx_graph_id": cctx_graph_mapping.cctx_graph_id,
                    "bridge": self.bridge_name,
                    "source_id": src_router.node_id,
                    "target_id": validator_node.node_id,
                    "deposit_id": str(cctx_id),
                    "blockchain_type": BlockchainType.OFFCHAIN.value,
                })
                self.graph_edges_repo.create({
                    "edge_type": GraphEdgeType.CROSS_CHAIN_RELATION.value,
                    "chain_graph_id": dst_mapping.graph_id,
                    "cctx_graph_id": cctx_graph_mapping.cctx_graph_id,
                    "bridge": self.bridge_name,
                    "source_id": validator_node.node_id,
                    "target_id": dst_router.node_id,
                    "deposit_id": str(cctx_id),
                    "blockchain_type": BlockchainType.OFFCHAIN.value,
                })


if __name__ == "__main__":
    yaml_file_path = sys.argv[1]
    script = AddNewAttacksScript()
    script.import_attacks_from_file(yaml_file_path)
