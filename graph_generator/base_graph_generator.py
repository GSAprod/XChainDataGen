import csv
import os
from abc import ABC, abstractmethod

from config.constants import (
    BLOCKCHAIN_IDS,
    TRACE_TRANSACTION_SUPPORTED_BLOCKCHAINS,
    Bridge,
)
from dune.dune_client import DuneClient
from graph_generator.graph_class import GraphObject
from graph_generator.graph_label import (
    BlockchainGraphLabel,
    BlockchainType,
    EventType,
    GraphEdgeType,
    CrossChainGraphLabel,
    GraphNodeType,
)
from graph_generator.pricing import TokenPricingService
from graph_generator.token_inspector import TokenInspector
from repository.common.models import BlockchainTransaction
from repository.common.repository import (
    BridgeRoutingContractMetadataRepository,
    TokenMetadataRepository,
    TokenPriceRepository,
)
from repository.database import DBSession
from repository.graphs.repository import (
    GraphEdgeRepository,
    GraphMappingBlockchainRepository,
    GraphMappingCrossChainRepository,
    GraphNodeRepository,
)
from rpcs.evm_rpc_client import EvmRPCClient
from utils.utils import CliColor, log_to_cli


class BaseGraphGenerator(ABC):
    def __init__(self, bridge: Bridge) -> None:
        self.bridge = bridge
        self.rpc_client = EvmRPCClient(bridge)
        self.bind_db_to_repos()
        self.chain_anomaly_transactions = {}
        self.offchain_anomaly_transactions = set()
        self.attacker_addresses = {}
        self.load_anomaly_data()

        try:
            self.dune_client = DuneClient(bridge)
            self.internal_tx_to_query_dune = []
        except Exception as e:
            log_to_cli(f"Failed to initialize Dune client: {e}. Dune-related functionalities will not work.", CliColor.ERROR)
            self.dune_client = None

        self.pricing = TokenPricingService(
            bridge,
            self.token_metadata_repo,
            self.token_price_repo,
            self.dune_client,
            self.fetch_transactions_timestamp_interval,
        )
        self.inspector = TokenInspector(self.rpc_client, self.token_metadata_repo)

    def bind_db_to_repos(self) -> None:
        self.bridge_router_metadata_repo = BridgeRoutingContractMetadataRepository(DBSession)
        self.token_metadata_repo = TokenMetadataRepository(DBSession)
        self.token_price_repo = TokenPriceRepository(DBSession)

        self.blockchain_graph_mapping_repo = GraphMappingBlockchainRepository(DBSession)
        self.cctx_graph_mapping_repo = GraphMappingCrossChainRepository(DBSession)
        self.graph_node_repo = GraphNodeRepository(DBSession)
        self.graph_edge_repo = GraphEdgeRepository(DBSession)

    def load_anomaly_data(self):
        """
        Loads anomaly data from CSV files for marking transactions and addresses in the graph.
        Expects three CSV files in the format:
        1. attacks.csv with columns: blockchain, tx_hash
        2. offchain_attacks.csv with columns: src_blockchain, src_tx_hash, dst_blockchain, dst_tx_hash
        3. attacker_addresses.csv with columns: blockchain, address
        """
        # Load on-chain anomalies
        anomaly_path = os.path.join(os.path.dirname(__file__), self.bridge.value, "anomaly_data/attacks.csv")
        chain_anomalies = {}
        with open(anomaly_path) as f:
            for row in csv.DictReader(f):
                blockchain = row.get("blockchain")
                tx_hash = row.get("tx_hash")
                if blockchain and tx_hash:
                    chain_anomalies.setdefault(blockchain, set()).add(tx_hash)
        self.chain_anomaly_transactions = chain_anomalies

        # Load off-chain anomalies
        offchain_anomaly_path = os.path.join(os.path.dirname(__file__), self.bridge.value, "anomaly_data/offchain_attacks.csv")
        offchain_anomalies = set()
        with open(offchain_anomaly_path) as f:
            for row in csv.DictReader(f):
                src_blockchain = row.get("src_blockchain")
                src_tx_hash = row.get("src_tx_hash")
                dst_blockchain = row.get("dst_blockchain")
                dst_tx_hash = row.get("dst_tx_hash")
                if src_tx_hash and dst_tx_hash:
                    offchain_anomalies.add((src_blockchain, src_tx_hash, dst_blockchain, dst_tx_hash))
        self.offchain_anomaly_transactions = offchain_anomalies

        # Load known attacker addresses
        attacker_address_path = os.path.join(os.path.dirname(__file__), self.bridge.value, "anomaly_data/attacker_addresses.csv")
        attackers = {}
        with open(attacker_address_path) as f:
            for row in csv.DictReader(f):
                blockchain = row.get("blockchain")
                address = row.get("address")
                if blockchain and address:
                    attackers.setdefault(blockchain, set()).add(address)
        self.attacker_addresses = attackers

    def generate_graph_data(self, blockchain: str, start_ts: int = None, end_ts: int = None) -> None:
        self.internal_tx_to_query_dune = []
        self.pricing.reset()

        for tx in self.fetch_transactions_for_blockchain(blockchain, start_ts, end_ts):
            self.process_partial_transaction(tx)

        if blockchain not in TRACE_TRANSACTION_SUPPORTED_BLOCKCHAINS and self.dune_client is not None:
            log_to_cli(f"Blockchain {blockchain} does not support transaction tracing. Will query Dune for native token transfers...")
            if self.internal_tx_to_query_dune:
                self.include_native_dune_transfers(blockchain)

        if self.dune_client is not None:
            self.pricing.batch_resolve_pending(self.graph_node_repo)

    def process_partial_transaction(self, tx: BlockchainTransaction):
        if self.blockchain_graph_mapping_repo.graph_exists(self.bridge.value, tx.blockchain, tx.transaction_hash) is not None:
            return

        # Create initial graph mapping and nodes for the transaction 
        # before processing events and traces, so that we have a graph context
        # to link events and internal transactions to, and to record missing price info if needed
        log_to_cli(f"Blockchain {tx.blockchain} - Processing transaction {tx.transaction_hash} for graph generation...")
        graph_obj = GraphObject(
            self.blockchain_graph_mapping_repo, self.graph_node_repo,
            self.graph_edge_repo, self.token_metadata_repo,
            resolve_address=self.resolve_node_address,
        )
        graph_obj.create_graph_mapping(
            self.bridge,
            tx.blockchain,
            tx.transaction_hash,
            tx.block_number,
            tx.timestamp,
            BlockchainGraphLabel.ANOMALY if tx.transaction_hash in self.chain_anomaly_transactions.get(tx.blockchain, set()) else BlockchainGraphLabel.NORMAL,
            self.attacker_addresses.get(tx.blockchain, set())
        )

        # First, check if there is a value transfer in the transaction itself
        op_index = 0
        if tx.value is not None and tx.value > 0:
            self.process_internal_token_transfer(graph_obj, tx.blockchain, 0, None, tx.from_address, tx.to_address, tx.value, tx.timestamp)
            op_index += 1

        # Then check for internal transactions (if supported) to capture token transfers that may not emit events.
        op_index = self._process_traces(graph_obj, tx, op_index)

        # Then process log events to capture token transfers and approvals, as well as router events. 
        # For tokens, also attempt to resolve price info and record any missing prices for later resolution.
        tx_receipt = self.rpc_client.get_transaction_receipt(tx.blockchain, tx.transaction_hash)
        for event in tx_receipt["logs"]:
            self._dispatch_log_event(graph_obj, tx, event, op_index)
            op_index += 1

    def _process_traces(self, graph_obj: GraphObject, tx: BlockchainTransaction, op_index: int) -> int:
        if tx.blockchain in TRACE_TRANSACTION_SUPPORTED_BLOCKCHAINS:
            internal_txs = self.rpc_client.get_transaction_trace(tx.blockchain, tx.transaction_hash)
            internal_inputs = set()
            for internal_tx in internal_txs:
                if (
                    internal_tx["type"] == "delegatecall"
                    and internal_tx["action"]["input"] in internal_inputs
                ): # Skip delegatecalls with duplicate input data to avoid processing the same token transfer multiple times
                    continue
                elif (
                    internal_tx["type"] == "call"
                    and internal_tx["action"]["callType"] in ["call", "callcode", "delegatecall"]
                    and internal_tx["action"]["value"] != "0x0"
                ): # Process internal transactions that transfer native tokens (value > 0)
                    from_address = internal_tx["action"]["from"]
                    to_address = internal_tx["action"]["to"]
                    value = int(internal_tx["action"]["value"], 16)
                    self.process_internal_token_transfer(graph_obj, tx.blockchain, op_index, internal_tx, from_address, to_address, value, tx.timestamp)
                    op_index += 1
                    internal_inputs.add(internal_tx["action"]["input"])
        else:
            # If a blockchain doesn't support transaction tracing, we will 
            # rely on Dune to provide information about native token transfers
            # that may not emit events.
            self.internal_tx_to_query_dune.append(tx.transaction_hash)
        return op_index

    def _dispatch_log_event(self, graph_obj: GraphObject, tx: BlockchainTransaction, event: dict, op_index: int):
        emitted_by = event["address"]
        blockchain = tx.blockchain

        # First check if the event was emitted by a known router contract, 
        # as this will determine how we parse the event and link it in the graph
        if self.bridge_router_metadata_repo.get_bridge_routing_metadata_by_address_and_blockchain(
            self.bridge.value, emitted_by.lower(), blockchain
        ):
            routing_node = graph_obj.fetch_or_create_node(
                emitted_by,
                node_type_if_missing=GraphNodeType.ROUTER.value,
                timestamp=tx.timestamp
            )
            graph_obj.update_node_type(routing_node.node_id, GraphNodeType.ROUTER.value)
            self.parse_bridge_router_event(tx, event, op_index, routing_node, graph_obj)
            return

        # If not a router event, check if it's a token event by trying to fetch
        # or create token metadata for the emitting address.
        token_metadata = self.inspector.ensure_metadata(emitted_by, blockchain)
        if token_metadata is not None:
            token_node = graph_obj.fetch_or_create_token_node(emitted_by, timestamp=tx.timestamp)
            self.parse_token_event(tx, event, op_index, token_node, graph_obj, token_metadata, op_index)
            return

        # Otherwise, we treat it as an unknown event and just create a log node for it 
        # without attempting to parse the event data, 
        # but still link it to the emitting address node in the graph for context.
        self._handle_unknown_event(graph_obj, tx, event, op_index)

    def _handle_unknown_event(self, graph_obj: GraphObject, tx: BlockchainTransaction, event: dict, op_index: int):
        address_node = graph_obj.fetch_or_create_node(event["address"], timestamp=tx.timestamp)
        log_event_node = graph_obj.create_log_node(
            op_index,
            event["topics"][0],
            EventType.UNKNOWN.value,
            None,
            event["topics"][1:],
            event["data"],
            tx.timestamp,
        )
        graph_obj.create_edge(address_node.node_id, log_event_node.node_id, GraphEdgeType.LOG_RELATION.value, op_index)

    def process_internal_token_transfer(self, graph_obj: GraphObject, blockchain, op_index, internal_tx, from_address, to_address, value, timestamp):
        # Cap value to avoid overflow issues in the graph dataset
        if value is not None and value > 10e27:
            value = 10e27

        # Create a token transfer edge for the internal transaction,
        # linking the sender and recipient addresses for the transfer.
        from_node = graph_obj.fetch_or_create_node(from_address, timestamp=timestamp)
        to_node = graph_obj.fetch_or_create_node(to_address, timestamp=timestamp)
        graph_obj.create_edge(from_node.node_id, to_node.node_id, GraphEdgeType.TOKEN_TRANSFER.value, op_index, attributes={
            "currency": "native",
            "amount": value
        })

        # For native token transfers, we won't have an ERC-20 Transfer event to capture the token metadata and price info, 
        # so we attempt to resolve the price here and create a log node for the transfer 
        # directly from the internal transaction data.
        blockchain_config = next((chain for chain in BLOCKCHAIN_IDS.values() if chain["name"] == blockchain), None)
        native_token_address = blockchain_config["native_token_contract"] if blockchain_config and "native_token_contract" in blockchain_config else "token_native"
        if blockchain == "ronin":
            native_token_symbol = "RON"
        else:
            native_token_symbol = blockchain_config["native_token"] if blockchain_config else "ETH"
            if native_token_symbol.startswith("W"):
                native_token_symbol = native_token_symbol[1:]

        # Create the native token node (if it doesn't already exist)
        native_token_node = graph_obj.fetch_or_create_node(
            native_token_address,
            node_type_if_missing=GraphNodeType.TOKEN.value,
            attributes={
                "symbol": native_token_symbol,
                "name": f"{native_token_symbol} Native Currency",
                "decimals": 18
            },
            timestamp=timestamp
        )

        # Resolve price for the native token transfer and 
        # create a log node for the transfer, linking it to the native token node for context.
        amount, amount_usd = self.pricing.resolve_native_amount(blockchain, value, timestamp)
        log_event_node = graph_obj.create_log_node(
            op_index,
            f"{from_address}_{to_address}",
            EventType.TRANSFER.value,
            "Transfer(address from, address to, uint256 value)",
            {"from": from_address, "to": to_address, "value": value},
            None,
            amount=value,
            amount_usd=amount_usd,
            token_symbol=native_token_symbol,
            timestamp=timestamp
        )
        if amount_usd is None:
            self.pricing.record_missing_price(log_event_node.node_id, native_token_symbol, timestamp, amount)
        graph_obj.create_edge(native_token_node.node_id, log_event_node.node_id, GraphEdgeType.LOG_RELATION.value, op_index)

    def create_unknown_router_event_node(self, tx, event, event_index, routing_node, graph_obj: GraphObject):
        """
        For events emitted by the router contract that we don't have specific parsing logic for,
        we still want to capture them in the graph as unknown events linked to the router node,
        as they may provide important context for the transaction and could be relevant for anomaly detection.
        """
        log_event_node = graph_obj.create_log_node(
            event_index,
            event["topics"][0],
            EventType.ROUTER_UNKNOWN.value,
            None,
            event["topics"][1:],
            event["data"],
            tx.timestamp
        )
        graph_obj.create_edge(routing_node.node_id, log_event_node.node_id, GraphEdgeType.LOG_RELATION.value, event_index)

    def parse_token_event(self, tx, event, event_index, token_node, graph_obj: GraphObject, token_metadata, op_index):
        contract = self.inspector.load_erc20_contract(token_node.address)

        # Check if the event is a Transfer or Approval event
        # and extract the relevant information to create edges and log nodes
        from_address, to_address, value, type = None, None, None, None
        if event["topics"][0] == "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef":  # Transfer
            event_signature = "event Transfer(address _from, address _to, uint256 _value)"
            event_args = contract.events.Transfer().process_log(event)["args"]
            from_address = event_args["from"]
            to_address = event_args["to"]
            value = event_args["value"]
            type = GraphEdgeType.TOKEN_TRANSFER.value
        elif event["topics"][0] == "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925":  # Approval
            event_signature = "event Approval(address _owner, address _spender, uint256 _value)"
            event_args = contract.events.Approval().process_log(event)["args"]
            from_address = event_args["owner"]
            to_address = event_args["spender"]
            value = event_args["value"]
            type = GraphEdgeType.TOKEN_AUTH.value
        else:
            # If it's an event from a token contract but not a Transfer or Approval,
            # we still want to capture it in the graph as an unknown token event,
            # as it may provide important context about interactions with the token contract.
            log_event_node = graph_obj.create_log_node(
                event_index,
                event["topics"][0],
                EventType.TOKEN_UNKNOWN.value,
                None,
                event["topics"],
                event["data"],
                timestamp=tx.timestamp,
            )
            graph_obj.create_edge(token_node.node_id, log_event_node.node_id, GraphEdgeType.LOG_RELATION.value, op_index)
            return

        # Cap value to avoid overflow issues in the graph dataset
        if value is not None and value > 10e27:
            value = 10e27

        # Create edges for the token transfer or approval, linking the sender to the recipient address
        from_node = graph_obj.fetch_or_create_node(from_address, timestamp=tx.timestamp)
        to_node = graph_obj.fetch_or_create_node(to_address, timestamp=tx.timestamp)
        if from_node.address == to_node.address:
            # If the sender and recipient are the same, the transfer/approval
            # may not be meaningful for our analysis and could be noise in the graph,
            # hence we skip creating an edge and log event for it.
            return

        amount, amount_usd = self.pricing.resolve_token_amount(token_metadata, value, tx.timestamp)
        graph_obj.create_edge(from_node.node_id, to_node.node_id, type, op_index)

        # Create a log node for the token event, linking it to the token node for context
        log_event_node = graph_obj.create_log_node(
            event_index,
            event["topics"][0],
            EventType.TRANSFER.value if type == GraphEdgeType.TOKEN_TRANSFER.value else EventType.APPROVAL.value,
            event_signature,
            event_args,
            event["data"],
            amount=value,
            amount_usd=amount_usd,
            token_symbol=token_metadata.symbol,
            timestamp=tx.timestamp,
        )
        if amount_usd is None:
            # Record missing price info for later resolution
            self.pricing.record_missing_price(log_event_node.node_id, token_metadata.symbol, tx.timestamp, amount)
        graph_obj.create_edge(token_node.node_id, log_event_node.node_id, GraphEdgeType.LOG_RELATION.value, op_index)

    def link_transactions_into_cctxs(self):
        cctx_data = self.fetch_cross_chain_transactions()

        for cctx in cctx_data:
            if self.cctx_graph_mapping_repo.get_by_chain_tx_hash(self.bridge.value, cctx.src_blockchain, cctx.src_transaction_hash):
                continue
            elif self.cctx_graph_mapping_repo.get_by_chain_tx_hash(self.bridge.value, cctx.dst_blockchain, cctx.dst_transaction_hash):
                continue

            source_graph_mapping = self.blockchain_graph_mapping_repo.graph_exists(self.bridge.value, cctx.src_blockchain, cctx.src_transaction_hash)
            destination_graph_mapping = self.blockchain_graph_mapping_repo.graph_exists(self.bridge.value, cctx.dst_blockchain, cctx.dst_transaction_hash)

            if source_graph_mapping is None or destination_graph_mapping is None:
                log_to_cli(f"Could not find graph mappings for CCTX src={cctx.src_transaction_hash}@{cctx.src_blockchain} dst={cctx.dst_transaction_hash}@{cctx.dst_blockchain}. Skipping...", CliColor.ERROR)
                continue

            if source_graph_mapping.label == BlockchainGraphLabel.ANOMALY.value:
                cctx_label = CrossChainGraphLabel.ANOMALY_SOURCE
            elif destination_graph_mapping.label == BlockchainGraphLabel.ANOMALY.value:
                cctx_label = CrossChainGraphLabel.ANOMALY_DESTINATION
            else:
                cctx_label = self.check_offchain_label(cctx)

            log_to_cli(f"Linking CCTX with source {cctx.src_blockchain}:{cctx.src_transaction_hash} and destination {cctx.dst_blockchain}:{cctx.dst_transaction_hash}")
            cctx_id = self.fetch_cctx_id(cctx)
            cctx_graph_mapping = self.cctx_graph_mapping_repo.create({
                "cctx_id": cctx_id,
                "bridge": self.bridge.value,
                "source_chain": cctx.src_blockchain,
                "target_chain": cctx.dst_blockchain,
                "source_tx_hash": cctx.src_transaction_hash,
                "destination_tx_hash": cctx.dst_transaction_hash,
                "label": cctx_label.value
            })
            self.blockchain_graph_mapping_repo.assign_cctx_id(source_graph_mapping.graph_id, cctx_graph_mapping.cctx_graph_id)
            self.blockchain_graph_mapping_repo.assign_cctx_id(destination_graph_mapping.graph_id, cctx_graph_mapping.cctx_graph_id)

            self.graph_node_repo.assign_cctx_id(source_graph_mapping.graph_id, cctx_graph_mapping.cctx_graph_id, blockchain_type=BlockchainType.SOURCE)
            self.graph_node_repo.assign_cctx_id(destination_graph_mapping.graph_id, cctx_graph_mapping.cctx_graph_id, blockchain_type=BlockchainType.DESTINATION)

            self.graph_edge_repo.assign_cctx_id(source_graph_mapping.graph_id, cctx_graph_mapping.cctx_graph_id, blockchain_type=BlockchainType.SOURCE)
            self.graph_edge_repo.assign_cctx_id(destination_graph_mapping.graph_id, cctx_graph_mapping.cctx_graph_id, blockchain_type=BlockchainType.DESTINATION)

            src_router_node = self.graph_node_repo.get_router_node_by_graph_id(source_graph_mapping.graph_id)
            dst_router_node = self.graph_node_repo.get_router_node_by_graph_id(destination_graph_mapping.graph_id)
            if src_router_node is not None and dst_router_node is not None:
                if self.graph_node_repo.get_by_address(destination_graph_mapping.graph_id, f"validator_{cctx_id}") is None:
                    validator_node = self.graph_node_repo.create({
                        "node_type": GraphNodeType.VALIDATOR.value,
                        "chain_graph_id": destination_graph_mapping.graph_id,
                        "cctx_graph_id": cctx_graph_mapping.cctx_graph_id,
                        "bridge": self.bridge.value,
                        "blockchain": None,
                        "blockchain_type": BlockchainType.OFFCHAIN.value,
                        "address": f"validator_{cctx_id}",
                        "attributes": {
                            "cctx_id": cctx_id,
                            "source_chain": cctx.src_blockchain,
                            "source_tx": cctx.src_transaction_hash,
                            "source_timestamp": source_graph_mapping.timestamp,
                            "target_chain": cctx.dst_blockchain,
                            "destination_tx": cctx.dst_transaction_hash,
                            "destination_timestamp": destination_graph_mapping.timestamp
                        },
                        "attributes_text": f"type = validator; cctx_id = {cctx_id}; src_blockchain = {cctx.src_blockchain}; dst_blockchain = {cctx.dst_blockchain}",
                    })

                    self.graph_edge_repo.create({
                        "edge_type": GraphEdgeType.CROSS_CHAIN_RELATION.value,
                        "chain_graph_id": source_graph_mapping.graph_id,
                        "cctx_graph_id": cctx_graph_mapping.cctx_graph_id,
                        "bridge": self.bridge.value,
                        "source_id": src_router_node.node_id,
                        "target_id": validator_node.node_id,
                        "deposit_id": cctx_id,
                        "blockchain_type": BlockchainType.OFFCHAIN.value
                    })
                    self.graph_edge_repo.create({
                        "edge_type": GraphEdgeType.CROSS_CHAIN_RELATION.value,
                        "chain_graph_id": destination_graph_mapping.graph_id,
                        "cctx_graph_id": cctx_graph_mapping.cctx_graph_id,
                        "bridge": self.bridge.value,
                        "source_id": validator_node.node_id,
                        "target_id": dst_router_node.node_id,
                        "deposit_id": cctx_id,
                        "blockchain_type": BlockchainType.OFFCHAIN.value
                    })
            else:
                log_to_cli(f"Could not find router nodes for source graph {source_graph_mapping.graph_id} and destination graph {destination_graph_mapping.graph_id}. Skipping validation node for CCTX {cctx_graph_mapping.cctx_graph_id}...", CliColor.ERROR)

        log_to_cli("Finished linking transactions into CCTXs. Refreshing node degrees in the repository...")
        self.graph_node_repo.refresh_degrees()

    def check_offchain_label(self, cctx):
        if (cctx.src_blockchain, cctx.src_transaction_hash, cctx.dst_blockchain, cctx.dst_transaction_hash) in self.offchain_anomaly_transactions:
            return CrossChainGraphLabel.ANOMALY_OFFCHAIN
        return CrossChainGraphLabel.NORMAL

    def include_native_dune_transfers(self, blockchain):
        tx_hashes = self.internal_tx_to_query_dune
        if not tx_hashes:
            return

        log_to_cli(f"Querying Dune for native token transfers related to {len(tx_hashes)} transaction hashes on {blockchain}...")
        try:
            min_ts, max_ts = self.fetch_transactions_timestamp_interval()
            dune_results = self.dune_client.fetch_native_transactions(blockchain, tx_hashes, min_ts, max_ts)
            op_idx_counters = {}

            for transfer in reversed(dune_results["rows"]):
                tx_hash = transfer["tx_hash"]
                graph_obj = GraphObject(
                    self.blockchain_graph_mapping_repo,
                    self.graph_node_repo,
                    self.graph_edge_repo,
                    self.token_metadata_repo
                ).load_from_db(self.bridge, blockchain, tx_hash)
                from_address = transfer["tx_from"]
                to_address = transfer["tx_to"]
                value = int(transfer["amount_raw"])

                if value is not None and value > 10e27:
                    value = 10e27

                log_to_cli(f"Including native token transfer from Dune for {tx_hash} on {blockchain}: {from_address} → {to_address} amount {value}")
                op_idx_counters[tx_hash] = op_idx_counters.get(tx_hash, 0) - 1
                self.process_internal_token_transfer(graph_obj, blockchain, op_idx_counters[tx_hash], transfer, from_address, to_address, value, graph_obj.tx_timestamp)

        except Exception as e:
            log_to_cli(f"Error fetching native token transfers from Dune for blockchain {blockchain}: {e}", CliColor.ERROR)


    #* Note: This method can be overridden in child classes to implement 
    #* specific address resolution logic if needed (e.g., to handle cases 
    #* where multiple addresses should be treated as the same entity in the graph).
    def resolve_node_address(self, address: str, _blockchain: str) -> str:
        return address
    
    @abstractmethod
    def fetch_transactions_for_blockchain(self, blockchain: str, start_ts: int = None, end_ts: int = None):
        pass

    @abstractmethod
    def parse_bridge_router_event(self, tx, event, event_index: int, routing_node, graph_obj: GraphObject):
        pass
    
    @abstractmethod
    def fetch_cross_chain_transactions(self):
        pass

    @abstractmethod
    def get_router_event_list(self, blockchain):
        pass

    @abstractmethod
    def fetch_cctx_id(self, cctx) -> str:
        pass

    @abstractmethod
    def fetch_transactions_timestamp_interval(self):
        pass
