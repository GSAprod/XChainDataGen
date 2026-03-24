import json
import os
from abc import ABC, abstractmethod

from eth_abi import decode as abi_decode
from web3 import Web3

from config.constants import Bridge
from graph_generator.graph_class import GraphObject
from graph_generator.graph_label import GraphLabel
from repository.common.models import BlockchainTransaction
from repository.common.repository import (
    BridgeRoutingContractMetadataRepository,
    TokenMetadataRepository,
)
from repository.database import DBSession
from repository.graphs.models import GraphEdgeType, GraphNodeType
from repository.graphs.repository import (
    BlockchainGraphMappingRepository,
    CrossChainGraphMappingRepository,
    GraphEdgeRepository,
    GraphNodeRepository,
)
from rpcs.evm_rpc_client import EvmRPCClient
from utils.utils import CliColor, log_to_cli


class BaseGraphGenerator(ABC):
    def __init__(self, bridge: Bridge) -> None:
        self.bridge = bridge
        self.rpc_client = EvmRPCClient(bridge)
        self.bind_db_to_repos()
        self.unknown_contracts = set()

    def bind_db_to_repos(self) -> None:
        self.bridge_router_metadata_repo = BridgeRoutingContractMetadataRepository(DBSession)
        self.token_metadata_repo = TokenMetadataRepository(DBSession)

        self.blockchain_graph_mapping_repo = BlockchainGraphMappingRepository(DBSession)
        self.cctx_graph_mapping_repo = CrossChainGraphMappingRepository(DBSession)
        self.graph_node_repo = GraphNodeRepository(DBSession)
        self.graph_edge_repo = GraphEdgeRepository(DBSession)

    
    def generate_graph_data(self, blockchain: str) -> None:
        func_name = "generate_graph_data"
        
        # Create a graph per single-ledger transaction
        txs = self.fetch_transactions_for_blockchain(blockchain)
        for tx in txs:
            self.process_partial_transaction(tx)

    def process_partial_transaction(self, tx: BlockchainTransaction):
        if self.blockchain_graph_mapping_repo.graph_exists(self.bridge.value, tx.blockchain, tx.transaction_hash) is not None:
            return
        
        log_to_cli(
            f"Blockchain {tx.blockchain} - Processing transaction {tx.transaction_hash} for graph generation..."
        )
        graph_obj = GraphObject(self.blockchain_graph_mapping_repo, self.graph_node_repo, self.graph_edge_repo)
        graph_mapping = graph_obj.create_graph_mapping(
            self.bridge, 
            tx.blockchain, 
            tx.transaction_hash, 
            tx.block_number, 
            GraphLabel.NORMAL
        )

        blockchain = tx.blockchain
        tx_hash = tx.transaction_hash
        tx_receipt = self.rpc_client.get_transaction_receipt(blockchain, tx_hash)

        for event in tx_receipt["logs"]:
            emitted_by = event["address"] 

            if self.bridge_router_metadata_repo.get_bridge_routing_metadata_by_address_and_blockchain(emitted_by.lower(), blockchain):
                # If the event is emitted by a known bridge router, we can 
                # create a router node and include additional relations based on the function calls and events
                routing_node = graph_obj.fetch_or_create_node(
                    emitted_by,
                    node_type_if_missing=GraphNodeType.ROUTER.value,
                    # we can also include the function signatures as attributes.
                    # we won't include them for now for space reasons
                )
                graph_obj.update_node_type(routing_node.node_id, GraphNodeType.ROUTER.value)
                self.parse_bridge_router_event(event, routing_node, graph_obj)
                continue

            # Check if the address is a known token contract
            token_metadata = self.token_metadata_repo.get_token_metadata_by_contract_and_blockchain(
                emitted_by, blockchain
            )

            # If no token info exists, check if the address is an ERC20 contract
            # and try to fetch its metadata, if it's the case
            if token_metadata is None and emitted_by not in self.unknown_contracts:
                log_to_cli(
                    f"Blockchain {blockchain} - Address {emitted_by} not found in token metadata repository. Checking if it's an ERC20 contract..."
                )
                if self.check_if_contract_erc20(emitted_by, blockchain):
                    token_metadata = self.token_metadata_repo.get_token_metadata_by_contract_and_blockchain(emitted_by, blockchain)
                else:
                    self.unknown_contracts.add(emitted_by)

            # If the event is emitted by a known token contract, we can create a token node 
            # and parse the event to include additional relations to the graph
            if token_metadata is not None:
                token_node = graph_obj.fetch_or_create_node(
                    emitted_by,
                    attributes={
                        "symbol": token_metadata.symbol,
                        "name": token_metadata.name
                    },
                    node_type_if_missing=GraphNodeType.TOKEN.value
                )
                self.parse_token_event(event, token_node, graph_obj)
                continue

            # For other events, we can create a log event node and link it to the respective address node
            address_node = graph_obj.fetch_or_create_node(emitted_by)
            log_event_node = graph_obj.create_log_node(
                event["topics"][0],
                None,
                event
            )
            graph_obj.create_edge(address_node.node_id, log_event_node.node_id, GraphEdgeType.LOG_RELATION.value)

    def load_erc20_contract(self, address):
        checksum_address = Web3.to_checksum_address(address)
        token_abi_path = os.path.normpath(os.path.join(os.path.dirname(__file__), "ABI", "erc20_abi.json"))
        with open(token_abi_path, "r") as abi_file:
            abi = json.load(abi_file)
        return Web3().eth.contract(address=checksum_address, abi=abi)
    
    def check_if_contract_erc20(self, contract_address: str, blockchain: str) -> bool:
        function_signatures = [
            { "signature": "0x06fdde03", "name": "name", "result": None, "resultType": "string" }, # name()
            { "signature": "0x95d89b41", "name": "symbol", "result": None, "resultType": "string" }, # symbol()
            { "signature": "0x313ce567", "name": "decimals", "result": None, "resultType": "uint8" }, # decimals()
            { "signature": "0x18160ddd", "name": "totalSupply", "result": None, "resultType": "uint256" }, # totalSupply()
        ]
        
        for func in function_signatures:
            try:
                res = self.rpc_client.function_call(blockchain, contract_address, func["signature"], no_backoff=True)
                if res is None or res == "0x0":
                    return False
                
                if func["resultType"] == "string":
                    func["result"] = abi_decode(["string"], bytes.fromhex(res[2:]))
                elif func["resultType"] == "uint8" or func["resultType"] == "uint256":
                    func["result"] = int(res, 16)
                else:
                    func["result"] = res
            except Exception as e:
                # If any of the function calls fail, we can assume it's not an ERC20 contract
                log_to_cli(f"Blockchain {blockchain} - [WARNING] Error calling function {func['name']} on contract {contract_address}: {e}", CliColor.ERROR)
                return False

        # Save the token metadata to the repository if it doesn't exist
        log_to_cli(
            f"Added newly discovered ERC20 token contract to the repository: {contract_address} with name {function_signatures[0]['result']} and symbol {function_signatures[1]['result']}"
        )
        if self.token_metadata_repo.get_token_metadata_by_contract_and_blockchain(contract_address, blockchain) is None:
            self.token_metadata_repo.create(
                {
                    "symbol": function_signatures[1]["result"],
                    "name": function_signatures[0]["result"],
                    "decimals": function_signatures[2]["result"],
                    "blockchain": blockchain,
                    "address": contract_address
                }
            )
        return True

    def parse_token_event(self, event, token_node, graph_obj: GraphObject):
        contract = self.load_erc20_contract(token_node.address)
        
        # Parsing logic for ERC20 Token events
        from_address, to_address, value, type = None, None, None, None
        if event["topics"][0] == "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef": # Transfer
            event_signature = "event Transfer(address indexed _from, address indexed _to, uint256 _value)"
            event_args = contract.events.Transfer().process_log(event)["args"]
            from_address = event_args["from"]
            to_address = event_args["to"]
            value = event_args["value"]
            type = GraphEdgeType.TOKEN_TRANSFER.value
        elif event["topics"][0] == "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925": # Approval
            event_signature = "event Approval(address indexed _owner, address indexed _spender, uint256 _value)"
            event_args = contract.events.Approval().process_log(event)["args"]
            from_address = event_args["owner"]
            to_address = event_args["spender"]
            value = event_args["value"]
            type = GraphEdgeType.TOKEN_AUTH.value
        else:
            # For other events, we can create a log event node and link it to the token node
            event_signature = None
            event_args = None
            log_event_node = graph_obj.create_log_node(
                event["topics"][0],
                None,
                event
            )
            graph_obj.create_edge(token_node.node_id, log_event_node.node_id, GraphEdgeType.LOG_RELATION.value)
            return
        
        from_node = graph_obj.fetch_or_create_node(from_address)
        to_node = graph_obj.fetch_or_create_node(to_address)
        graph_obj.create_edge(from_node.node_id, to_node.node_id, type, attributes={"amount": value})

        # Create and link log event node to the token node
        log_event_node = graph_obj.create_log_node(
            event["topics"][0],
            event_signature,
            event_args
        )
        graph_obj.create_edge(token_node.node_id, log_event_node.node_id, GraphEdgeType.LOG_RELATION.value)

    @abstractmethod
    def fetch_transactions_for_blockchain(self, blockchain: str):
        pass

    @abstractmethod
    def parse_bridge_router_event(self, event, routing_node, graph_obj: GraphObject):
        pass