import json
import os
from abc import ABC, abstractmethod
from datetime import datetime

from eth_abi import decode as abi_decode
from web3 import Web3

from config.constants import (
    BLOCKCHAIN_IDS,
    TOKEN_PRICING_SUPPORTED_BLOCKCHAINS,
    TRACE_TRANSACTION_SUPPORTED_BLOCKCHAINS,
    Bridge,
)
from dune.dune_client import DuneClient
from generator.base_generator import PriceGenerator
from graph_generator.graph_class import GraphObject
from graph_generator.graph_label import BlockchainType, GraphEdgeType, GraphLabel, GraphNodeType
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
        self.unknown_contracts = set()
        self.unknown_contract_prices = set()

        try:
            self.dune_client = DuneClient(bridge)
            self.tx_to_query_dune = []
        except Exception as e:
            log_to_cli(f"Failed to initialize Dune client: {e}. Dune-related functionalities will not work.", CliColor.ERROR)
            self.dune_client = None

    def bind_db_to_repos(self) -> None:
        self.bridge_router_metadata_repo = BridgeRoutingContractMetadataRepository(DBSession)
        self.token_metadata_repo = TokenMetadataRepository(DBSession)
        self.token_price_repo = TokenPriceRepository(DBSession)

        self.blockchain_graph_mapping_repo = GraphMappingBlockchainRepository(DBSession)
        self.cctx_graph_mapping_repo = GraphMappingCrossChainRepository(DBSession)
        self.graph_node_repo = GraphNodeRepository(DBSession)
        self.graph_edge_repo = GraphEdgeRepository(DBSession)

    
    def generate_graph_data(self, blockchain: str) -> None:
        func_name = "generate_graph_data"
        
        # Create a graph per single-ledger transaction
        txs = self.fetch_transactions_for_blockchain(blockchain)
        for tx in txs:
            self.process_partial_transaction(tx)

        if blockchain not in TRACE_TRANSACTION_SUPPORTED_BLOCKCHAINS and self.dune_client is not None:
            log_to_cli(f"Blockchain {blockchain} does not support transaction tracing. Will query Dune for native token transfers related to the transactions to include in the graphs...")
            #self.tx_to_query_dune.extend([tx.transaction_hash for tx in txs]) #! TESTING ONLY, REMOVE THIS
            if len(self.tx_to_query_dune) > 0:
                self.include_native_dune_transfers(blockchain)

    def process_partial_transaction(self, tx: BlockchainTransaction):
        if self.blockchain_graph_mapping_repo.graph_exists(self.bridge.value, tx.blockchain, tx.transaction_hash) is not None:
            return

        log_to_cli(
            f"Blockchain {tx.blockchain} - Processing transaction {tx.transaction_hash} for graph generation..."
        )
        graph_obj = GraphObject(self.blockchain_graph_mapping_repo, self.graph_node_repo, self.graph_edge_repo, self.token_metadata_repo)
        graph_obj.create_graph_mapping(
            self.bridge, 
            tx.blockchain, 
            tx.transaction_hash, 
            tx.block_number,
            tx.timestamp,
            GraphLabel.NORMAL
        )

        blockchain = tx.blockchain
        tx_hash = tx.transaction_hash
        tx_receipt = self.rpc_client.get_transaction_receipt(blockchain, tx_hash)

        op_index = 0

        # Check for internal transactions first, that move value between addresses
        # and include them in the graph as well (if blockchain supports it)
        if blockchain in TRACE_TRANSACTION_SUPPORTED_BLOCKCHAINS:
            internal_txs = self.rpc_client.get_transaction_trace(blockchain, tx_hash)
            internal_inputs = set()     # to avoid processing the same delegatecall
            for internal_tx in internal_txs:
                if (
                    internal_tx["type"] == "delegatecall"
                    and internal_tx["action"]["input"] in internal_inputs
                ):
                    # If the delegatecall input is the same as a previous one, we can assume it's part of the same execution flow and skip it to avoid redundancy in the graph
                    continue
                elif (
                    internal_tx["type"] == "call" 
                    and internal_tx["action"]["callType"] in ["call", "callcode", "delegatecall"] 
                    and internal_tx["action"]["value"] != '0x0'
                ):
                    # Add an edge for the value transfer between the from and to addresses
                    from_address = internal_tx["action"]["from"]
                    to_address = internal_tx["action"]["to"]
                    value = int(internal_tx["action"]["value"], 16)

                    self.process_internal_token_transfer(graph_obj, blockchain, op_index, internal_tx, from_address, to_address, value, tx.timestamp)
                    # Add the delegatecall input to the set to avoid re-processing
                    op_index += 1
                    internal_inputs.add(internal_tx["action"]["input"])
        else:
            # If the blockchain doesn't support transaction tracing, 
            # The transaction will be queried with Dune later 
            # for native token transfers related to the transaction
            self.tx_to_query_dune.append(tx_hash)

        for event in tx_receipt["logs"]:
            emitted_by = event["address"]

            if self.bridge_router_metadata_repo.get_bridge_routing_metadata_by_address_and_blockchain(emitted_by.lower(), blockchain):
                # If the event is emitted by a known bridge router, we can 
                # create a router node and include additional relations based on the function calls and events
                routing_node = graph_obj.fetch_or_create_node(
                    emitted_by,
                    node_type_if_missing=GraphNodeType.ROUTER.value,
                    attributes_text=f"type = router; blockchain = {blockchain}; bridge = {self.bridge.value}",
                    timestamp=tx.timestamp
                    # we can also include the function signatures as attributes.
                    # we won't include them for now for space reasons
                )
                graph_obj.update_node_type(routing_node.node_id, GraphNodeType.ROUTER.value)
                self.parse_bridge_router_event(tx, event, op_index, routing_node, graph_obj)
                op_index += 1
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
                token_node = graph_obj.fetch_or_create_token_node(
                    emitted_by,
                    timestamp=tx.timestamp
                )
                self.parse_token_event(tx, event, op_index, token_node, graph_obj, token_metadata, op_index)
                op_index += 1
                continue

            # For other events, we can create a log event node and link it to the respective address node
            address_node = graph_obj.fetch_or_create_node(emitted_by, timestamp=tx.timestamp)
            log_event_node = graph_obj.create_log_node(
                op_index,
                event["topics"][0],
                None,
                event,
                tx.timestamp,
                attributes_text=f"""event UnknownEvent
blockchain = {blockchain}
address = {event["address"][:6]}...{event["address"][-4:]}
topic = {event["topics"][0][:6]}...{event["topics"][0][-4:]}
number_of_args = {len(event["topics"]) - 1}
data_size = {len(event["data"]) // 32}
"""
            )
            graph_obj.create_edge(address_node.node_id, log_event_node.node_id, GraphEdgeType.LOG_RELATION.value, op_index)
            op_index += 1

    def process_internal_token_transfer(self, graph_obj, blockchain, op_index, internal_tx, from_address, to_address, value, timestamp):
        from_node = graph_obj.fetch_or_create_node(from_address, timestamp=timestamp)
        to_node = graph_obj.fetch_or_create_node(to_address, timestamp=timestamp)
        graph_obj.create_edge(from_node.node_id, to_node.node_id, GraphEdgeType.TOKEN_TRANSFER.value, op_index, attributes={
            "currency": "native",
            "amount": value
        })

        # We can also create a log event node for the internal transaction and link it 
        # to the native token node
        native_token_node = graph_obj.fetch_or_create_node(
            "token_native",
            node_type_if_missing=GraphNodeType.TOKEN.value,
            attributes={
                "symbol": "ETH Native Currency",
                "blockchain": blockchain,
            },
            attributes_text=f"type = token; blockchain = {blockchain}; symbol = ETH Native Currency",
            timestamp=timestamp
        )

        amount, amount_usd = self.convert_native_value_to_amount(blockchain, timestamp, value)
        description = f"""event Transfer(address from, address to, uint256 value)
token = ETH Native Currency at token_native
from = {from_node.node_type} ({from_node.address[:6]}...{from_node.address[-4:]})
to = {to_node.node_type} ({to_node.address[:6]}...{to_node.address[-4:]})
value = {amount} ETH
blockchain = {blockchain}
"""  # Needs to change symbol based on the blockchain
                        
        log_event_node = graph_obj.create_log_node(
            op_index,
            f"{from_address}_{to_address}",
            f"Transfer(address from, address to, uint256 value)",
            internal_tx,
            attributes_text=description,
            amount=value,
            amount_usd=amount_usd,
            timestamp=timestamp
        )
        graph_obj.create_edge(native_token_node.node_id, log_event_node.node_id, GraphEdgeType.LOG_RELATION.value, op_index)

    def load_erc20_contract(self, address):
        checksum_address = Web3.to_checksum_address(address)
        token_abi_path = os.path.normpath(os.path.join(os.path.dirname(__file__), "ABI", "erc20_abi.json"))
        with open(token_abi_path, "r") as abi_file:
            abi = json.load(abi_file)
        return Web3().eth.contract(address=checksum_address, abi=abi)
    
    def load_token_metadata(self, contract_address: str, blockchain: str):
        metadata = self.token_metadata_repo.get_token_metadata_by_contract_and_blockchain(contract_address, blockchain)
        if metadata is None:
            res = self.check_if_contract_erc20(contract_address, blockchain)
            if res:
                metadata = self.token_metadata_repo.get_token_metadata_by_contract_and_blockchain(contract_address, blockchain)
            else:
                return None
        return metadata

    def convert_token_value_to_amount(self, timestamp: int, token_metadata, raw_value: int):
        amount = float(raw_value) / (10 ** token_metadata.decimals)
        date = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d")
        token_price = self.token_price_repo.get_token_price_by_symbol_and_date(token_metadata.symbol, date)
        
        if token_price is None and token_metadata.address not in self.unknown_contract_prices:
            log_to_cli(
                f"Price for token {token_metadata.symbol} on {date} not found. Fetching price using PriceGenerator..."
            )
            min_ts, max_ts = self.fetch_transactions_timestamp_interval()
            try:
                # Initially fetch the token price using the Ethereum chain as a reference (for price consistency between chains)
                PriceGenerator.fetch_and_store_token_prices(self.bridge, self.token_price_repo, min_ts, max_ts, token_metadata.name, symbol=token_metadata.symbol, blockchain="ethereum")
                token_price = self.token_price_repo.get_token_price_by_symbol_and_date(token_metadata.symbol, date)
            except Exception as _:
                # If this does not work, try fetching the price using the current chain. This is a fallback mechanism.
                try:
                    log_to_cli(f"Failed to fetch price for token {token_metadata.symbol} on ethereum. Trying in {token_metadata.blockchain}...")
                    PriceGenerator.fetch_and_store_token_prices(self.bridge, self.token_price_repo, min_ts, max_ts, token_metadata.name, symbol=token_metadata.symbol, blockchain=token_metadata.blockchain, token_address=token_metadata.address)
                    token_price = self.token_price_repo.get_token_price_by_symbol_and_date(token_metadata.symbol, date)
                except Exception as e2:
                    log_to_cli(f"Failed to fetch price for token {token_metadata.symbol} through PriceGenerator: {e2}", CliColor.ERROR)
                    self.unknown_contract_prices.add(token_metadata.address)
                    return amount, None
            
        amount_usd = int(int(raw_value) * token_price.price_usd * (10 ** (18 - token_metadata.decimals))) if token_price else None
        return amount, amount_usd
    
    def convert_native_value_to_amount(self, blockchain: str, timestamp: int, raw_value: int):
        # By norm, all EVM chains need to have native token decimals set to 18 for compatibility with Solidity
        amount = float(raw_value) / 10**18
        date = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d")
        if blockchain == "ronin":
            symbol = "RON" #* Manual override for Ronin, as the native token is not named RON
        else:
            blockchain_config = next((chain for chain in BLOCKCHAIN_IDS.values() if chain["name"] == blockchain), None)
            symbol = blockchain_config["native_token"] if blockchain_config else None
            if symbol is None:
                log_to_cli(f"Native token symbol for blockchain {blockchain} not found in BLOCKCHAIN_IDS. Cannot fetch price for native token.", CliColor.ERROR)
                return amount, None

        currency_price = self.token_price_repo.get_token_price_by_symbol_and_date(symbol, date) if symbol else None
        if currency_price is None:
            min_ts, max_ts = self.fetch_transactions_timestamp_interval()
            try:
                log_to_cli(f"Price for native token {symbol} on {date} not found. Fetching price using PriceGenerator...")
                PriceGenerator.fetch_and_store_token_prices(self.bridge, self.token_price_repo, min_ts, max_ts, symbol, symbol=symbol, blockchain=blockchain)
                currency_price = self.token_price_repo.get_token_price_by_symbol_and_date(symbol, date)
            except Exception as e:
                log_to_cli(f"Failed to fetch price for native token {symbol} through PriceGenerator: {e}", CliColor.ERROR)
                return amount, None

        amount_usd = int(int(raw_value) * currency_price.price_usd) if currency_price else None
        return amount, amount_usd

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
                    func["result"] = abi_decode(["string"], bytes.fromhex(res[2:]))[0]
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

        if blockchain in TOKEN_PRICING_SUPPORTED_BLOCKCHAINS:
            min_ts, max_ts = self.fetch_transactions_timestamp_interval()
            PriceGenerator.fetch_and_store_token_prices(self.bridge, self.token_price_repo, min_ts, max_ts, 
                function_signatures[0]["result"], function_signatures[1]["result"], blockchain, contract_address
            )

        return True

    def parse_token_event(self, tx, event, event_index, token_node, graph_obj: GraphObject, token_metadata, op_index):
        contract = self.load_erc20_contract(token_node.address)
        
        # Parsing logic for ERC20 Token events
        from_address, to_address, value, type = None, None, None, None
        if event["topics"][0] == "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef": # Transfer
            event_signature = "event Transfer(address _from, address _to, uint256 _value)"
            event_args = contract.events.Transfer().process_log(event)["args"]
            from_address = event_args["from"]
            to_address = event_args["to"]
            value = event_args["value"]
            type = GraphEdgeType.TOKEN_TRANSFER.value
        elif event["topics"][0] == "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925": # Approval
            event_signature = "event Approval(address _owner, address _spender, uint256 _value)"
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
                event_index,
                event["topics"][0],
                None,
                event,
                timestamp=tx.timestamp,
                attributes_text=f"""event UnknownTokenEvent
token = {token_metadata.name} ({token_metadata.symbol}) at {token_node.address[:6]}...{token_node.address[-4:]}
blockchain = {token_node.blockchain}
topic: {event['topics'][0][:6]}...{event['topics'][0][-4:]}
number_of_args = {len(event["topics"]) - 1}
data_chunks = {len(event["data"]) // 32}
"""
            )
            graph_obj.create_edge(token_node.node_id, log_event_node.node_id, GraphEdgeType.LOG_RELATION.value, op_index)
            return
        
        from_node = graph_obj.fetch_or_create_node(from_address, timestamp=tx.timestamp)
        to_node = graph_obj.fetch_or_create_node(to_address, timestamp=tx.timestamp)

        #normalize for llm:
        # For better readability of the graph data when normalized for LLMs, 
        # we include the event signature and arguments in a more human-readable format
        from_text = "from" if type == GraphEdgeType.TOKEN_TRANSFER.value else "owner"
        to_text = "to" if type == GraphEdgeType.TOKEN_TRANSFER.value else "spender"
        
        amount, amount_usd = self.convert_token_value_to_amount(tx.timestamp, token_metadata, value)
        description = f"""{event_signature}
token = {token_metadata.name} ({token_metadata.symbol}) at {token_node.address[:6]}...{token_node.address[-4:]}
{from_text} = {from_node.node_type} ({from_node.address[:6]}...{from_node.address[-4:]})
{to_text} = {to_node.node_type} ({to_node.address[:6]}...{to_node.address[-4:]})
value = {amount} {token_metadata.symbol}
blockchain = {token_node.blockchain}
"""

        graph_obj.create_edge(from_node.node_id, to_node.node_id, type, op_index)

        # Create and link log event node to the token node
        log_event_node = graph_obj.create_log_node(
            event_index,
            event["topics"][0],
            event_signature,
            event_args,
            amount=value,
            amount_usd=amount_usd,
            timestamp=tx.timestamp,
            attributes_text=description
        )
        graph_obj.create_edge(token_node.node_id, log_event_node.node_id, GraphEdgeType.LOG_RELATION.value, op_index)

    @abstractmethod
    def fetch_transactions_for_blockchain(self, blockchain: str):
        pass

    @abstractmethod
    def parse_bridge_router_event(self, tx, event, event_index: int, routing_node, graph_obj: GraphObject):
        pass

    def link_transactions_into_cctxs(self):
        # First, fetch all the generated graphs for the bridge and blockchains
        cctx_data = self.fetch_cross_chain_transactions()
        
        for cctx in cctx_data:
            # Skip if the CCTX is already linked to a graph
            if self.cctx_graph_mapping_repo.get_by_chain_tx_hash(self.bridge.value, cctx.src_blockchain, cctx.src_transaction_hash):
                continue
            elif self.cctx_graph_mapping_repo.get_by_chain_tx_hash(self.bridge.value, cctx.dst_blockchain, cctx.dst_transaction_hash):
                continue

            # Get the respective graph mappings for the source and destination transactions
            source_graph_mapping = self.blockchain_graph_mapping_repo.graph_exists(self.bridge.value, cctx.src_blockchain, cctx.src_transaction_hash)
            destination_graph_mapping = self.blockchain_graph_mapping_repo.graph_exists(self.bridge.value, cctx.dst_blockchain, cctx.dst_transaction_hash)

            if source_graph_mapping is None or destination_graph_mapping is None:
                log_to_cli(f"Could not find graph mappings for CCTX with source tx {cctx.src_transaction_hash} on {cctx.src_blockchain} and destination tx {cctx.dst_transaction_hash} on {cctx.dst_blockchain}. Skipping...", CliColor.ERROR)
                continue
                
            log_to_cli(f"Linking CCTX with source {cctx.src_blockchain}:{cctx.src_transaction_hash} and destination {cctx.dst_blockchain}:{cctx.dst_transaction_hash}")
            # If both graph mappings exist, we can create a cross-chain graph mapping and link the respective graphs in the graph nodes and edges
            cctx_id = self.fetch_cctx_id(cctx)
            cctx_graph_mapping = self.cctx_graph_mapping_repo.create(
                {
                    "cctx_id": cctx_id,
                    "bridge": self.bridge.value,
                    "source_chain": cctx.src_blockchain,
                    "target_chain": cctx.dst_blockchain,
                    "source_tx_hash": cctx.src_transaction_hash,
                    "destination_tx_hash": cctx.dst_transaction_hash,
                    "label": GraphLabel.NORMAL.value, # Placeholder, should be changed on attack transactions 
                }
            )
            # Update the blockchain graphs and its nodes and edges to link to the cross-chain graph
            self.blockchain_graph_mapping_repo.assign_cctx_id(source_graph_mapping.graph_id, cctx_graph_mapping.cctx_graph_id)
            self.blockchain_graph_mapping_repo.assign_cctx_id(destination_graph_mapping.graph_id, cctx_graph_mapping.cctx_graph_id)

            self.graph_node_repo.assign_cctx_id(source_graph_mapping.graph_id, cctx_graph_mapping.cctx_graph_id, blockchain_type=BlockchainType.SOURCE)
            self.graph_node_repo.assign_cctx_id(destination_graph_mapping.graph_id, cctx_graph_mapping.cctx_graph_id, blockchain_type=BlockchainType.DESTINATION)

            self.graph_edge_repo.assign_cctx_id(source_graph_mapping.graph_id, cctx_graph_mapping.cctx_graph_id, blockchain_type=BlockchainType.SOURCE)
            self.graph_edge_repo.assign_cctx_id(destination_graph_mapping.graph_id, cctx_graph_mapping.cctx_graph_id, blockchain_type=BlockchainType.DESTINATION)

            # Create validation nodes in order to structurally link the graphs together
            src_router_node = self.graph_node_repo.get_router_node_by_graph_id(source_graph_mapping.graph_id)
            dst_router_node = self.graph_node_repo.get_router_node_by_graph_id(destination_graph_mapping.graph_id)
            if src_router_node is not None and dst_router_node is not None:
                if self.graph_node_repo.get_by_address(destination_graph_mapping.graph_id, f"validator_{cctx_id}") is None:
                    validator_node = self.graph_node_repo.create(
                        {
                            "node_type": GraphNodeType.VALIDATOR.value,
                            "chain_graph_id": destination_graph_mapping.graph_id, # can be either source or destination
                            "cctx_graph_id": cctx_graph_mapping.cctx_graph_id,
                            "bridge": self.bridge.value,
                            "blockchain": None,
                            "blockchain_type": BlockchainType.OFFCHAIN.value,
                            "address": f"validator_{cctx_id}",
                            "attributes": {
                                "cctx_id": cctx_id,
                                "source_chain": cctx.src_blockchain,
                                "source_tx": cctx.src_transaction_hash,
                                "target_chain": cctx.dst_blockchain,
                                "destination_tx": cctx.dst_transaction_hash
                            },
                            "attributes_text": f"type = validator; cctx_id = {cctx_id}; src_blockchain = {cctx.src_blockchain}; dst_blockchain = {cctx.dst_blockchain}",
                        }
                    )

                    # Add edges to link the validator node to the respective router nodes 
                    # on both source and destination graphs
                    self.graph_edge_repo.create(
                        {
                            "edge_type": GraphEdgeType.CROSS_CHAIN_RELATION.value,
                            "chain_graph_id": source_graph_mapping.graph_id,
                            "cctx_graph_id": cctx_graph_mapping.cctx_graph_id,
                            "bridge": self.bridge.value,
                            "source_id": src_router_node.node_id,
                            "target_id": validator_node.node_id,
                            "deposit_id": cctx_id,
                            "blockchain_type": BlockchainType.OFFCHAIN.value
                        }
                    )
                    self.graph_edge_repo.create(
                        {
                            "edge_type": GraphEdgeType.CROSS_CHAIN_RELATION.value,
                            "chain_graph_id": destination_graph_mapping.graph_id,
                            "cctx_graph_id": cctx_graph_mapping.cctx_graph_id,
                            "bridge": self.bridge.value,
                            "source_id": validator_node.node_id,
                            "target_id": dst_router_node.node_id,
                            "deposit_id": cctx_id,
                            "blockchain_type": BlockchainType.OFFCHAIN.value
                        }
                    )
            else:
                log_to_cli(f"Could not find router nodes for source graph {source_graph_mapping.graph_id} and destination graph {destination_graph_mapping.graph_id}. Skipping creation of validation node for CCTX {cctx_graph_mapping.cctx_graph_id}...", CliColor.ERROR)

    def include_native_dune_transfers(self, blockchain):
        # For each transaction hash that we couldn't trace through RPC, we can query Dune for native token transfers related to the transaction
        # and include them in the respective graphs. This way, we can still capture value movements related to the transactions even if the blockchain doesn't support transaction tracing or if the tracing data is incomplete.
        tx_hashes = self.tx_to_query_dune
        if len(tx_hashes) == 0:
            return
        
        log_to_cli(f"Querying Dune for native token transfers related to {len(tx_hashes)} transaction hashes on {blockchain}...")
        try:
            dune_results = self.dune_client.fetch_native_transactions(blockchain, tx_hashes)
            # Create a counter to generate unique operation indexes for the internal transactions, starting from the last used index in the respective graph
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
                log_to_cli(f"Including native token transfer from Dune for transaction {tx_hash} on {blockchain}: from {from_address} to {to_address} amount {value}")

                # We'll be using negative operation indexes for the internal transactions fetched from Dune
                # the main idea will be to then change the op index numbers to start from 0 
                # in a post-processing step on the BridgeDefender repository.
                op_idx_counters[tx_hash] = op_idx_counters.get(tx_hash, 0) - 1
                self.process_internal_token_transfer(graph_obj, blockchain, op_idx_counters[tx_hash], transfer, from_address, to_address, value, graph_obj.tx_timestamp)

        except Exception as e:
            log_to_cli(f"Error fetching native token transfers from Dune for blockchain {blockchain}: {e}", CliColor.ERROR)

    @abstractmethod
    def fetch_cross_chain_transactions(self):
        pass

    @abstractmethod
    def fetch_cctx_id(self, cctx):
        pass

    @abstractmethod
    def fetch_transactions_timestamp_interval(self):
        pass