import csv
import os
from typing import Any, Dict

from config.constants import Bridge
from graph_generator.base_graph_generator import BaseGraphGenerator
from graph_generator.graph_class import GraphObject
from graph_generator.graph_label import EventType, GraphEdgeType, GraphNodeType
from repository.database import DBSession
from repository.polynetwork.models import PolynetworkCrossChainTransactions
from repository.polynetwork.repository import (
    PolynetworkBlockchainTransactionRepository,
    PolynetworkCrossChainEventRepository,
    PolynetworkCrossChainTransactionsRepository,
    PolynetworkLockEventRepository,
    PolynetworkUnlockEventRepository,
    PolynetworkVerifyHeaderAndExecuteTxEventRepository,
)
from utils.utils import log_error

# PolyNetwork protocol domain IDs, as described in bridge-common:base/mainnet.go
POLYNETWORK_DOMAIN_IDS = {
    0: "poly",
    1: "bitcoin",
    2: "ethereum",
    3: "ontology",
    4: "neo",
    5: "switcheo",
    6: "bnb",
    7: "heco",
    8: "palette",
    10: "o3swap",
    12: "okxchain",
    14: "neo3",
    15: "heimdall",
    17: "polygon",
    18: "zilliqa",
    19: "arbitrum",
    20: "gnosis",
    21: "avalanche",
    22: "fantom",
    23: "optimism",
    24: "metis",
    25: "boba",
    26: "oasis",
    27: "harmony",
    28: "hsc",
    29: "bytom",
    30: "kcc",
    31: "starcoin",
    32: "kava",
    34: "milkomeda",
    35: "cube",
    36: "celo",
    37: "clover",
    38: "conflux",
    40: "astar",
    41: "aptos",
    42: "brise",
    43: "dexit",
    44: "cloudtx",
    45: "zksync",
    46: "xinfin",
    47: "ontevm",
}

class PolynetworkGraphGenerator(BaseGraphGenerator):
    def __init__(self):
        self.bridge = Bridge.POLYNETWORK
        super().__init__(self.bridge)

    def bind_db_to_repos(self) -> None:
        super().bind_db_to_repos()

        self.cross_chain_transactions_repo = PolynetworkCrossChainTransactionsRepository(DBSession)
        self.blockchain_transactions_repo = PolynetworkBlockchainTransactionRepository(DBSession)
        self.cross_chain_event_repo = PolynetworkCrossChainEventRepository(DBSession)
        self.verify_header_repo = PolynetworkVerifyHeaderAndExecuteTxEventRepository(DBSession)
        self.lock_event_repo = PolynetworkLockEventRepository(DBSession)
        self.unlock_event_repo = PolynetworkUnlockEventRepository(DBSession)

    def fetch_transactions_for_blockchain(self, blockchain: str, start_ts: int = None, end_ts: int = None):
        return self.blockchain_transactions_repo.get_transactions_from_blockchain(blockchain, start_ts, end_ts)

    def fetch_cross_chain_transactions(self):
        return self.cross_chain_transactions_repo.get_all()

    def fetch_transactions_timestamp_interval(self):
        return self.blockchain_transactions_repo.get_min_timestamp(), self.blockchain_transactions_repo.get_max_timestamp()

    def get_router_event_list(self, blockchain):
        t = (
            "event CrossChainEvent(address indexed sender, bytes txId, address proxyOrAssetContract, uint64 toChainId, bytes toContract, bytes rawdata), ",
            "event VerifyHeaderAndExecuteTxEvent(uint64 fromChainID, bytes toContract, bytes crossChainTxHash, bytes fromChainTxHash), ",
            "event LockEvent(address fromAssetHash, address fromAddress, uint64 toChainId, bytes toAssetHash, bytes toAddress, uint256 amount), ",
            "event UnlockEvent(address toAssetHash, address toAddress, uint256 amount), "
        )

        if blockchain == "ethereum" or blockchain == "bnb":
            return t + (
                "event LockEvent(address tokenAddress, address fromAddress, uint64 toChainId, bytes toAssetHash, bytes toAddress, uint256 amount, bytes txArgs), "
                "event UnlockEvent(address tokenAddress, address toAddress, uint256 amount, bytes txArgs)"
            )
        else:
            return t

    #override
    def resolve_node_address(self, address, blockchain):
        if (blockchain == "ethereum" and address.lower() in (
            '0xcf2afe102057ba5c16f899271045a0a37fcb10f2',
            '0x81910675dbaf69dee0fd77570bfd07f8e436386a'
        )) or (blockchain == "bnb" and address.lower() in (
            '0x11e2a718d46ebe97645b87f2363afe1bf28c2672',
            '0xccb7a45e36f22ede66b6222a0a55c547e6d516d7'
        )) or (blockchain == "polygon" and address.lower() in (
            '0x7cea671dabfba880af6723bddd6b9f4caa15c87b',
            '0xa9472da7e5f349a59c074e059ef0ab504735dfa2',
        )) or (blockchain == "arbitrum" and address.lower() in (
            '0x11e2a718d46ebe97645b87f2363afe1bf28c2672',
            '0x446eb3ac5e6267931ed1198203b12cafcd2e6240',
        )) or self.bridge_router_metadata_repo.get_bridge_routing_metadata_by_address_and_blockchain(
            self.bridge.value,
            address,
            blockchain
        ):
            return "__POLYNETWORK_BRIDGE_ROUTER__"
        return address

    def fetch_cctx_id(self, cctx: PolynetworkCrossChainTransactions):
        return str(cctx.deposit_id)

    def parse_bridge_router_event(self, tx, event, event_index, routing_node, graph_obj: GraphObject):
        if (
            event["topics"][0]
            == "0x6ad3bf15c1988bc04bc153490cab16db8efb9a3990215bf1c64ea6e28be88483"
        ): # CrossChainEvent(address indexed sender, bytes txId, 
            # address proxyOrAssetContract, uint64 toChainId, bytes toContract,
            # bytes rawdata)
            self.parse_cross_chain_event(tx, event, event_index, routing_node, graph_obj)
        elif (
            event["topics"][0]
            == "0x8a4a2663ce60ce4955c595da2894de0415240f1ace024cfbff85f513b656bdae"
        ): # event VerifyHeaderAndExecuteTxEvent(uint64 fromChainID, bytes toContract,
            # bytes crossChainTxHash, bytes fromChainTxHash)
            self.parse_verify_header_and_execute_tx_event(tx, event, event_index, routing_node, graph_obj)
        elif (
            event["topics"][0]
            == "0x8636abd6d0e464fe725a13346c7ac779b73561c705506044a2e6b2cdb1295ea5"
        ): # event LockEvent(address fromAssetHash, address fromAddress, uint64 toChainId,
            # bytes toAssetHash, bytes toAddress, uint256 amount)
            self.parse_lock_event(tx, event, event_index, routing_node, graph_obj)
        elif (
            event["topics"][0]
            == "0x3aa1a37a3bb16943a2c97dd810c5601a4ce19bb1942a54401f821af5515c5530"
        ): # event LockEvent(address fromAssetHash, address fromAddress, uint64 toChainId, 
            # bytes toAssetHash, bytes toAddress, uint256 amount, bytes txArgs)
            self.parse_lock_event(tx, event, event_index, routing_node, graph_obj)
        elif (
            event["topics"][0]
            == "0xd90288730b87c2b8e0c45bd82260fd22478aba30ae1c4d578b8daba9261604df"
        ): # event UnlockEvent(address toAssetHash, address toAddress, uint256 amount)
            self.parse_unlock_event(tx, event, event_index, routing_node, graph_obj)
        elif (
            event["topics"][0]
            == "0x2d3f6ad356f1c408166244c68a928a722472299760d71a6de97f6057b912972c"
        ): # event UnlockEvent(address toAssetHash, address toAddress, uint256 amount, 
            # bytes txArgs)
            self.parse_unlock_event(tx, event, event_index, routing_node, graph_obj)
        elif event:
            self.create_unknown_router_event_node(tx, event, event_index, routing_node, graph_obj)

    def parse_cross_chain_event(self, tx, event, event_index, routing_node, graph_obj: GraphObject):
        event_signature = "event CrossChainEvent(address indexed sender, bytes txId, address proxyOrAssetContract, uint64 toChainId, bytes toContract, bytes rawdata)"
        event_data = event["data"]
        if event_data.startswith("0x"):
            event_data = event_data[2:]
        raw_data = bytes.fromhex(event_data[640:])
        offset, _ = self.decode_var_bytes_at(raw_data, 0)
        _, cross_chain_tx_hash = self.decode_var_bytes_at(raw_data, offset)
        if cross_chain_tx_hash is None:
            request_desc = (
                f"CrossChainEvent with missing cross_chain_tx_hash. tx_hash:{graph_obj.graph_mapping.tx_hash}."
            )
            log_error(self.bridge, request_desc)
            return

        event_record = self.cross_chain_event_repo.event_exists(graph_obj.graph_mapping.tx_hash, cross_chain_tx_hash)
        if event_record is None:
            request_desc = (
                f"CrossChainEvent record not found for tx_hash:{graph_obj.graph_mapping.tx_hash} and cross_chain_tx_hash:{cross_chain_tx_hash}."
            )
            log_error(self.bridge, request_desc)
            return

        sender_node = graph_obj.fetch_or_create_node(
            event_record.sender,
            node_type_if_missing=GraphNodeType.USER.value,
            timestamp=tx.timestamp,
        )
        graph_obj.update_node_type(sender_node.node_id, GraphNodeType.USER.value)
        graph_obj.create_edge(
            sender_node.node_id,
            routing_node.node_id,
            GraphEdgeType.TRANSACTION.value,
            event_index,
        )

        log_event_node = graph_obj.create_log_node(
            event_index,
            event["topics"][0],
            EventType.DEPOSIT_REQUEST.value,
            event_signature,
            {
                "sender": event_record.sender,
                "tx_id": event_record.tx_id,
                "proxy_or_asset_contract": event_record.proxy_or_contract_address,
                "to_chain": event_record.to_chain,
                "to_contract": event_record.to_contract,
                "cross_chain_tx_hash": event_record.cross_chain_tx_hash,
            },
            event["data"],
            timestamp=tx.timestamp,
        )
        graph_obj.create_edge(
            routing_node.node_id, log_event_node.node_id,
            GraphEdgeType.LOG_RELATION.value, event_index
        )

    def parse_verify_header_and_execute_tx_event(self, tx, event, event_index, routing_node, graph_obj: GraphObject):
        event_signature = "event VerifyHeaderAndExecuteTxEvent(uint64 fromChainID, bytes toContract, bytes crossChainTxHash, bytes fromChainTxHash)"
        event_data = event["data"]
        if event_data.startswith("0x"):
            event_data = event_data[2:]
        cross_chain_tx_hash = event_data[448:448+64]

        if cross_chain_tx_hash is None:
            request_desc = (
                f"VerifyHeaderAndExecuteTxEvent with missing cross_chain_tx_hash. tx_hash:{graph_obj.graph_mapping.tx_hash}."
            )
            log_error(self.bridge, request_desc)
            return

        event_record = self.verify_header_repo.event_exists(graph_obj.graph_mapping.tx_hash, cross_chain_tx_hash)
        if event_record is None:
            request_desc = (
                f"VerifyHeaderAndExecuteTxEvent record not found for tx_hash:{graph_obj.graph_mapping.tx_hash}."
            )
            log_error(self.bridge, request_desc)
            return

        log_event_node = graph_obj.create_log_node(
            event_index,
            event["topics"][0],
            EventType.OPERATION_FINALIZED.value,
            event_signature,
            {
                "from_chain": event_record.from_chain,
                "to_contract": event_record.to_contract,
                "cross_chain_tx_hash": event_record.cross_chain_tx_hash,
                "from_chain_tx_hash": event_record.from_chain_tx_hash,
            },
            event["data"],
            timestamp=tx.timestamp,
        )
        graph_obj.create_edge(
            routing_node.node_id, log_event_node.node_id,
            GraphEdgeType.LOG_RELATION.value, event_index
        )

    def parse_lock_event(self, tx, event, event_index, routing_node, graph_obj: GraphObject):
        event_signature = "event LockEvent(address fromAssetHash, address fromAddress, uint64 toChainId, bytes toAssetHash, bytes toAddress, uint256 amount)"
        event_data = event["data"]
        if event_data.startswith("0x"):
            event_data = event_data[2:]
        from_address = "0x" + event_data[88:88+40]
        to_chain_id = int(event_data[176:176+16], 16)
        to_chain = self.domain_to_blockchain(to_chain_id)
        to_asset_hash = "0x" + event_data[448:448+40]
        to_address = "0x" + event_data[576:576+40]
        
        event_record = self.lock_event_repo.event_exists(graph_obj.graph_mapping.tx_hash, from_address, to_chain, to_asset_hash, to_address)
        if event_record is None:
            request_desc = (
                f"LockEvent record not found for tx_hash:{graph_obj.graph_mapping.tx_hash}.\n"
                f"Args: from_address={from_address}, to_chain={to_chain}, to_asset_hash={to_asset_hash}, to_address={to_address}"
            )
            log_error(self.bridge, request_desc)
            return

        value = int(event_record.amount)
        if value > 10e27:
            value = 10e27

        # Ensure the token node is typed correctly
        token_node = graph_obj.fetch_or_create_token_node(event_record.from_asset_hash, timestamp=tx.timestamp)
        graph_obj.update_node_type(token_node.node_id, GraphNodeType.TOKEN.value)

        token_metadata = self.inspector.ensure_metadata(
            event_record.from_asset_hash, graph_obj.graph_mapping.blockchain
        )
        if token_metadata is not None:
            _, amount_usd = self.pricing.resolve_token_amount(token_metadata, value, tx.timestamp)
        else:
            amount_usd = None

        event_args = {
            "from_asset_hash": event_record.from_asset_hash,
            "from_address": event_record.from_address,
            "to_chain": event_record.to_chain,
            "to_asset_hash": event_record.to_asset_hash,
            "to_address": event_record.to_address,
            "amount": value,
        }
        if event_record.fee_amount is not None:
            event_args["fee_amount"] = int(event_record.fee_amount)
        if event_record.fee_address is not None:
            event_args["fee_address"] = event_record.fee_address
        if event_record.nonce is not None:
            event_args["nonce"] = event_record.nonce

        log_event_node = graph_obj.create_log_node(
            event_index,
            event["topics"][0],
            EventType.OPERATION_REQUEST_SIGNING.value,
            event_signature,
            event_args,
            event["data"],
            amount=value,
            amount_usd=amount_usd,
            token_symbol=token_metadata.symbol if token_metadata else None,
            timestamp=tx.timestamp,
        )
        if token_metadata and amount_usd is None:
            self.pricing.record_missing_price(
                log_event_node.node_id, token_metadata.symbol, tx.timestamp, value
            )
        graph_obj.create_edge(
            routing_node.node_id, log_event_node.node_id,
            GraphEdgeType.LOG_RELATION.value, event_index
        )

    def parse_unlock_event(self, tx, event, event_index, routing_node, graph_obj: GraphObject):
        event_signature = "event UnlockEvent(address toAssetHash, address toAddress, uint256 amount)"
        event_data = event["data"]
        if event_data.startswith("0x"):
            event_data = event_data[2:]
        to_asset_hash = "0x" + event_data[24:24+40]
        to_address = "0x" + event_data[88:88+40]

        event_record = self.unlock_event_repo.event_exists(graph_obj.graph_mapping.tx_hash, to_asset_hash, to_address)
        if event_record is None:
            request_desc = (
                f"UnlockEvent record not found for tx_hash:{graph_obj.graph_mapping.tx_hash}.\n"
                f"Args: to_asset_hash={to_asset_hash}, to_address={to_address}"
            )
            log_error(self.bridge, request_desc)
            return
        
        receiver_node = graph_obj.fetch_or_create_node(
            event_record.to_address,
            node_type_if_missing=GraphNodeType.USER.value,
            timestamp=tx.timestamp,
        )
        graph_obj.update_node_type(receiver_node.node_id, GraphNodeType.USER.value)

        value = int(event_record.amount)
        if value > 10e27:
            value = 10e27

        # Ensure the token node is typed correctly
        token_node = graph_obj.fetch_or_create_token_node(event_record.to_asset_hash, timestamp=tx.timestamp)
        graph_obj.update_node_type(token_node.node_id, GraphNodeType.TOKEN.value)

        token_metadata = self.inspector.ensure_metadata(
            event_record.to_asset_hash, graph_obj.graph_mapping.blockchain
        )
        if token_metadata is not None:
            _, amount_usd = self.pricing.resolve_token_amount(token_metadata, value, tx.timestamp)
        else:
            amount_usd = None

        event_args = {
            "to_asset_hash": event_record.to_asset_hash,
            "to_address": event_record.to_address,
            "amount": value,
        }
        if event_record.from_asset_hash is not None:
            event_args["from_asset_hash"] = event_record.from_asset_hash
        if event_record.from_address is not None:
            event_args["from_address"] = event_record.from_address
        if event_record.fee_amount is not None:
            event_args["fee_amount"] = int(event_record.fee_amount)
        if event_record.fee_address is not None:
            event_args["fee_address"] = event_record.fee_address
        if event_record.nonce is not None:
            event_args["nonce"] = event_record.nonce

        log_event_node = graph_obj.create_log_node(
            event_index,
            event["topics"][0],
            EventType.DEPOSIT_CONFIRMATION.value,
            event_signature,
            event_args,
            event["data"],
            amount=value,
            amount_usd=amount_usd,
            token_symbol=token_metadata.symbol if token_metadata else None,
            timestamp=tx.timestamp,
        )
        if token_metadata and amount_usd is None:
            self.pricing.record_missing_price(
                log_event_node.node_id, token_metadata.symbol, tx.timestamp, value
            )
        graph_obj.create_edge(
            routing_node.node_id, log_event_node.node_id,
            GraphEdgeType.LOG_RELATION.value, event_index
        )

    # =================
    # Helper functions for decoding data from events.
    # The return value is the new offset, followed by the decoded value.
    def domain_to_blockchain(self, domain_id: int) -> str | None:
        return POLYNETWORK_DOMAIN_IDS.get(domain_id)
    
    def decode_var_bytes_at(self, data: bytes, offset: int) -> str:
        if offset > len(data):
            return offset, None
        first = data[offset]
        offset += 1

        if first < 0xFD:
            length = first
        elif first == 0xFD:
            length = int.from_bytes(data[offset : offset + 2], "little")
            offset += 2
        elif first == 0xFE:
            length = int.from_bytes(data[offset : offset + 4], "little")
            offset += 4
        else:  # 0xFF
            length = int.from_bytes(data[offset : offset + 8], "little")
            offset += 8

        return offset + length, data[offset : offset + length].hex()

    def decode_var_uint_at(self, data: bytes, offset: int, num_bits: int) -> int:
        if offset > len(data):
            return offset, None
        right = offset + (num_bits + 7) // 8
        raw = data[offset: right]
        return right, int.from_bytes(raw, "little")
    
    def decode_tx_args(self, data) -> Dict[str, Any]:
        if isinstance(data, str):
            if data.startswith("0x"):
                data = data[2:]
            data = bytes.fromhex(data)
        offset, from_asset_hash = self.decode_var_bytes_at(data, 0)
        offset, to_asset_hash = self.decode_var_bytes_at(data, offset)
        offset, amount = self.decode_var_uint_at(data, offset, 256)
        offset, fee_amount = self.decode_var_uint_at(data, offset, 256)
        offset, fee_address = self.decode_var_bytes_at(data, offset)
        offset, from_address = self.decode_var_bytes_at(data, offset)
        offset, nonce = self.decode_var_uint_at(data, offset, 256)

        return {
            "from_asset_hash": from_asset_hash,
            "to_asset_hash": to_asset_hash,
            "amount": amount,
            "fee_amount": fee_amount,
            "fee_address": fee_address,
            "from_address": from_address,
            "nonce": nonce,
        }