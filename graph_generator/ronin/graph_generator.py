from config.constants import Bridge
from graph_generator.base_graph_generator import BaseGraphGenerator
from graph_generator.graph_label import GraphLabel
from repository.common.repository import (
    BridgeRoutingContractMetadataRepository,
    TokenMetadataRepository,
)
from repository.database import DBSession
from repository.graphs.models import GraphEdgeType, GraphNodeType
from repository.graphs.repository import (
    GraphEdgeRepository,
    GraphMappingRepository,
    GraphNodeRepository,
)
from repository.ronin.repository import (
    RoninBlockchainTransactionRepository,
    RoninCrossChainTransactionRepository,
    RoninDepositRequestedRepository,
    RoninTokenDepositedRepository,
    RoninTokenWithdrewRepository,
    RoninWithdrawalRequestedRepository,
)


class RoninGraphGenerator(BaseGraphGenerator):
    def __init__(self):
        super().__init__()
        self.bridge = Bridge.RONIN

    def bind_db_to_repos(self) -> None:
        self.cross_chain_transactions_repo = RoninCrossChainTransactionRepository(DBSession)
        self.transactions_repo = RoninBlockchainTransactionRepository(DBSession)
        self.deposit_requested_repo = RoninDepositRequestedRepository(DBSession)
        self.token_deposited_repo = RoninTokenDepositedRepository(DBSession)
        self.withdrawal_requested_repo = RoninWithdrawalRequestedRepository(DBSession)
        self.token_withdrew_repo = RoninTokenWithdrewRepository(DBSession)

        self.bridge_router_metadata_repo = BridgeRoutingContractMetadataRepository(DBSession)
        self.token_metadata_repo = TokenMetadataRepository(DBSession)

        self.graph_mapping_repo = GraphMappingRepository(DBSession)
        self.graph_node_repo = GraphNodeRepository(DBSession)
        self.graph_edge_repo = GraphEdgeRepository(DBSession)

    def generate_graph_data(self) -> None:
        func_name = "generate_graph_data"
        
        self.generate_from_cctx()

    def generate_from_cctx(self) -> None:
        func_name = "generate_from_cctx"

        cctxs = self.cross_chain_transactions_repo.get_all()
        for cctx in cctxs:
            # TODO Check whether the graph for this cctx already exists to avoid duplicates.
            self.process_cross_chain_transaction(cctx)

    def process_cross_chain_transaction(self, cctx) -> None:
        func_name = "process_cross_chain_transaction"

        if self.graph_mapping_repo.graph_exists(self.bridge.value, cctx.deposit_id):
            return
        
        # Graph Mapping table
        mapping = self.graph_mapping_repo.create(
            {
                "cctx_id": cctx.deposit_id,
                "bridge": self.bridge.value,
                "source_chain": cctx.src_blockchain,
                "target_chain": cctx.dst_blockchain,
                "source_tx_hash": cctx.src_transaction_hash,
                "destination_tx_hash": cctx.dst_transaction_hash,
                "label": GraphLabel.NORMAL.value,   # This is a placeholder. Labeling logic can be implemented here based on certain heuristics or rules.
            }
        )

        # Source Chain
        # Starting User Node
        depositor = self.graph_node_repo.create(
            {
                "node_type": GraphNodeType.USER.value,
                "graph_id": mapping.graph_id,
                "bridge": self.bridge.value,
                "blockchain": cctx.src_blockchain,
                "address": cctx.depositor
            }
        )

        if cctx.depositor.lower() != cctx.src_from_address.lower():
            # Add another node for the sender of the transaction 
            # if it's different from the depositor
            # And a token auth edge from the depositor to the sender
            sender = self.graph_node_repo.create(
                {
                    "node_type": GraphNodeType.USER.value, #! This may also be an "OTHER_CONTRACT" if the sender is a contract
                    "graph_id": mapping.graph_id,
                    "bridge": self.bridge.value,
                    "blockchain": cctx.src_blockchain,
                    "address": cctx.src_from_address,
                }
            )
            self.graph_edge_repo.create(
                {
                    "edge_type": GraphEdgeType.TOKEN_AUTH.value,
                    "graph_id": mapping.graph_id,
                    "bridge": self.bridge.value,
                    "source_id": depositor.node_id,
                    "target_id": sender.node_id,
                }
            )
        else:
            sender = None

        # Bridge Router Node
        bridge_router = self.graph_node_repo.create(
            {
                "node_type": GraphNodeType.ROUTER.value,
                "graph_id": mapping.graph_id,
                "bridge": self.bridge.value,
                "blockchain": cctx.src_blockchain,
                "address": cctx.src_to_address,
                # function list is not included for now for space reasons
                # it is available in the bridge_router_metadata_repo
            }
        )
        self.graph_edge_repo.create(
            {
                "edge_type": GraphEdgeType.TRANSACTION.value,
                "graph_id": mapping.graph_id,
                "bridge": self.bridge.value,
                "source_id": sender.node_id if sender is not None else depositor.node_id,
                "target_id": bridge_router.node_id,
                "tx_hash": cctx.src_transaction_hash,
                "amount": cctx.amount,
            }
        )

        # Log entry for the deposit event
        source_tx = self.transactions_repo.get_transaction_by_hash(cctx.src_transaction_hash)
        event_name, event = self.obtain_event_from_transaction(cctx.src_blockchain, cctx.deposit_id, cctx.src_transaction_hash)
        log_event = self.graph_node_repo.create(
            {
                "node_type": GraphNodeType.LOG_EVENT.value,
                "graph_id": mapping.graph_id,
                "bridge": self.bridge.value,
                "blockchain": cctx.src_blockchain,
                "attributes": {
                    "input": source_tx.decoded_input if source_tx.decoded_input is not None else None, # decoded_input is stored as a string representation of a dict
                    "event_name": event_name,
                    "event": event if event is not None else None,
                    "src_fee": int(cctx.src_fee),
                },
                "timestamp": cctx.src_timestamp
            }
        )
        self.graph_edge_repo.create(
            {
                "edge_type": GraphEdgeType.LOG_RELATION.value,
                "graph_id": mapping.graph_id,
                "bridge": self.bridge.value,
                "source_id": bridge_router.node_id,
                "target_id": log_event.node_id, # The log event node is connected to the bridge router node to indicate that the event is emitted by the router
                "tx_hash": cctx.src_transaction_hash,
            }
        )

        # Token Node for the transferred token
        token_metadata = self.obtain_token_metadata(cctx.src_blockchain, cctx.src_contract_address)
        token_node = self.graph_node_repo.create(
            {
                "node_type": GraphNodeType.TOKEN.value,
                "graph_id": mapping.graph_id,
                "bridge": self.bridge.value,
                "blockchain": cctx.src_blockchain,
                "address": cctx.src_contract_address,
                "attributes": {
                    "symbol": token_metadata.symbol,
                    "name": token_metadata.name
                } if token_metadata is not None else None
            }
        )
        self.graph_edge_repo.create(
            {
                "edge_type": GraphEdgeType.FUNCTION_CALL.value,
                "graph_id": mapping.graph_id,
                "bridge": self.bridge.value,
                "source_id": bridge_router.node_id,
                "target_id": token_node.node_id,
                "tx_hash": cctx.src_transaction_hash,
                "amount": cctx.amount,
            }
        )

        # Validation Node
        validator_node = self.graph_node_repo.create(
            {
                "node_type": GraphNodeType.VALIDATOR.value,
                "graph_id": mapping.graph_id,
                "bridge": self.bridge.value,
                "blockchain": cctx.dst_blockchain,   # Related to the address that initiated the transaction
                "address": cctx.dst_from_address,
            }
        )
        self.graph_edge_repo.create(
            {
                "edge_type": GraphEdgeType.CROSS_CHAIN_RELATION.value,
                "graph_id": mapping.graph_id,
                "bridge": self.bridge.value,
                "source_id": bridge_router.node_id,
                "target_id": validator_node.node_id,
                "deposit_id": cctx.deposit_id,
            }
        )

        # Destination Chain
        # Bridge Router Node
        dest_bridge_router = self.graph_node_repo.create(
            {
                "node_type": GraphNodeType.ROUTER.value,
                "graph_id": mapping.graph_id,
                "bridge": self.bridge.value,
                "blockchain": cctx.dst_blockchain,
                "address": cctx.dst_to_address,
                # function list is not included for now for space reasons
                # it is available in the bridge_router_metadata_repo
            }
        )
        self.graph_edge_repo.create(
            {
                "edge_type": GraphEdgeType.CROSS_CHAIN_RELATION.value,
                "graph_id": mapping.graph_id,
                "bridge": self.bridge.value,
                "source_id": validator_node.node_id,
                "target_id": dest_bridge_router.node_id,
                "deposit_id": cctx.deposit_id,
            }
        )

        # Log entry
        dest_tx = self.transactions_repo.get_transaction_by_hash(cctx.dst_transaction_hash)
        event_name, event = self.obtain_event_from_transaction(cctx.dst_blockchain, cctx.deposit_id, cctx.dst_transaction_hash)
        dest_log_event = self.graph_node_repo.create(
            {
                "node_type": GraphNodeType.LOG_EVENT.value,
                "graph_id": mapping.graph_id,
                "bridge": self.bridge.value,
                "blockchain": cctx.dst_blockchain,
                "attributes": {
                    "input": dest_tx.decoded_input if dest_tx.decoded_input is not None else None,
                    "event_name": event_name,
                    "event": event if event is not None else None,
                    "dst_fee": int(cctx.dst_fee),
                },
                "timestamp": cctx.dst_timestamp
            }
        )
        self.graph_edge_repo.create(
            {
                "edge_type": GraphEdgeType.LOG_RELATION.value,
                "graph_id": mapping.graph_id,
                "bridge": self.bridge.value,
                "source_id": dest_bridge_router.node_id,
                "target_id": dest_log_event.node_id, # The log event node is connected to the bridge router node to indicate that the event is emitted by the router
                "tx_hash": cctx.dst_transaction_hash,
            }
        )

        # Token Node
        token_metadata = self.obtain_token_metadata(cctx.dst_blockchain, cctx.dst_contract_address)
        dest_token_node = self.graph_node_repo.create(
            {
                "node_type": GraphNodeType.TOKEN.value,
                "graph_id": mapping.graph_id,
                "bridge": self.bridge.value,
                "blockchain": cctx.dst_blockchain,
                "address": cctx.dst_contract_address,
                "attributes": {
                    "symbol": token_metadata.symbol,
                    "name": token_metadata.name
                } if token_metadata is not None else None
            }
        )
        self.graph_edge_repo.create(
            {
                "edge_type": GraphEdgeType.FUNCTION_CALL.value,
                "graph_id": mapping.graph_id,
                "bridge": self.bridge.value,
                "source_id": dest_bridge_router.node_id,
                "target_id": dest_token_node.node_id,
                "tx_hash": cctx.dst_transaction_hash,
                "amount": cctx.amount,
            }
        )

        # User Node for the recipient of the transaction
        dest_user_node = self.graph_node_repo.create(
            {
                "node_type": GraphNodeType.USER.value,
                "graph_id": mapping.graph_id,
                "bridge": self.bridge.value,
                "blockchain": cctx.dst_blockchain,
                "address": cctx.dst_to_address,
            }
        )
        self.graph_edge_repo.create(
            {
                "edge_type": GraphEdgeType.TOKEN_TRANSFER.value,
                "graph_id": mapping.graph_id,
                "bridge": self.bridge.value,
                "source_id": dest_token_node.node_id,
                "target_id": dest_user_node.node_id,
                "tx_hash": cctx.dst_transaction_hash,
            }
        )
        

        

    def obtain_event_from_transaction(self, blockchain: str, deposit_id: str, tx_hash: str):
        deposit_event = self.deposit_requested_repo.event_exists(deposit_id)
        if deposit_event is not None and blockchain == deposit_event.blockchain and tx_hash == deposit_event.transaction_hash:
            event_dict = deposit_event.__dict__
            event_dict.pop("_sa_instance_state")
            event_dict["amount"] = int(event_dict["amount"]) # Convert Decimal to int for JSON serialization
            return "DepositRequested", event_dict
        
        token_deposited_event = self.token_deposited_repo.event_exists(deposit_id)
        if token_deposited_event is not None and blockchain == token_deposited_event.blockchain and tx_hash == token_deposited_event.transaction_hash:
            event_dict = token_deposited_event.__dict__
            event_dict.pop("_sa_instance_state")
            event_dict["amount"] = int(event_dict["amount"])
            return "TokenDeposited", event_dict
        
        withdrawal_requested_event = self.withdrawal_requested_repo.event_exists(deposit_id)
        if withdrawal_requested_event is not None and blockchain == withdrawal_requested_event.blockchain and tx_hash == withdrawal_requested_event.transaction_hash:
            event_dict = withdrawal_requested_event.__dict__
            event_dict["amount"] = int(event_dict["amount"])
            event_dict.pop("_sa_instance_state")
            return "WithdrawalRequested", event_dict
        
        token_withdrew_event = self.token_withdrew_repo.event_exists(deposit_id)
        if token_withdrew_event is not None and blockchain == token_withdrew_event.blockchain and tx_hash == token_withdrew_event.transaction_hash:
            event_dict = token_withdrew_event.__dict__
            event_dict["amount"] = int(event_dict["amount"])
            event_dict.pop("_sa_instance_state")
            return "TokenWithdrew", event_dict
        
        return None, None

    def obtain_token_metadata(self, blockchain: str, token_address: str) -> dict:
        return self.token_metadata_repo.get_token_metadata_by_contract_and_blockchain(token_address, blockchain)
