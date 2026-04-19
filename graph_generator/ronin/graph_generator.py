

from config.constants import Bridge
from graph_generator.base_graph_generator import BaseGraphGenerator
from graph_generator.graph_class import GraphObject
from graph_generator.graph_label import EventType, GraphEdgeType, GraphNodeType
from graph_generator.ronin.constants import ANOMALY_TRANSACTIONS, OFFCHAIN_ANOMALY_TRANSACTIONS
from repository.database import DBSession
from repository.ronin.models import RoninCrossChainTransaction
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
        self.bridge = Bridge.RONIN
        self.chain_anomaly_transactions = ANOMALY_TRANSACTIONS
        self.offchain_anomaly_transactions = OFFCHAIN_ANOMALY_TRANSACTIONS
        super().__init__(self.bridge)

    def bind_db_to_repos(self) -> None:
        super().bind_db_to_repos()

        self.cross_chain_transactions_repo = RoninCrossChainTransactionRepository(DBSession)
        self.blockchain_transactions_repo = RoninBlockchainTransactionRepository(DBSession)
        self.deposit_requested_repo = RoninDepositRequestedRepository(DBSession)
        self.token_deposited_repo = RoninTokenDepositedRepository(DBSession)
        self.withdrawal_requested_repo = RoninWithdrawalRequestedRepository(DBSession)
        self.token_withdrew_repo = RoninTokenWithdrewRepository(DBSession)

    def fetch_transactions_for_blockchain(self, blockchain: str):
        return self.blockchain_transactions_repo.get_transactions_from_blockchain(blockchain)

    def fetch_cross_chain_transactions(self):
        return self.cross_chain_transactions_repo.get_all()
    
    def fetch_transactions_timestamp_interval(self):
        return self.blockchain_transactions_repo.get_min_timestamp(), self.blockchain_transactions_repo.get_max_timestamp()

    def get_router_event_list(self, blockchain):
        if blockchain == "ethereum":
            return "event DepositRequested(bytes32 receiptHash, tuple receipt), " + \
                   "event TokenWithdrew(bytes32 receiptHash, tuple receipt)"
        elif blockchain == "ronin":
            return "event TokenDeposited(bytes32 receiptHash, tuple receipt), " + \
                   "event WithdrawRequested(bytes32 receiptHash, tuple receipt), "

    def fetch_cctx_id(self, cctx: RoninCrossChainTransaction):
        # For Ronin, we can directly use the cctx_id from the database as the unique identifier for the cross-chain transaction
        return str(cctx.deposit_id)

    def parse_bridge_router_event(self, tx, event, event_index, routing_node, graph_obj: GraphObject):
        if (
            event["topics"][0] == "0xd7b25068d9dc8d00765254cfb7f5070f98d263c8d68931d937c7362fa738048b"
        ): # DepositRequested
            self.parse_deposit_requested_event(tx, event, event_index, routing_node, graph_obj)
        elif (
            event["topics"][0] == "0x8d20d8121a34dded9035ff5b43e901c142824f7a22126392992c353c37890524"
        ): # Deposited
            self.parse_token_deposited_event(tx, event, event_index, routing_node, graph_obj)
        elif (
            event["topics"][0] == "0xf313c253a5be72c29d0deb2c8768a9543744ac03d6b3cafd50cc976f1c2632fc"
        ): # WithdrawRequested
            self.parse_withdraw_requested_event(tx, event, event_index, routing_node, graph_obj)
        elif (
            event["topics"][0] == "0x21e88e956aa3e086f6388e899965cef814688f99ad8bb29b08d396571016372d"
        ): # Withdrew
            self.parse_token_withdrew_event(tx, event, event_index, routing_node, graph_obj)
        elif event:
            self.create_unknown_router_event_node(tx, event, event_index, routing_node, graph_obj)

    def parse_deposit_requested_event(self, tx, event, event_index, routing_node, graph_obj: GraphObject):
        event_signature = "event DepositRequested(bytes32 receiptHash, tuple receipt)"
        # Fetch the respective metadata from the repository
        event_record = self.deposit_requested_repo.fetch_by_transaction_hash(graph_obj.graph_mapping.tx_hash)
        if event_record is None:
            # Log error to error.log #TODO TODO
            pass

        # Ensure the depositor is a user node
        depositor_node = graph_obj.fetch_or_create_node(
            event_record.depositor,
            node_type_if_missing=GraphNodeType.USER.value,
            timestamp=tx.timestamp
        )
        graph_obj.update_node_type(depositor_node.node_id, GraphNodeType.USER.value)
        graph_obj.create_edge(
            depositor_node.node_id, 
            routing_node.node_id, 
            GraphEdgeType.TRANSACTION.value,
            event_index,
            attributes={
                "amount": int(event_record.amount)
            }
        )

        # Link the routing node and the token node with a function call edge
        token_node = graph_obj.fetch_or_create_token_node(
            event_record.input_token,
            timestamp=tx.timestamp
        )
        graph_obj.create_edge(
            routing_node.node_id,
            token_node.node_id,
            GraphEdgeType.FUNCTION_CALL.value,
            event_index,
        )

        event_args = {
            "deposit_id": event_record.deposit_id,
            "kind": event_record.kind,
            "amount": int(event_record.amount),
            "depositor": event_record.depositor,
            "input_token": event_record.input_token,
            "destination_chain": event_record.dst_blockchain,
            "recipient": event_record.recipient,
            "output_token": event_record.output_token,
        }
        input_token_metadata = self.load_token_metadata(event_record.input_token, graph_obj.graph_mapping.blockchain)
        
        if input_token_metadata is not None:
            amount, amount_usd = self.convert_token_value_to_amount(tx.timestamp, input_token_metadata, event_record.amount)
        else:
            amount, amount_usd = None, None
        event_text = f"""{event_signature}
bridge = ronin
blockchain = {graph_obj.graph_mapping.blockchain}
cctx_id = {event_record.deposit_id}
depositor = {depositor_node.node_type} ({depositor_node.address[:6]}...{depositor_node.address[-4:]})
input_token ={f" {input_token_metadata.name} ({input_token_metadata.symbol}) at" if input_token_metadata else ""} {event_record.input_token[:6]}...{event_record.input_token[-4:]}
{f"in_amount = {amount} {input_token_metadata.symbol}" if input_token_metadata else f"amount = {int(event_record.amount)}"}
recipient = {GraphNodeType.USER.value} ({event_record.recipient[:6]}...{event_record.recipient[-4:]})
destination_chain = {event_record.dst_blockchain}
"""

        # Create and link log event node to the routing node
        log_event_node = graph_obj.create_log_node(
            event_index,
            event["topics"][0],
            EventType.DEPOSIT_REQUEST.value,
            event_signature,
            event_args,
            None,
            attributes_text=event_text,
            amount=int(event_record.amount),
            amount_usd=amount_usd,
            token_symbol=input_token_metadata.symbol if input_token_metadata else None,
            timestamp=tx.timestamp
        )
        graph_obj.create_edge(
            routing_node.node_id, 
            log_event_node.node_id, 
            GraphEdgeType.LOG_RELATION.value,
            event_index
        )

    def parse_token_deposited_event(self, tx, event, event_index, routing_node, graph_obj: GraphObject):
        event_signature = "event TokenDeposited(bytes32 receiptHash, tuple receipt)"
        # Fetch the respective metadata from the repository
        event_record = self.token_deposited_repo.fetch_by_transaction_hash(graph_obj.graph_mapping.tx_hash)
        if event_record is None:
            # Log error to error.log
            pass

        # Link the routing node and the token node with a function call edge
        token_node = graph_obj.fetch_or_create_token_node(
            event_record.output_token,
            timestamp=tx.timestamp
        )
        graph_obj.create_edge(
            routing_node.node_id,
            token_node.node_id,
            GraphEdgeType.FUNCTION_CALL.value,
            event_index
        )

        # Ensure the recipient is a user node
        recipient_node = graph_obj.fetch_or_create_node(
            event_record.recipient,
            node_type_if_missing=GraphNodeType.USER.value,
            timestamp=tx.timestamp
        )
        graph_obj.update_node_type(recipient_node.node_id, GraphNodeType.USER.value)
        
        event_args = {
            "deposit_id": event_record.deposit_id,
            "kind": event_record.kind,
            "amount": int(event_record.amount),
            "depositor": event_record.depositor,
            "input_token": event_record.input_token,
            "source_chain": event_record.src_blockchain,
            "recipient": event_record.recipient,
            "output_token": event_record.output_token,
        }
        output_token_metadata = self.load_token_metadata(event_record.output_token, graph_obj.graph_mapping.blockchain)
        
        if output_token_metadata is not None:
            out_amount, out_amount_usd = self.convert_token_value_to_amount(tx.timestamp, output_token_metadata, event_record.amount)
        else:
            out_amount, out_amount_usd = None, None
        event_text = f"""{event_signature}
bridge = ronin
blockchain = {graph_obj.graph_mapping.blockchain}
cctx_id = {event_record.deposit_id}
depositor = {GraphNodeType.USER.value} ({event_record.depositor[:6]}...{event_record.depositor[-4:]})
recipient = {recipient_node.node_type} ({recipient_node.address[:6]}...{recipient_node.address[-4:]})
output_token ={f" {output_token_metadata.name} ({output_token_metadata.symbol}) at" if output_token_metadata else ""} {event_record.output_token[:6]}...{event_record.output_token[-4:]}
{f"out_amount = {out_amount} {output_token_metadata.symbol}" if output_token_metadata else f"amount = {int(event_record.amount)}"}
source_chain = {event_record.src_blockchain}
"""

        # Create and link log event node to the routing node
        log_event_node = graph_obj.create_log_node(
            event_index,
            event["topics"][0],
            EventType.DEPOSIT_CONFIRMATION.value,
            event_signature,
            event_args,
            None,
            attributes_text=event_text,
            amount=int(event_record.amount),
            amount_usd=out_amount_usd,
            token_symbol=output_token_metadata.symbol if output_token_metadata else None,
            timestamp=tx.timestamp,
        )
        graph_obj.create_edge(
            routing_node.node_id, 
            log_event_node.node_id, 
            GraphEdgeType.LOG_RELATION.value,
            event_index
        )

    def parse_withdraw_requested_event(self, tx, event, event_index, routing_node, graph_obj: GraphObject):
        event_signature = "event WithdrawRequested(bytes32 receiptHash, tuple receipt)"
        # Fetch the respective metadata from the repository
        event_record = self.withdrawal_requested_repo.fetch_by_transaction_hash(graph_obj.graph_mapping.tx_hash)
        if event_record is None:
            # Log error to error.log #TODO TODO
            pass

        # Ensure the withdrawer is a user node
        withdrawer_node = graph_obj.fetch_or_create_node(
            event_record.withdrawer,
            node_type_if_missing=GraphNodeType.USER.value,
            timestamp=tx.timestamp
        )
        graph_obj.update_node_type(withdrawer_node.node_id, GraphNodeType.USER.value)
        graph_obj.create_edge(
            withdrawer_node.node_id, 
            routing_node.node_id, 
            GraphEdgeType.TRANSACTION.value,
            event_index,
            attributes={
                "amount": int(event_record.amount)
            }
        )

        # Link the routing node and the token node with a function call edge
        token_node = graph_obj.fetch_or_create_token_node(
            event_record.input_token,
            timestamp=tx.timestamp
        )
        graph_obj.create_edge(
            routing_node.node_id,
            token_node.node_id,
            GraphEdgeType.FUNCTION_CALL.value,
            event_index
        )

        event_args = {
            "receipt": {
                "withdrawal_id": event_record.withdrawal_id,
                "kind": event_record.kind,
                "amount": int(event_record.amount),
                "withdrawer": event_record.withdrawer,
                "input_token": event_record.input_token,
                "destination_chain": event_record.dst_blockchain,
                "recipient": event_record.recipient,
                "output_token": event_record.output_token,
            }
        }
        input_token_metadata = self.load_token_metadata(event_record.input_token, graph_obj.graph_mapping.blockchain)
        
        if input_token_metadata is not None:
            in_amount, in_amount_usd = self.convert_token_value_to_amount(tx.timestamp, input_token_metadata, event_record.amount)
        else:
            in_amount, in_amount_usd = None, None
        event_text = f"""{event_signature}
bridge = ronin
blockchain = {graph_obj.graph_mapping.blockchain}
cctx_id = {event_record.withdrawal_id}
withdrawer = {withdrawer_node.node_type} ({withdrawer_node.address[:6]}...{withdrawer_node.address[-4:]})
input_token ={f" {input_token_metadata.name} ({input_token_metadata.symbol}) at" if input_token_metadata else ""} {event_record.input_token[:6]}...{event_record.input_token[-4:]}
{f"in_amount = {in_amount} {input_token_metadata.symbol}" if input_token_metadata else f"amount = {int(event_record.amount)}"}
recipient = {GraphNodeType.USER.value} ({event_record.recipient[:6]}...{event_record.recipient[-4:]})
destination_chain = {event_record.dst_blockchain}
"""

        # Create and link log event node to the routing node
        log_event_node = graph_obj.create_log_node(
            event_index,
            event["topics"][0],
            EventType.WITHDRAWAL_REQUEST.value,
            event_signature,
            event_args,
            None,
            attributes_text=event_text,
            amount=int(event_record.amount),
            amount_usd=in_amount_usd,
            token_symbol=input_token_metadata.symbol if input_token_metadata else None,
            timestamp=tx.timestamp
        )
        graph_obj.create_edge(
            routing_node.node_id, 
            log_event_node.node_id, 
            GraphEdgeType.LOG_RELATION.value,
            event_index
        )

    def parse_token_withdrew_event(self, tx, event, event_index, routing_node, graph_obj: GraphObject):
        event_signature = "event TokenWithdrew(bytes32 receiptHash, tuple receipt)"
        # Fetch the respective metadata from the repository
        event_record = self.token_withdrew_repo.fetch_by_transaction_hash(graph_obj.graph_mapping.tx_hash)
        if event_record is None:
            # Log error to error.log #TODO TODO
            pass

        # Link the routing node and the token node with a function call edge
        token_node = graph_obj.fetch_or_create_token_node(
            event_record.output_token,
            timestamp=tx.timestamp
        )
        graph_obj.create_edge(
            routing_node.node_id,
            token_node.node_id,
            GraphEdgeType.FUNCTION_CALL.value,
            event_index
        )

        # Ensure the recipient is a user node
        recipient_node = graph_obj.fetch_or_create_node(
            event_record.recipient,
            node_type_if_missing=GraphNodeType.USER.value,
            timestamp=tx.timestamp
        )
        graph_obj.update_node_type(recipient_node.node_id, GraphNodeType.USER.value)

        event_args = {
            "receipt": {
                "withdrawal_id": event_record.withdrawal_id,
                "kind": event_record.kind,
                "amount": int(event_record.amount),
                "withdrawer": event_record.withdrawer,
                "input_token": event_record.input_token,
                "source_chain": event_record.src_blockchain,
                "recipient": event_record.recipient,
                "output_token": event_record.output_token,
            }
        }
        output_token_metadata = self.load_token_metadata(event_record.output_token, graph_obj.graph_mapping.blockchain)
        
        if output_token_metadata is not None:
            out_amount, out_amount_usd = self.convert_token_value_to_amount(tx.timestamp, output_token_metadata, event_record.amount)
        else:
            out_amount, out_amount_usd = None, None
        event_text = f"""{event_signature}
bridge = ronin
blockchain = {graph_obj.graph_mapping.blockchain}
cctx_id = {event_record.withdrawal_id}
withdrawer = {GraphNodeType.USER.value} ({event_record.withdrawer[:6]}...{event_record.withdrawer[-4:]})
recipient = {recipient_node.node_type} ({recipient_node.address[:6]}...{recipient_node.address[-4:]})
output_token ={f" {output_token_metadata.name} ({output_token_metadata.symbol}) at" if output_token_metadata else ""} {event_record.output_token[:6]}...{event_record.output_token[-4:]}
{f"out_amount = {out_amount} {output_token_metadata.symbol}" if output_token_metadata else f"amount = {int(event_record.amount)}"}
source_chain = {event_record.src_blockchain}
"""

        # Create and link log event node to the routing node
        log_event_node = graph_obj.create_log_node(
            event_index,
            event["topics"][0],
            EventType.WITHDRAWAL_CONFIRMATION.value,
            event_signature,
            event_args,
            None,
            attributes_text=event_text,
            amount=int(event_record.amount),
            amount_usd=out_amount_usd,
            token_symbol=output_token_metadata.symbol if output_token_metadata else None,
            timestamp=tx.timestamp
        )
        graph_obj.create_edge(
            routing_node.node_id, 
            log_event_node.node_id, 
            GraphEdgeType.LOG_RELATION.value,
            event_index
        )
