

from config.constants import BLOCKCHAIN_IDS, Bridge
from graph_generator.base_graph_generator import BaseGraphGenerator
from graph_generator.graph_class import GraphObject
from repository.database import DBSession
from repository.graphs.models import GraphEdgeType, GraphNodeType
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
        super().__init__(Bridge.RONIN)

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

    def fetch_cctx_id(self, cctx: RoninCrossChainTransaction):
        # For Ronin, we can directly use the cctx_id from the database as the unique identifier for the cross-chain transaction
        return cctx.deposit_id

    def parse_bridge_router_event(self, event, routing_node, graph_obj: GraphObject):
        if (
            event["topics"][0] == "0xd7b25068d9dc8d00765254cfb7f5070f98d263c8d68931d937c7362fa738048b"
        ): # DepositRequested
            self.parse_deposit_requested_event(event, routing_node, graph_obj)
        elif (
            event["topics"][0] == "0x8d20d8121a34dded9035ff5b43e901c142824f7a22126392992c353c37890524"
        ): # Deposited
            self.parse_token_deposited_event(event, routing_node, graph_obj)
        elif (
            event["topics"][0] == "0xf313c253a5be72c29d0deb2c8768a9543744ac03d6b3cafd50cc976f1c2632fc"
        ): # WithdrawRequested
            self.parse_withdraw_requested_event(event, routing_node, graph_obj)
        elif (
            event["topics"][0] == "0x21e88e956aa3e086f6388e899965cef814688f99ad8bb29b08d396571016372d"
        ): # Withdrew
            self.parse_token_withdrew_event(event, routing_node, graph_obj)
        elif event:
            #? What to do if the event is not one of the above? For now we will ignore it
            # We can still create a log event node and link it to the routing node
            event_signature = None
            event_args = None
            log_event_node = graph_obj.create_log_node(
                event["topics"][0],
                event_signature,
                event
            )
            graph_obj.create_edge(routing_node.node_id, log_event_node.node_id, GraphEdgeType.LOG_RELATION.value)

    def parse_deposit_requested_event(self, event, routing_node, graph_obj: GraphObject):
        event_signature = "event DepositRequested(bytes32 receiptHash, tuple receipt)"
        # Fetch the respective metadata from the repository
        event_record = self.deposit_requested_repo.fetch_by_transaction_hash(graph_obj.graph_mapping.tx_hash)
        if event_record is None:
            # Log error to error.log #TODO TODO
            pass

        event_args = {
            "receipt": {
                "deposit_id": event_record.deposit_id,
                "kind": event_record.kind,
                "amount": int(event_record.amount),
                "depositor": event_record.depositor,
                "input_token": event_record.input_token,
                "destination_chain": event_record.dst_blockchain,
                "recipient": event_record.recipient,
                "output_token": event_record.output_token,
            }
        }

        # Create and link log event node to the routing node
        log_event_node = graph_obj.create_log_node(
            event["topics"][0],
            event_signature,
            event_args
        )
        graph_obj.create_edge(
            routing_node.node_id, 
            log_event_node.node_id, 
            GraphEdgeType.LOG_RELATION.value
        )

        # Ensure the depositor is a user node
        depositor_node = graph_obj.fetch_or_create_node(
            event_record.depositor,
            node_type_if_missing=GraphNodeType.USER.value
        )
        graph_obj.update_node_type(depositor_node.node_id, GraphNodeType.USER.value)
        graph_obj.create_edge(
            depositor_node.node_id, 
            routing_node.node_id, 
            GraphEdgeType.TRANSACTION.value, 
            attributes={
                "amount": int(event_record.amount)
            }
        )

        # Link the routing node and the token node with a function call edge
        token_node = graph_obj.fetch_or_create_node(
            event_record.input_token,
            node_type_if_missing=GraphNodeType.TOKEN.value
        )
        graph_obj.create_edge(
            routing_node.node_id,
            token_node.node_id,
            GraphEdgeType.FUNCTION_CALL.value
        )

    def parse_token_deposited_event(self, event, routing_node, graph_obj: GraphObject):
        event_signature = "event TokenDeposited(bytes32 receiptHash, tuple receipt)"
        # Fetch the respective metadata from the repository
        event_record = self.token_deposited_repo.fetch_by_transaction_hash(graph_obj.graph_mapping.tx_hash)
        if event_record is None:
            # Log error to error.log
            pass

        event_args = {
            "receipt": {
                "deposit_id": event_record.deposit_id,
                "kind": event_record.kind,
                "amount": int(event_record.amount),
                "depositor": event_record.depositor,
                "input_token": event_record.input_token,
                "source_chain": event_record.src_blockchain,
                "recipient": event_record.recipient,
                "output_token": event_record.output_token,
            }
        }

        # Create and link log event node to the routing node
        log_event_node = graph_obj.create_log_node(
            event["topics"][0],
            event_signature,
            event_args
        )
        graph_obj.create_edge(
            routing_node.node_id, 
            log_event_node.node_id, 
            GraphEdgeType.LOG_RELATION.value
        )

        # Link the routing node and the token node with a function call edge
        token_node = graph_obj.fetch_or_create_node(
            event_record.output_token,
            node_type_if_missing=GraphNodeType.TOKEN.value
        )
        graph_obj.create_edge(
            routing_node.node_id,
            token_node.node_id,
            GraphEdgeType.FUNCTION_CALL.value
        )

        # Ensure the recipient is a user node
        recipient_node = graph_obj.fetch_or_create_node(
            event_record.recipient,
            node_type_if_missing=GraphNodeType.USER.value
        )
        graph_obj.update_node_type(recipient_node.node_id, GraphNodeType.USER.value)
        
        # Handle native tokens
        if graph_obj.graph_mapping.blockchain == "ronin" and event_record.input_token is None:
            # Create the Transfer representation of the native token deposit as well
            graph_obj.create_edge(routing_node.node_id, recipient_node.node_id, GraphEdgeType.TOKEN_TRANSFER.value, attributes={
                "amount": int(event_record.amount), 
                #"timestamp": tx.timestamp
            })

            # Create corresponding log event node for the burning of the native token
            burn_log_event_node = graph_obj.create_log_node(
                "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                "event Transfer(address indexed _from, address indexed _to, uint256 _value)",
                {
                    "from": routing_node.address,
                    "to": recipient_node.address,
                    "value": int(event_record.amount)
                }
            )
            graph_obj.create_edge(token_node.node_id, burn_log_event_node.node_id, GraphEdgeType.LOG_RELATION.value)

    def parse_withdraw_requested_event(self, event, routing_node, graph_obj: GraphObject):
        event_signature = "event WithdrawRequested(bytes32 receiptHash, tuple receipt)"
        # Fetch the respective metadata from the repository
        event_record = self.withdrawal_requested_repo.fetch_by_transaction_hash(graph_obj.graph_mapping.tx_hash)
        if event_record is None:
            # Log error to error.log #TODO TODO
            pass

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

        # Create and link log event node to the routing node
        log_event_node = graph_obj.create_log_node(
            event["topics"][0],
            event_signature,
            event_args
        )
        graph_obj.create_edge(
            routing_node.node_id, 
            log_event_node.node_id, 
            GraphEdgeType.LOG_RELATION.value
        )

        # Ensure the withdrawer is a user node
        withdrawer_node = graph_obj.fetch_or_create_node(
            event_record.withdrawer,
            node_type_if_missing=GraphNodeType.USER.value
        )
        graph_obj.update_node_type(withdrawer_node.node_id, GraphNodeType.USER.value)
        graph_obj.create_edge(
            withdrawer_node.node_id, 
            routing_node.node_id, 
            GraphEdgeType.TRANSACTION.value, 
            attributes={
                "amount": int(event_record.amount)
            }
        )

        # Link the routing node and the token node with a function call edge
        token_node = graph_obj.fetch_or_create_node(
            event_record.input_token,
            node_type_if_missing=GraphNodeType.TOKEN.value
        )
        graph_obj.create_edge(
            routing_node.node_id,
            token_node.node_id,
            GraphEdgeType.FUNCTION_CALL.value
        )

        # Handle native tokens (Wrapped Ethereum)
        if graph_obj.graph_mapping.blockchain == "ronin" and event_record.input_token.lower() is None:
            # Create the Transfer representation of the native token deposit as well
            graph_obj.create_edge(withdrawer_node.node_id, routing_node.node_id, GraphEdgeType.TOKEN_TRANSFER.value, attributes={
                "amount": int(event_record.amount), 
                #"timestamp": tx.timestamp
            })

            # Create corresponding log event node for the burning of the native token
            burn_log_event_node = graph_obj.create_log_node(
                "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                "event Transfer(address indexed _from, address indexed _to, uint256 _value)",
                {
                    "from": event_record.withdrawer,
                    "to": routing_node.address,
                    "value": int(event_record.amount)
                }
            )
            graph_obj.create_edge(token_node.node_id, burn_log_event_node.node_id, GraphEdgeType.LOG_RELATION.value)

    def parse_token_withdrew_event(self, event, routing_node, graph_obj: GraphObject):
        event_signature = "event TokenWithdrew(bytes32 receiptHash, tuple receipt)"
        # Fetch the respective metadata from the repository
        event_record = self.token_withdrew_repo.fetch_by_transaction_hash(graph_obj.graph_mapping.tx_hash)
        if event_record is None:
            # Log error to error.log #TODO TODO
            pass

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

        # Create and link log event node to the routing node
        log_event_node = graph_obj.create_log_node(
            event["topics"][0],
            event_signature,
            event_args
        )
        graph_obj.create_edge(
            routing_node.node_id, 
            log_event_node.node_id, 
            GraphEdgeType.LOG_RELATION.value
        )

        # Link the routing node and the token node with a function call edge
        token_node = graph_obj.fetch_or_create_node(
            event_record.output_token,
            node_type_if_missing=GraphNodeType.TOKEN.value
        )
        graph_obj.create_edge(
            routing_node.node_id,
            token_node.node_id,
            GraphEdgeType.FUNCTION_CALL.value
        )

        # Ensure the recipient is a user node
        recipient_node = graph_obj.fetch_or_create_node(
            event_record.recipient,
            node_type_if_missing=GraphNodeType.USER.value
        )
        graph_obj.update_node_type(recipient_node.node_id, GraphNodeType.USER.value)
