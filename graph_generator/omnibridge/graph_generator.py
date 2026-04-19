

from config.constants import Bridge
from graph_generator.base_graph_generator import BaseGraphGenerator
from graph_generator.graph_class import GraphObject
from graph_generator.graph_label import EventType, GraphEdgeType, GraphNodeType
from graph_generator.omnibridge.constants import ANOMALY_TRANSACTIONS, OFFCHAIN_ANOMALY_TRANSACTIONS
from repository.database import DBSession
from repository.omnibridge.models import OmnibridgeCrossChainTransactions
from repository.omnibridge.repository import (
    OmnibridgeAffirmationCompletedRepository,
    OmnibridgeBlockchainTransactionRepository,
    OmnibridgeCrossChainTransactionsRepository,
    OmnibridgeRelayedMessageRepository,
    OmnibridgeTokensBridgedRepository,
    OmnibridgeTokensBridgingInitiatedRepository,
    OmnibridgeUserRequestForAffirmationRepository,
    OmnibridgeUserRequestForSignatureRepository,
)


class OmnibridgeGraphGenerator(BaseGraphGenerator):
    def __init__(self):
        self.bridge = Bridge.OMNIBRIDGE
        self.chain_anomaly_transactions = ANOMALY_TRANSACTIONS
        self.offchain_anomaly_transactions = OFFCHAIN_ANOMALY_TRANSACTIONS
        super().__init__(self.bridge)

    def bind_db_to_repos(self) -> None:
        super().bind_db_to_repos()

        self.cross_chain_transactions_repo = OmnibridgeCrossChainTransactionsRepository(DBSession)
        self.blockchain_transactions_repo = OmnibridgeBlockchainTransactionRepository(DBSession)
        self.user_request_affirmation_repo = OmnibridgeUserRequestForAffirmationRepository(DBSession)
        self.user_request_signature_repo = OmnibridgeUserRequestForSignatureRepository(DBSession)
        self.tokens_bridging_initiated_repo = OmnibridgeTokensBridgingInitiatedRepository(DBSession)
        self.tokens_bridged_repo = OmnibridgeTokensBridgedRepository(DBSession)
        self.affirmation_completed_repo = OmnibridgeAffirmationCompletedRepository(DBSession)
        self.relayed_message_repo = OmnibridgeRelayedMessageRepository(DBSession)

    def fetch_transactions_for_blockchain(self, blockchain: str):
        return self.blockchain_transactions_repo.get_transactions_from_blockchain(blockchain)

    def fetch_cross_chain_transactions(self):
        return self.cross_chain_transactions_repo.get_all()
    
    def fetch_transactions_timestamp_interval(self):
        return self.blockchain_transactions_repo.get_min_timestamp(), self.blockchain_transactions_repo.get_max_timestamp()

    def get_router_event_list(self, blockchain):
        if blockchain == "ethereum":
            return "event TokensBridgingInitiated(address token, address sender, uint256 value, bytes32 messageId), " + \
                   "event TokensBridged(address token, address recipient, uint256 value, bytes32 messageId), " + \
                   "event UserRequestForAffirmation(bytes32 messageId, bytes encodedData), " + \
                   "event RelayedMessage(address sender, address executor, bytes32 messageId, bool status), " + \
                   "event RelayedMessage(address recipient, uint256 value, bytes32 nonce)"
        elif blockchain == "gnosis":
            return "event TokensBridgingInitiated(address token, address sender, uint256 value, bytes32 messageId), " + \
                   "event TokensBridged(address token, address recipient, uint256 value, bytes32 messageId), " + \
                   "event UserRequestForSignature(bytes32 messageId, bytes encodedData), " + \
                   "event AffirmationCompleted(address sender, address executor, bytes32 messageId, bool status), " + \
                   "event AffirmationCompleted(address recipient, uint256 value, bytes32 nonce)"

    def fetch_cctx_id(self, cctx: OmnibridgeCrossChainTransactions):
        # For OmniBridge, we can directly use the message_id from the database as the unique identifier for the cross-chain transaction
        return cctx.message_id

    def parse_bridge_router_event(self, tx, event, event_index, routing_node, graph_obj: GraphObject):
        if (
            event["topics"][0] == "0x482515ce3d9494a37ce83f18b72b363449458435fafdd7a53ddea7460fe01b58"
        ): # UserRequestForAffirmation
            self.parse_user_request_for_affirmation(tx, event, event_index, routing_node, graph_obj)
        elif (
            event["topics"][0] == "0x520d2afde79cbd5db58755ac9480f81bc658e5c517fcae7365a3d832590b0183"
        ): # UserRequestForSignature
            self.parse_user_request_for_signature(tx, event, event_index, routing_node, graph_obj)
        elif (
            event["topics"][0] == "0x59a9a8027b9c87b961e254899821c9a276b5efc35d1f7409ea4f291470f1629a"
        ): # TokensBridgingInitiated
            self.parse_tokens_bridging_initiated(tx, event, event_index, routing_node, graph_obj)
        elif (
            event["topics"][0] == "0x9afd47907e25028cdaca89d193518c302bbb128617d5a992c5abd45815526593"
        ): # TokensBridged
            self.parse_tokens_bridged(tx, event, event_index, routing_node, graph_obj)
        elif (
            event["topics"][0] == "0xe194ef610f9150a2db4110b3db5116fd623175dca3528d7ae7046a1042f84fe7"
        ): # AffirmationCompleted (index_topic_1 address sender, index_topic_2 address executor, index_topic_3 bytes32 messageId, bool status)
            self.parse_affirmation_completed(tx, event, event_index, routing_node, graph_obj)
        elif (
            event["topics"][0] == "0x6fc115a803b8703117d9a3956c5a15401cb42401f91630f015eb6b043fa76253"
        ): # AffirmationCompleted (address recipient, uint256 value, bytes32 nonce)
            self.parse_affirmation_completed_recipient(tx, event, event_index, routing_node, graph_obj)
        elif (
            event["topics"][0] == "0x27333edb8bdcd40a0ae944fb121b5e2d62ea782683946654a0f5e607a908d578"
        ): # RelayedMessage (index_topic_1 address sender, index_topic_2 address executor, index_topic_3 bytes32 messageId, bool status)
            self.parse_relayed_message(tx, event, event_index, routing_node, graph_obj)
        elif (
            event["topics"][0] == "(index_topic_1 address sender, index_topic_2 address executor, index_topic_3 bytes32 messageId, bool status)"
        ): # RelayedMessage (address recipient, uint256 value, bytes32 nonce)
            self.parse_relayed_message_recipient(tx, event, event_index, routing_node, graph_obj)
        elif event:
            self.create_unknown_router_event_node(tx, event, event_index, routing_node, graph_obj)

    def parse_user_request_for_affirmation(self, tx, event, event_index, routing_node, graph_obj: GraphObject):
        event_signature = "event UserRequestForAffirmation(bytes32 messageId, bytes encodedData)"
        event_record = self.user_request_affirmation_repo.fetch_by_transaction_hash(graph_obj.graph_mapping.tx_hash)

        event_args = {
            "message_id": event_record.message_id,
            "encoded_data": event_record.encoded_data
        }
        event_text = f"""{event_signature}
bridge = omnibridge
blockchain = {graph_obj.graph_mapping.blockchain}
message_id = {event_record.message_id[:6]}...{event_record.message_id[-4:]}
encoded_data_length = {len(event_record.encoded_data) // 32}
"""
                
        # Create and link log event node (with specific information) to the routing node
        log_event_node = graph_obj.create_log_node(
            event_index,
            event["topics"][0],
            EventType.OPERATION_REQUEST_SIGNING.value,
            event_signature,
            event_args,
            event["data"],
            attributes_text=event_text,
            timestamp=tx.timestamp
        )
        graph_obj.create_edge(
            routing_node.node_id, 
            log_event_node.node_id, 
            GraphEdgeType.LOG_RELATION.value,
            event_index
        )

    def parse_user_request_for_signature(self, tx, event, event_index, routing_node, graph_obj: GraphObject):
        event_signature = "event UserRequestForSignature(bytes32 messageId, bytes encodedData)"
        event_record = self.user_request_signature_repo.fetch_by_transaction_hash(graph_obj.graph_mapping.tx_hash)

        event_args = {
            "message_id": event_record.message_id,
            "encoded_data": event_record.encoded_data
        }
        event_text = f"""{event_signature}
bridge = omnibridge
blockchain = {graph_obj.graph_mapping.blockchain}
message_id = {event_record.message_id[:6]}...{event_record.message_id[-4:]}
encoded_data_length = {len(event_record.encoded_data) // 32}
"""
        # Create and link log event node (with specific information) to the routing node
        log_event_node = graph_obj.create_log_node(
            event_index,
            event["topics"][0],
            EventType.OPERATION_REQUEST_SIGNING.value,
            event_signature,
            event_args,
            event["data"],
            attributes_text=event_text,
            timestamp=tx.timestamp
        )
        graph_obj.create_edge(
            routing_node.node_id, 
            log_event_node.node_id, 
            GraphEdgeType.LOG_RELATION.value,
            event_index
        )

    def parse_tokens_bridging_initiated(self, tx, event, event_index, routing_node, graph_obj: GraphObject):
        event_signature = "event TokensBridgingInitiated(address token, address sender, uint256 value, bytes32 messageId)"
        event_record = self.tokens_bridging_initiated_repo.fetch_by_transaction_hash(graph_obj.graph_mapping.tx_hash)

        # Get the other routing node responsible for the signature request / affirmation request and 
        # link a function call edge from that node to this routing node
        other_routing_address = (
            "0x4c36d2919e407f0cc2ee3c993ccf8ac26d9ce64e" if graph_obj.graph_mapping.blockchain == "ethereum"
            else "0x75df5af045d91108662d8080fd1fefad6aa0bb59" # gnosis
        )
        other_routing_node = graph_obj.fetch_or_create_node(
            other_routing_address,
            node_type_if_missing=GraphNodeType.ROUTER.value,
            timestamp=tx.timestamp
        )
        graph_obj.create_edge(
            other_routing_node.node_id,
            routing_node.node_id,
            GraphEdgeType.FUNCTION_CALL.value,
            event_index
        )

        # Ensure the sender is a user node
        sender_node = graph_obj.fetch_or_create_node(
            event_record.sender,
            node_type_if_missing=GraphNodeType.USER.value,
            timestamp=tx.timestamp
        )
        graph_obj.update_node_type(sender_node.node_id, GraphNodeType.USER.value)
        graph_obj.create_edge(
            sender_node.node_id, 
            routing_node.node_id, 
            GraphEdgeType.TRANSACTION.value,
            event_index,
            attributes={
                "amount": int(event_record.value)
            }
        )

        # Link the routing node and the token node with a function call edge
        token_node = graph_obj.fetch_or_create_token_node(
            event_record.token,
            timestamp=tx.timestamp
        )
        graph_obj.create_edge(
            routing_node.node_id,
            token_node.node_id,
            GraphEdgeType.FUNCTION_CALL.value,
            event_index
        )

        event_args = {
            "token": event_record.token,
            "sender": event_record.sender,
            "amount": int(event_record.value),
            "message_id": event_record.message_id
        }
        token_metadata = self.load_token_metadata(event_record.token, graph_obj.graph_mapping.blockchain)

        if token_metadata is not None:
            amount, amount_usd = self.convert_token_value_to_amount(tx.timestamp, token_metadata, event_record.value)
        else:
            amount, amount_usd = None, None
        event_text = f"""{event_signature}
bridge = omnibridge
blockchain = {graph_obj.graph_mapping.blockchain}
message_id = {event_record.message_id[:6]}...{event_record.message_id[-4:]}
sender = {sender_node.node_type} ({sender_node.address[:6]}...{sender_node.address[-4:]})
token ={f" {token_metadata.name} ({token_metadata.symbol}) at" if token_metadata else ""} {event_record.token[:6]}...{event_record.token[-4:]}
{f"amount = {amount} {token_metadata.symbol}" if token_metadata else f"amount = {int(event_record.value)}"}
"""

        # Create and link log event node to the routing node
        log_event_node = graph_obj.create_log_node(
            event_index,
            event["topics"][0],
            EventType.DEPOSIT_REQUEST.value if tx.blockchain == "ethereum" else EventType.WITHDRAWAL_REQUEST.value,
            event_signature,
            event_args,
            None,
            attributes_text=event_text,
            amount=int(event_record.value),
            amount_usd=amount_usd,
            token_symbol=token_metadata.symbol if token_metadata else None,
            timestamp=tx.timestamp
        )
        graph_obj.create_edge(
            routing_node.node_id, 
            log_event_node.node_id, 
            GraphEdgeType.LOG_RELATION.value,
            event_index
        )

    def parse_tokens_bridged(self, tx, event, event_index, routing_node, graph_obj: GraphObject):
        event_signature = "event TokensBridged(address token, address recipient, uint256 value, bytes32 messageId)"
        event_record = self.tokens_bridged_repo.fetch_by_transaction_hash(graph_obj.graph_mapping.tx_hash)
        if event_record is None:
            # Log error to error.log
            pass

        # Ensure the recipient is a user node
        recipient_node = graph_obj.fetch_or_create_node(
            event_record.recipient,
            node_type_if_missing=GraphNodeType.USER.value,
            timestamp=tx.timestamp
        )
        graph_obj.update_node_type(recipient_node.node_id, GraphNodeType.USER.value)

        # Link the routing node and the token node with a function call edge
        token_node = graph_obj.fetch_or_create_token_node(
            event_record.token,
            timestamp=tx.timestamp
        )
        graph_obj.create_edge(
            routing_node.node_id,
            token_node.node_id,
            GraphEdgeType.FUNCTION_CALL.value,
            event_index
        )

        event_args = {
            "token": event_record.token,
            "recipient": event_record.recipient,
            "amount": int(event_record.value),
            "message_id": event_record.message_id
        }
        token_metadata = self.load_token_metadata(event_record.token, graph_obj.graph_mapping.blockchain)

        if token_metadata is not None:
            amount, amount_usd = self.convert_token_value_to_amount(tx.timestamp, token_metadata, event_record.value)
        else:
            amount, amount_usd = None, None
        event_text = f"""{event_signature}
bridge = omnibridge
blockchain = {graph_obj.graph_mapping.blockchain}
message_id = {event_record.message_id[:6]}...{event_record.message_id[-4:]}
recipient = {recipient_node.node_type} ({recipient_node.address[:6]}...{recipient_node.address[-4:]})
token ={f" {token_metadata.name} ({token_metadata.symbol}) at" if token_metadata else ""} {event_record.token[:6]}...{event_record.token[-4:]}
{f"amount = {amount} {token_metadata.symbol}" if token_metadata else f"amount = {int(event_record.value)}"}
"""
        
        # Create and link log event node to the routing node
        log_event_node = graph_obj.create_log_node(
            event_index,
            event["topics"][0],
            EventType.DEPOSIT_CONFIRMATION.value if tx.blockchain == "gnosis" else EventType.WITHDRAWAL_CONFIRMATION.value,
            event_signature,
            event_args,
            None,
            attributes_text=event_text,
            amount=int(event_record.value),
            amount_usd=amount_usd,
            token_symbol=token_metadata.symbol if token_metadata else None,
            timestamp=tx.timestamp
        )
        graph_obj.create_edge(
            routing_node.node_id, 
            log_event_node.node_id, 
            GraphEdgeType.LOG_RELATION.value,
            event_index
        )

    def parse_affirmation_completed_recipient(self, tx, event, event_index, routing_node, graph_obj: GraphObject):
        event_signature = "event AffirmationCompleted(address recipient, uint256 value, bytes32 nonce)"
        event_record = self.affirmation_completed_repo.fetch_by_transaction_hash(graph_obj.graph_mapping.tx_hash)

        # Ensure the recipient is a user node
        recipient_node = graph_obj.fetch_or_create_node(
            event_record.recipient,
            node_type_if_missing=GraphNodeType.USER.value,
            timestamp=tx.timestamp
        )
        graph_obj.update_node_type(recipient_node.node_id, GraphNodeType.USER.value)
        
        # Ensure there is a token DAO node and link it to the routing node with a function call edge
        token_dao_address = "0x97630e2ae609d4104abda91f3066c556403182dd" # Hardcoded
        token_dao_node = graph_obj.fetch_or_create_node(
            token_dao_address,
            node_type_if_missing=GraphNodeType.TOKEN.value,
            timestamp=tx.timestamp
        )
        graph_obj.create_edge(
            routing_node.node_id,
            token_dao_node.node_id,
            GraphEdgeType.FUNCTION_CALL.value,
            event_index
        )

        event_args = {
            "token": token_dao_address,
            "recipient": event_record.recipient,
            "amount": int(event_record.value),
            "nonce": event_record.nonce
        }
        token_metadata = self.load_token_metadata(token_dao_address, graph_obj.graph_mapping.blockchain)

        if token_metadata is not None:
            amount, amount_usd = self.convert_token_value_to_amount(tx.timestamp, token_metadata, event_record.value)
        else:
            amount, amount_usd = None, None
        event_text = f"""{event_signature}
bridge = omnibridge
blockchain = {graph_obj.graph_mapping.blockchain}
nonce = {event_record.nonce[:6]}...{event_record.nonce[-4:]}
recipient = {recipient_node.node_type} ({recipient_node.address[:6]}...{recipient_node.address[-4:]})
token ={f" {token_metadata.name} ({token_metadata.symbol}) at" if token_metadata else ""} {token_dao_address[:6]}...{token_dao_address[-4:]}
{f"amount = {amount} {token_metadata.symbol}" if token_metadata else f"amount = {int(event_record.value)}"}
"""
        # Create and link log event node to the routing node
        log_event_node = graph_obj.create_log_node(
            event_index,
            event["topics"][0],
            EventType.DEPOSIT_CONFIRMATION.value,
            event_signature,
            event_args,
            event["data"],
            attributes_text=event_text,
            amount=int(event_record.value),
            amount_usd=amount_usd,
            token_symbol=token_metadata.symbol if token_metadata else None,
            timestamp=tx.timestamp
        )
        graph_obj.create_edge(
            routing_node.node_id, 
            log_event_node.node_id, 
            GraphEdgeType.LOG_RELATION.value,
            event_index
        )

    def parse_affirmation_completed(self, tx, event, event_index, routing_node, graph_obj: GraphObject):
        event_signature = "event AffirmationCompleted(address sender, address executor, bytes32 messageId, bool status)"
        event_record = self.affirmation_completed_repo.fetch_by_transaction_hash(graph_obj.graph_mapping.tx_hash)

        # Link the executor node to this routing node with a function call edge
        executor_node = graph_obj.fetch_or_create_node(
            event_record.executor,
            node_type_if_missing=GraphNodeType.OTHER_ACCOUNT.value,
            timestamp=tx.timestamp
        )
        graph_obj.create_edge(
            executor_node.node_id,
            routing_node.node_id,
            GraphEdgeType.FUNCTION_CALL.value,
            event_index
        )

        event_args = {
            "sender": event_record.sender,
            "executor": event_record.executor,
            "message_id": event_record.message_id
        }
        event_text = f"""{event_signature}
bridge = omnibridge
blockchain = {graph_obj.graph_mapping.blockchain}
message_id = {event_record.message_id[:6]}...{event_record.message_id[-4:]}
sender = {event_record.sender[:6]}...{event_record.sender[-4:]}
executor = {executor_node.node_type} ({executor_node.address[:6]}...{executor_node.address[-4:]})
"""
        
        # Create and link log event node to the routing node
        log_event_node = graph_obj.create_log_node(
            event_index,
            event["topics"][0],
            EventType.OPERATION_FINALIZED.value,
            event_signature,
            event_args,
            event["data"],
            attributes_text=event_text,
            timestamp=tx.timestamp
        )
        graph_obj.create_edge(
            routing_node.node_id, 
            log_event_node.node_id, 
            GraphEdgeType.LOG_RELATION.value,
            event_index
        )

    def parse_relayed_message_recipient(self, tx, event, event_index, routing_node, graph_obj: GraphObject):
        event_signature = "event RelayedMessage(address recipient, uint256 value, bytes32 nonce)"
        event_record = self.relayed_message_repo.fetch_by_transaction_hash(graph_obj.graph_mapping.tx_hash)

        # Ensure the recipient is a user node
        recipient_node = graph_obj.fetch_or_create_node(
            event_record.recipient,
            node_type_if_missing=GraphNodeType.USER.value,
            timestamp=tx.timestamp
        )
        graph_obj.update_node_type(recipient_node.node_id, GraphNodeType.USER.value)

        # Ensure there is a DAI stablecoin node and link it to the routing node with a function call edge
        dai_token_address = "0x6b175474e89094c44da98b954eedeac495271d0f" # Hardcoded
        dai_token_node = graph_obj.fetch_or_create_token_node(
            dai_token_address,
            node_type_if_missing=GraphNodeType.TOKEN.value,
            timestamp=tx.timestamp
        )
        graph_obj.create_edge(
            routing_node.node_id,
            dai_token_node.node_id,
            GraphEdgeType.FUNCTION_CALL.value,
            event_index
        )

        event_args = {
            "token": dai_token_address,
            "recipient": event_record.recipient,
            "amount": int(event_record.value),
            "nonce": event_record.nonce
        }
        token_metadata = self.load_token_metadata(dai_token_address, graph_obj.graph_mapping.blockchain)
        if token_metadata is not None:
            amount, amount_usd = self.convert_token_value_to_amount(tx.timestamp, token_metadata, event_record.value)
        else:
            amount, amount_usd = None, None
        event_text = f"""{event_signature}
bridge = omnibridge
blockchain = {graph_obj.graph_mapping.blockchain}
nonce = {event_record.nonce[:6]}...{event_record.nonce[-4:]}
recipient = {recipient_node.node_type} ({recipient_node.address[:6]}...{recipient_node.address[-4:]})
token ={f" {token_metadata.name} ({token_metadata.symbol}) at" if token_metadata else ""} {dai_token_address[:6]}...{dai_token_address[-4:]}
{f"amount = {amount} {token_metadata.symbol}" if token_metadata else f"amount = {int(event_record.value)}"}
"""
        # Create and link log event node to the routing node
        log_event_node = graph_obj.create_log_node(
            event_index,
            event["topics"][0],
            EventType.WITHDRAWAL_CONFIRMATION.value,
            event_signature,
            event_args,
            event["data"],
            attributes_text=event_text,
            amount=int(event_record.value),
            amount_usd=amount_usd,
            token_symbol=token_metadata.symbol if token_metadata else None,
            timestamp=tx.timestamp
        )
        graph_obj.create_edge(
            routing_node.node_id, 
            log_event_node.node_id, 
            GraphEdgeType.LOG_RELATION.value,
            event_index
        )


    def parse_relayed_message(self, tx, event, event_index, routing_node, graph_obj: GraphObject):
        event_signature = "event RelayedMessage(address sender, address executor, bytes32 messageId, bool status)"
        event_record = self.relayed_message_repo.fetch_by_transaction_hash(graph_obj.graph_mapping.tx_hash)

        # Link the executor node to this routing node with a function call edge
        executor_node = graph_obj.fetch_or_create_node(
            event_record.executor,
            node_type_if_missing=GraphNodeType.OTHER_ACCOUNT.value,
            timestamp=tx.timestamp
        )
        graph_obj.create_edge(
            executor_node.node_id,
            routing_node.node_id,
            GraphEdgeType.FUNCTION_CALL.value,
            event_index
        )

        event_args = {
            "sender": event_record.sender,
            "executor": event_record.executor,
            "message_id": event_record.message_id
        }
        event_text = f"""{event_signature}
bridge = omnibridge
blockchain = {graph_obj.graph_mapping.blockchain}
message_id = {event_record.message_id[:6]}...{event_record.message_id[-4:]}
sender = {event_record.sender[:6]}...{event_record.sender[-4:]}
executor = {executor_node.node_type} ({executor_node.address[:6]}...{executor_node.address[-4:]})
"""
        # Create and link log event node to the routing node
        log_event_node = graph_obj.create_log_node(
            event_index,
            event["topics"][0],
            EventType.OPERATION_FINALIZED.value,
            event_signature,
            event_args,
            event["data"],
            attributes_text=event_text,
            timestamp=tx.timestamp
        )
        graph_obj.create_edge(
            routing_node.node_id, 
            log_event_node.node_id, 
            GraphEdgeType.LOG_RELATION.value,
            event_index
        )