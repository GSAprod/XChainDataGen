from config.constants import Bridge
from graph_generator.base_graph_generator import BaseGraphGenerator
from graph_generator.graph_class import GraphObject
from graph_generator.graph_label import EventType, GraphEdgeType, GraphNodeType
from repository.database import DBSession
from repository.nomad.models import NomadCrossChainTransaction
from repository.nomad.repository import (
    NomadBlockchainTransactionRepository,
    NomadCrossChainTransactionRepository,
    NomadEthHelperSendRepository,
    NomadHomeDispatchRepository,
    NomadReplicaProcessRepository,
    NomadRouterReceiveRepository,
    NomadRouterSendRepository,
)


class NomadGraphGenerator(BaseGraphGenerator):
    def __init__(self):
        self.bridge = Bridge.NOMAD
        super().__init__(self.bridge)

    def bind_db_to_repos(self) -> None:
        super().bind_db_to_repos()

        self.cross_chain_transactions_repo = NomadCrossChainTransactionRepository(DBSession)
        self.blockchain_transactions_repo = NomadBlockchainTransactionRepository(DBSession)
        self.router_send_repo = NomadRouterSendRepository(DBSession)
        self.router_receive_repo = NomadRouterReceiveRepository(DBSession)
        self.eth_helper_send_repo = NomadEthHelperSendRepository(DBSession)
        self.replica_process_repo = NomadReplicaProcessRepository(DBSession)
        self.home_dispatch_repo = NomadHomeDispatchRepository(DBSession)

    def fetch_transactions_for_blockchain(self, blockchain: str, start_ts: int = None, end_ts: int = None):
        return self.blockchain_transactions_repo.get_transactions_from_blockchain(blockchain, start_ts, end_ts)

    def fetch_cross_chain_transactions(self):
        return self.cross_chain_transactions_repo.get_all()

    def fetch_transactions_timestamp_interval(self):
        return (
            self.blockchain_transactions_repo.get_min_timestamp(),
            self.blockchain_transactions_repo.get_max_timestamp(),
        )

    def fetch_cctx_id(self, cctx: NomadCrossChainTransaction) -> str:
        return cctx.deposit_hash

    def get_router_event_list(self, _):
        return (
            "event Send(address token, address from, uint32 toDomain, bytes32 toId, uint256 amount, bool fastLiquidityEnabled), "
            "event Receive(uint32 originAndNonce, address token, address recipient, address liquidityProvider, uint256 amount), "
            "event Send(address indexed from), "
            "event Dispatch(bytes32 messageHash, uint256 leafIndex, uint64 destinationAndNonce, bytes32 committedRoot, bytes message), "
            "event Process(bytes32 indexed messageHash, bool indexed success, bytes indexed returnData)"
        )
    
    #override
    def resolve_node_address(self, address, blockchain):
        if self.bridge_router_metadata_repo.get_bridge_routing_metadata_by_address_and_blockchain(
            self.bridge.value,
            address,
            blockchain
        ):
            return "__NOMAD_BRIDGE_ROUTER__"
        return address

    def parse_bridge_router_event(self, tx, event, event_index, routing_node, graph_obj: GraphObject):
        topic = event["topics"][0]
        if (
            topic == "0xa3d219cf126a12be40d7ad1ceef46231c987988dd4e686457b610e1b6b80a4bf"
        ):  # Send (BridgeRouter)
            self.parse_router_send_event(tx, event, event_index, routing_node, graph_obj)
        elif (
            topic == "0x9f9a97db84f39202ca3b409b63f7ccf7d3fd810e176573c7483088b6f181bbbb"
        ):  # Receive (BridgeRouter)
            self.parse_router_receive_event(tx, event, event_index, routing_node, graph_obj)
        elif (
            topic == "0x7d4b3c5c44bd8008199bb99f184426274cf24f917f4da3485d6a39f894366b10"
        ):  # Send (ETHHelper)
            self.parse_eth_helper_send_event(tx, event, event_index, routing_node, graph_obj)
        elif (
            topic == "0x9d4c83d2e57d7d381feb264b44a5015e7f9ef26340f4fc46b558a6dc16dd811a"
        ):  # Dispatch (Home)
            self.parse_home_dispatch_event(tx, event, event_index, routing_node, graph_obj)
        elif (
            topic == "0xd42de95a9b26f1be134c8ecce389dc4fcfa18753d01661b7b361233569e8fe48"
        ):  # Process (Replica)
            self.parse_replica_process_event(tx, event, event_index, routing_node, graph_obj)
        elif event:
            self.create_unknown_router_event_node(tx, event, event_index, routing_node, graph_obj)

    def parse_router_send_event(self, tx, event, event_index, routing_node, graph_obj: GraphObject):
        event_signature = "event Send(address token, address from, uint32 toDomain, bytes32 toId, uint256 amount, bool fastLiquidityEnabled)"
        # Fetch the respective metadata from the repository
        if len(event["data"]) < 66: # Ensure there is a recipient field in the data
            return
        event_record = self.router_send_repo.fetch_by_transaction_hash_token_depositor_recipient(
            graph_obj.graph_mapping.blockchain,
            graph_obj.graph_mapping.tx_hash,
            "0x" + event["topics"][1][-40:], # token (use the last 40 characters of the topic to get the address)
            "0x" + event["topics"][2][-40:], # depositor (use the last 40 characters of the topic to get the address)
            "0x" + event["data"][26:66], # recipient
        )
        if event_record is None:
            return

        value = int(event_record.amount)
        if value is not None and value > 10e27:
            value = 10e27

        # Ensure the depositor is a user node
        #* Note: The depositor can be the ETHHelper contract in some cases.
        #* This happens when users deposit native token funds.
        #* If that is the case, the fact that we condensed the two routing addresses into one
        #* means that there shouldn't be a transaction edge between them, and 
        #* thus the Transaction edge will only be created on the ETHHelper's Send event.
        depositor_node = graph_obj.fetch_or_create_node(
            event_record.depositor,
            node_type_if_missing=GraphNodeType.USER.value,
            timestamp=tx.timestamp,
        )
        if depositor_node.address == routing_node.address:
            graph_obj.update_node_type(depositor_node.node_id, GraphNodeType.ROUTER.value)
        else:
            graph_obj.update_node_type(depositor_node.node_id, GraphNodeType.USER.value)
            graph_obj.create_edge(
                depositor_node.node_id,
                routing_node.node_id,
                GraphEdgeType.TRANSACTION.value,
                event_index,
                attributes={"amount": int(value)},
            )

        # Link the routing node and the token node with a function call edge
        token_node = graph_obj.fetch_or_create_token_node(event_record.input_token, timestamp=tx.timestamp)
        graph_obj.create_edge(routing_node.node_id, token_node.node_id, GraphEdgeType.FUNCTION_CALL.value, event_index)

        input_token_metadata = self.inspector.ensure_metadata(event_record.input_token, graph_obj.graph_mapping.blockchain)
        if input_token_metadata is not None:
            _, amount_usd = self.pricing.resolve_token_amount(input_token_metadata, value, tx.timestamp)
        else:
            amount_usd = None

        log_event_node = graph_obj.create_log_node(
            event_index,
            event["topics"][0],
            EventType.DEPOSIT_REQUEST.value,
            event_signature,
            {
                "token": event_record.input_token,
                "from": event_record.depositor,
                "toDomain": event_record.dst_blockchain,
                "toId": event_record.recipient,
                "amount": int(value),
                "fastLiquidityEnabled": bool(event_record.fast_liquidity_enabled),
            },
            None,
            amount=int(value),
            amount_usd=amount_usd,
            token_symbol=input_token_metadata.symbol if input_token_metadata else None,
            timestamp=tx.timestamp,
        )
        if input_token_metadata and amount_usd is None:
            self.pricing.record_missing_price(log_event_node.node_id, input_token_metadata.symbol, tx.timestamp, int(value))
        graph_obj.create_edge(routing_node.node_id, log_event_node.node_id, GraphEdgeType.LOG_RELATION.value, event_index)

    def parse_router_receive_event(self, tx, event, event_index, routing_node, graph_obj: GraphObject):
        event_signature = "event Receive(uint32 originAndNonce, address token, address recipient, address liquidityProvider, uint256 amount)"
        event_record = self.router_receive_repo.fetch_by_transaction_hash_token_recipient(
            graph_obj.graph_mapping.blockchain,
            graph_obj.graph_mapping.tx_hash,
            "0x" + event["topics"][2][-40:], # token (use the last 40 characters of the topic to get the address)
            "0x" + event["topics"][3][-40:]  # recipient
        )
        if event_record is None:
            return

        value = int(event_record.amount)
        if value is not None and value > 10e27:
            value = 10e27

        recipient_node = graph_obj.fetch_or_create_node(
            event_record.recipient,
            node_type_if_missing=GraphNodeType.USER.value,
            timestamp=tx.timestamp,
        )
        graph_obj.update_node_type(recipient_node.node_id, GraphNodeType.USER.value)

        # Link the routing node and the token node with a function call edge
        token_node = graph_obj.fetch_or_create_token_node(event_record.output_token, timestamp=tx.timestamp)
        graph_obj.create_edge(routing_node.node_id, token_node.node_id, GraphEdgeType.FUNCTION_CALL.value, event_index)

        output_token_metadata = self.inspector.ensure_metadata(event_record.output_token, graph_obj.graph_mapping.blockchain)
        if output_token_metadata is not None:
            _, amount_usd = self.pricing.resolve_token_amount(output_token_metadata, value, tx.timestamp)
        else:
            amount_usd = None

        log_event_node = graph_obj.create_log_node(
            event_index,
            event["topics"][0],
            EventType.DEPOSIT_CONFIRMATION.value,
            event_signature,
            {
                "originAndNonce": event_record.nonce,
                "token": event_record.output_token,
                "recipient": event_record.recipient,
                "liquidityProvider": event_record.liquidity_provider,
                "amount": int(value),
            },
            None,
            amount=int(value),
            amount_usd=amount_usd,
            token_symbol=output_token_metadata.symbol if output_token_metadata else None,
            timestamp=tx.timestamp,
        )
        if output_token_metadata and amount_usd is None:
            self.pricing.record_missing_price(log_event_node.node_id, output_token_metadata.symbol, tx.timestamp, int(value))
        graph_obj.create_edge(routing_node.node_id, log_event_node.node_id, GraphEdgeType.LOG_RELATION.value, event_index)

    def parse_eth_helper_send_event(self, tx, event, event_index, routing_node, graph_obj: GraphObject):
        event_signature = "event Send(address indexed from)"
        event_record = self.eth_helper_send_repo.fetch_by_transaction_hash_from(
            graph_obj.graph_mapping.blockchain,
            graph_obj.graph_mapping.tx_hash,
            "0x" + event["topics"][1][-40:]  # from (use the last 40 characters of the topic to get the address)
        )
        if event_record is None:
            return

        # Ensure the sender is a user node and link it to the routing node with a transaction edge
        from_node = graph_obj.fetch_or_create_node(
            event_record.from_address,
            node_type_if_missing=GraphNodeType.USER.value,
            timestamp=tx.timestamp,
        )
        graph_obj.update_node_type(from_node.node_id, GraphNodeType.USER.value)
        graph_obj.create_edge(
            from_node.node_id,
            routing_node.node_id,
            GraphEdgeType.TRANSACTION.value,
            event_index,
        )

        # Because this helper contract is an assistant to the BridgeRouter's Send event,
        # there is no need to create a separate log node for it. Doing so would just
        # duplicate the event already captured by the BridgeRouter's Send event, and
        # generate unnecessary noise in the graph.

    def parse_home_dispatch_event(self, tx, event, event_index, routing_node, graph_obj: GraphObject):
        event_signature = "event Dispatch(bytes32 messageHash, uint256 leafIndex, uint64 destinationAndNonce, bytes32 committedRoot, bytes message)"
        # Fetch the respective metadata from the repository
        event_record = self.home_dispatch_repo.fetch_by_transaction_message(
            graph_obj.graph_mapping.blockchain,
            graph_obj.graph_mapping.tx_hash,
            event["topics"][1][2:]  # messageHash (remove the "0x" prefix from the topic)
        )
        if event_record is None:
            return

        value = int(event_record.amount)
        if value is not None and value > 10e27:
            value = 10e27

        # Obtain the token metadata for the dispatched token to resolve its USD value (if possible)
        token_node = graph_obj.fetch_or_create_token_node(event_record.token_address, timestamp=tx.timestamp)
        graph_obj.update_node_type(token_node.node_id, GraphNodeType.TOKEN.value)
        token_metadata = self.inspector.ensure_metadata(event_record.token_address, event_record.token_blockchain)
        if token_metadata is not None:
            _, amount_usd = self.pricing.resolve_token_amount(token_metadata, value, tx.timestamp)
        else:
            amount_usd = None

        # Create and link log event node to the routing node
        log_event_node = graph_obj.create_log_node(
            event_index,
            event["topics"][0],
            EventType.OPERATION_REQUEST_SIGNING.value,
            event_signature,
            {
                "messageHash": event_record.message_hash,
                "leafIndex": event_record.leaf_index,
                "nonce": event_record.nonce,
                "srcBlockchain": event_record.src_blockchain,
                "dstBlockchain": event_record.dst_blockchain,
                "tokenAddress": event_record.token_address,
                "recipient": event_record.recipient,
                "amount": int(value),
            },
            None,
            amount=int(value),
            amount_usd=amount_usd,
            token_symbol=token_metadata.symbol if token_metadata else None,
            timestamp=tx.timestamp,
        )
        if token_metadata and amount_usd is None:
            self.pricing.record_missing_price(log_event_node.node_id, token_metadata.symbol, tx.timestamp, int(value))
        graph_obj.create_edge(routing_node.node_id, log_event_node.node_id, GraphEdgeType.LOG_RELATION.value, event_index)

    def parse_replica_process_event(self, tx, event, event_index, routing_node, graph_obj: GraphObject):
        event_signature = "event Process(bytes32 indexed messageHash, bool indexed success, bytes indexed returnData)"
        event_record = self.replica_process_repo.fetch_by_transaction_and_message(
            graph_obj.graph_mapping.blockchain,
            graph_obj.graph_mapping.tx_hash,
            event["topics"][1][2:], # messageHash (remove the "0x" prefix from the topic)
        )
        if event_record is None:
            return

        # Create and link log event node to the routing node
        log_event_node = graph_obj.create_log_node(
            event_index,
            event["topics"][0],
            EventType.OPERATION_FINALIZED.value,
            event_signature,
            {"messageHash": event_record.message_hash, "success": bool(event_record.success), "returnData": event_record.return_data},
            None,
            timestamp=tx.timestamp,
        )
        graph_obj.create_edge(routing_node.node_id, log_event_node.node_id, GraphEdgeType.LOG_RELATION.value, event_index)

        # Link Replica to BridgeRouter (BridgeRouter was created first during Receive)
        other_routers = sorted(
            [n for n in graph_obj.nodes if n.node_type == GraphNodeType.ROUTER.value and n.node_id != routing_node.node_id],
            key=lambda n: n.node_id,
        )
        if other_routers:
            bridge_router_node = other_routers[0]
            graph_obj.create_edge(routing_node.node_id, bridge_router_node.node_id, GraphEdgeType.FUNCTION_CALL.value, event_index)
