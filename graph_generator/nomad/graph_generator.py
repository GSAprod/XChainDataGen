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

    def fetch_transactions_for_blockchain(self, blockchain: str):
        return self.blockchain_transactions_repo.get_transactions_from_blockchain(blockchain)

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
        event_record = self.router_send_repo.fetch_by_transaction_hash(graph_obj.graph_mapping.tx_hash)
        if event_record is None:
            return

        value = int(event_record.amount)
        if value is not None and value > 10e27:
            value = 10e27

        # Ensure the depositor is a user node
        #* Note: The depositor can be the ETHHelper contract in some cases.
        #* If that is the case, the ETHHelper's Send event will convert the
        #* user node back into a router node. However, we don't want to 
        #* create a Transaction edge between the Router and ETHHelper.
        #* A function call edge will be created between the two routers instead
        #* when parsing the ETHHelper's Send event.
        depositor_node = graph_obj.fetch_or_create_node(
            event_record.depositor,
            node_type_if_missing=GraphNodeType.USER.value,
            timestamp=tx.timestamp,
        )
        if event_record.depositor in (
            "0x2d6775c1673d4ce55e1f827a0d53e62c43d1f304",  # Ethereum ETH Helper
            "0xb70588b1a51f847d13158ff18e9cac861df5fb00"   # Moonbeam ETH Helper
        ):
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
        graph_obj.create_edge(routing_node.node_id, log_event_node.node_id, GraphEdgeType.LOG_RELATION.value, event_index)

        # Link BridgeRouter to Home (Home was created first during Dispatch, so it has a lower node_id)
        other_routers = sorted(
            [n for n in graph_obj.nodes if n.node_type == GraphNodeType.ROUTER.value and n.node_id != routing_node.node_id],
            key=lambda n: n.node_id,
        )
        if other_routers:
            home_node = other_routers[0]
            graph_obj.create_edge(routing_node.node_id, home_node.node_id, GraphEdgeType.FUNCTION_CALL.value, event_index)

    def parse_router_receive_event(self, tx, event, event_index, routing_node, graph_obj: GraphObject):
        event_signature = "event Receive(uint32 originAndNonce, address token, address recipient, address liquidityProvider, uint256 amount)"
        event_record = self.router_receive_repo.fetch_by_transaction_hash(graph_obj.graph_mapping.tx_hash)
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
        graph_obj.create_edge(routing_node.node_id, log_event_node.node_id, GraphEdgeType.LOG_RELATION.value, event_index)

    def parse_eth_helper_send_event(self, tx, event, event_index, routing_node, graph_obj: GraphObject):
        event_signature = "event Send(address indexed from)"
        event_record = self.eth_helper_send_repo.fetch_by_transaction_hash(graph_obj.graph_mapping.tx_hash)
        if event_record is None:
            return

        # Ensure the sender is a user node
        from_node = graph_obj.fetch_or_create_node(
            event_record.from_address,
            node_type_if_missing=GraphNodeType.USER.value,
            timestamp=tx.timestamp,
        )
        graph_obj.update_node_type(from_node.node_id, GraphNodeType.USER.value)

        # Create and link log event node to the routing node
        log_event_node = graph_obj.create_log_node(
            event_index,
            event["topics"][0],
            EventType.DEPOSIT_REQUEST.value,
            event_signature,
            {"from": event_record.from_address},
            None,
            timestamp=tx.timestamp,
        )
        graph_obj.create_edge(routing_node.node_id, log_event_node.node_id, GraphEdgeType.LOG_RELATION.value, event_index)

        # Link BridgeRouter to ETHHelper (BridgeRouter is the most recently created router node before ETHHelper)
        other_routers = sorted(
            [n for n in graph_obj.nodes if n.node_type == GraphNodeType.ROUTER.value and n.node_id != routing_node.node_id],
            key=lambda n: n.node_id,
        )
        if other_routers:
            bridge_router_node = other_routers[-1]
            graph_obj.create_edge(bridge_router_node.node_id, routing_node.node_id, GraphEdgeType.FUNCTION_CALL.value, event_index)

    def parse_home_dispatch_event(self, tx, event, event_index, routing_node, graph_obj: GraphObject):
        event_signature = "event Dispatch(bytes32 messageHash, uint256 leafIndex, uint64 destinationAndNonce, bytes32 committedRoot, bytes message)"
        # Fetch the respective metadata from the repository
        event_record = self.home_dispatch_repo.fetch_by_transaction_hash(graph_obj.graph_mapping.tx_hash)
        if event_record is None:
            return

        value = int(event_record.amount)
        if value is not None and value > 10e27:
            value = 10e27

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
                "recipient": event_record.recipient,
                "amount": int(value),
            },
            None,
            amount=int(value),
            timestamp=tx.timestamp,
        )
        graph_obj.create_edge(routing_node.node_id, log_event_node.node_id, GraphEdgeType.LOG_RELATION.value, event_index)
        # BridgeRouter node does not exist yet (fires after Dispatch), so no inter-router link here.

    def parse_replica_process_event(self, tx, event, event_index, routing_node, graph_obj: GraphObject):
        event_signature = "event Process(bytes32 indexed messageHash, bool indexed success, bytes indexed returnData)"
        event_record = self.replica_process_repo.fetch_by_transaction_hash(graph_obj.graph_mapping.tx_hash)
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
