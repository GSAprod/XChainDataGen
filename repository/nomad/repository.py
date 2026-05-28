from sqlalchemy import Index, func

from repository.base import BaseRepository

from .models import (
    NomadBlockchainTransaction,
    NomadCrossChainTransaction,
    NomadEthHelperSend,
    NomadHomeDispatch,
    NomadReplicaProcess,
    NomadRouterReceive,
    NomadRouterSend,
)


class NomadRouterSendRepository(BaseRepository):
    def __init__(self, session_factory):
        super().__init__(NomadRouterSend, session_factory)

    def event_exists(self, blockchain: str, transaction_hash: str, log_index: int):
        with self.get_session() as session:
            return (
                session.query(NomadRouterSend)
                .filter(NomadRouterSend.blockchain == blockchain)
                .filter(NomadRouterSend.transaction_hash == transaction_hash)
                .filter(NomadRouterSend.log_index == log_index)
                .first()
            )

    def fetch_by_transaction_hash_token_depositor_recipient(self, blockchain: str, transaction_hash: str, input_token: str, depositor: str, recipient: str):
        with self.get_session() as session:
            return (
                session.query(NomadRouterSend)
                .filter(NomadRouterSend.blockchain == blockchain)
                .filter(NomadRouterSend.transaction_hash == transaction_hash)
                .filter(func.lower(NomadRouterSend.input_token) == input_token.lower())
                .filter(func.lower(NomadRouterSend.depositor) == depositor.lower())
                .filter(func.lower(NomadRouterSend.recipient) == recipient.lower())
                .first()
            )
        
class NomadRouterReceiveRepository(BaseRepository):
    def __init__(self, session_factory):
        super().__init__(NomadRouterReceive, session_factory)

    def event_exists(
            self, 
            blockchain: str, 
            transaction_hash: str, 
            log_index: int,
            output_token: str,
            recipient: str
    ):
        with self.get_session() as session:
            return (
                session.query(NomadRouterReceive)
                .filter(NomadRouterReceive.blockchain == blockchain)
                .filter(NomadRouterReceive.transaction_hash == transaction_hash)
                .filter(NomadRouterReceive.log_index == log_index)
                .filter(func.lower(NomadRouterReceive.output_token) == output_token.lower())
                .filter(func.lower(NomadRouterReceive.recipient) == recipient.lower())
                .first()
            )

    def fetch_by_transaction_hash_token_recipient(self, blockchain: str, transaction_hash: str, output_token: str, recipient: str):
        with self.get_session() as session:
            return (
                session.query(NomadRouterReceive)
                .filter(NomadRouterReceive.blockchain == blockchain)
                .filter(NomadRouterReceive.transaction_hash == transaction_hash)
                .filter(func.lower(NomadRouterReceive.output_token) == output_token.lower())
                .filter(func.lower(NomadRouterReceive.recipient) == recipient.lower())
                .first()
            )
        
class NomadEthHelperSendRepository(BaseRepository):
    def __init__(self, session_factory):
        super().__init__(NomadEthHelperSend, session_factory)

    def event_exists(self, blockchain: str, transaction_hash: str, log_index: int):
        with self.get_session() as session:
            return (
                session.query(NomadEthHelperSend)
                .filter(NomadEthHelperSend.blockchain == blockchain)
                .filter(NomadEthHelperSend.transaction_hash == transaction_hash)
                .filter(NomadEthHelperSend.log_index == log_index)
                .first()
            )

    def fetch_by_transaction_hash_from(self, blockchain: str, transaction_hash: str, from_address: str):
        with self.get_session() as session:
            return (
                session.query(NomadEthHelperSend)
                .filter(NomadEthHelperSend.blockchain == blockchain)
                .filter(NomadEthHelperSend.transaction_hash == transaction_hash)
                .filter(func.lower(NomadEthHelperSend.from_address) == from_address.lower())
                .first()
            )
        
class NomadReplicaProcessRepository(BaseRepository):
    def __init__(self, session_factory):
        super().__init__(NomadReplicaProcess, session_factory)
    
    def event_exists(
            self, 
            blockchain: str, 
            transaction_hash: str, 
            log_index: int, 
            message_hash: str
    ):
        with self.get_session() as session:
            return (
                session.query(NomadReplicaProcess)
                .filter(NomadReplicaProcess.blockchain == blockchain)
                .filter(NomadReplicaProcess.transaction_hash == transaction_hash)
                .filter(NomadReplicaProcess.log_index == log_index)
                .filter(NomadReplicaProcess.message_hash == message_hash)
                .first()
            )

    def fetch_by_transaction_and_message(self, blockchain: str, transaction_hash: str, message_hash: str):
        with self.get_session() as session:
            return (
                session.query(NomadReplicaProcess)
                .filter(NomadReplicaProcess.blockchain == blockchain)
                .filter(NomadReplicaProcess.transaction_hash == transaction_hash)
                .filter(NomadReplicaProcess.message_hash == message_hash)
                .first()
            )
        
class NomadHomeDispatchRepository(BaseRepository):
    def __init__(self, session_factory):
        super().__init__(NomadHomeDispatch, session_factory)
    
    def event_exists(self, blockchain: str, transaction_hash: str, log_index: int):
        with self.get_session() as session:
            return (
                session.query(NomadHomeDispatch)
                .filter(NomadHomeDispatch.blockchain == blockchain)
                .filter(NomadHomeDispatch.transaction_hash == transaction_hash)
                .filter(NomadHomeDispatch.log_index == log_index)
                .first()
            )

    def fetch_by_transaction_message(self, blockchain: str, transaction_hash: str, message_hash: str):
        with self.get_session() as session:
            return (
                session.query(NomadHomeDispatch)
                .filter(NomadHomeDispatch.blockchain == blockchain)
                .filter(NomadHomeDispatch.transaction_hash == transaction_hash)
                .filter(NomadHomeDispatch.message_hash == message_hash)
                .first()
            )

class NomadBlockchainTransactionRepository(BaseRepository):
    def __init__(self, session_factory):
        super().__init__(NomadBlockchainTransaction, session_factory)

    def get_transactions_from_blockchain(self, blockchain: str, start_ts: int = None, end_ts: int = None):
        with self.get_session() as session:
            query = session.query(NomadBlockchainTransaction).filter(NomadBlockchainTransaction.blockchain == blockchain)
            if start_ts is not None:
                query = query.filter(NomadBlockchainTransaction.timestamp >= start_ts)
            if end_ts is not None:
                query = query.filter(NomadBlockchainTransaction.timestamp <= end_ts)
            return query.all()

    def get_transaction_by_hash(self, transaction_hash: str):
        with self.get_session() as session:
            return session.get(NomadBlockchainTransaction, transaction_hash)

    def get_min_timestamp(self):
        with self.get_session() as session:
            return session.query(func.min(NomadBlockchainTransaction.timestamp)).scalar()

    def get_max_timestamp(self):
        with self.get_session() as session:
            return session.query(func.max(NomadBlockchainTransaction.timestamp)).scalar()


########## Processed Data ##########

class NomadCrossChainTransactionRepository(BaseRepository):
    def __init__(self, session_factory):
        super().__init__(NomadCrossChainTransaction, session_factory)

    def get_number_of_records(self):
        with self.get_session() as session:
            return session.query(func.count(NomadCrossChainTransaction.id)).scalar()

    def empty_table(self):
        with self.get_session() as session:
            return session.query(NomadCrossChainTransaction).delete()

    def update_amount_usd(self, transaction_hash: str, amount_usd: float):
        with self.get_session() as session:
            session.query(NomadCrossChainTransaction).filter(
                NomadCrossChainTransaction.src_transaction_hash == transaction_hash
            ).update({"amount_usd": amount_usd})

    def get_by_src_tx_hash(self, src_tx_hash: str):
        with self.get_session() as session:
            return (
                session.query(NomadCrossChainTransaction)
                .filter(NomadCrossChainTransaction.src_transaction_hash == src_tx_hash)
                .first()
            )

    def get_unique_src_dst_contract_pairs(self):
        with self.get_session() as session:
            return (
                session.query(
                    NomadCrossChainTransaction.src_blockchain,
                    NomadCrossChainTransaction.src_contract_address,
                    NomadCrossChainTransaction.dst_blockchain,
                    NomadCrossChainTransaction.dst_contract_address,
                )
                .group_by(
                    NomadCrossChainTransaction.src_blockchain,
                    NomadCrossChainTransaction.src_contract_address,
                    NomadCrossChainTransaction.dst_blockchain,
                    NomadCrossChainTransaction.dst_contract_address,
                )
                .all()
            )

    def get_total_amount_usd_transacted(self):
        with self.get_session() as session:
            return session.query(func.sum(NomadCrossChainTransaction.amount_usd)).scalar()

Index("nomad_router_receive_nonce_blockchain_idx",
      NomadRouterReceive.nonce,
      NomadRouterReceive.blockchain,
      NomadRouterReceive.src_blockchain,
)
Index("nomad_replica_process_message_hash_blockchain_idx", 
      NomadReplicaProcess.message_hash, 
      NomadReplicaProcess.blockchain
)
Index("nomad_home_dispatch_message_hash_blockchain_idx", 
      NomadHomeDispatch.message_hash, 
      NomadHomeDispatch.blockchain
)
