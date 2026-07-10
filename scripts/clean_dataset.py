import os
import sys

from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

from sqlalchemy import create_engine, select  # noqa: E402
from sqlalchemy.orm import sessionmaker  # noqa: E402

from repository.graphs.models import (  # noqa: E402
    GraphEdge,
    GraphMappingBlockchain,
    GraphNode,
)
from repository.graphs.repository import (  # noqa: E402
    GraphEdgeRepository,
    GraphMappingBlockchainRepository,
    GraphMappingCrossChainRepository,
    GraphNodeRepository,
)
from repository.nomad.models import NomadRouterReceive  # noqa: E402
from repository.polynetwork.models import (  # noqa: E402
    PolynetworkVerifyHeaderAndExecuteTxEvent,
)
from repository.ronin.models import RoninTokenWithdrew  # noqa: E402


def connect_to_db():
    DATABASE_URL = os.getenv("DATABASE_URL")
    engine = create_engine(DATABASE_URL)
    return sessionmaker(bind=engine)

class CleanDataset:
    def __init__(self):
        self.session = connect_to_db()
        self.load_base_repos()

    def load_base_repos(self):
        self.cctx_graph_repo = GraphMappingCrossChainRepository(self.session)
        self.chain_graph_repo = GraphMappingBlockchainRepository(self.session)
        self.graph_nodes_repo = GraphNodeRepository(self.session)
        self.graph_edges_repo = GraphEdgeRepository(self.session)

    def discard_associated_nodes_and_edges(self, session, graph_ids):
        session.query(GraphNode).filter(GraphNode.chain_graph_id.in_(graph_ids)).update(
            {GraphNode.discard_flag: 1}, synchronize_session=False
        )
        session.query(GraphEdge).filter(GraphEdge.chain_graph_id.in_(graph_ids)).update(
            {GraphEdge.discard_flag: 1}, synchronize_session=False
        )

    def clean_ronin_unlinked_destination_graphs(self):
        session = self.session()
        try:
            graph_ids_subquery = (
                select(GraphMappingBlockchain.graph_id)
                .join(
                    RoninTokenWithdrew,
                    RoninTokenWithdrew.transaction_hash == GraphMappingBlockchain.tx_hash,
                )
                .where(
                    GraphMappingBlockchain.cctx_graph_id.is_(None),
                    GraphMappingBlockchain.bridge == "ronin",
                    GraphMappingBlockchain.label != "anomaly",
                )
            )
            graph_ids = [row[0] for row in session.execute(graph_ids_subquery).fetchall()]

            updated_count = (
                session.query(GraphMappingBlockchain)
                .filter(GraphMappingBlockchain.graph_id.in_(graph_ids))
                .update({GraphMappingBlockchain.discard_flag: 1}, synchronize_session=False)
            )
            self.discard_associated_nodes_and_edges(session, graph_ids)
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

        return updated_count

    def clean_nomad_unlinked_destination_graphs(self):
        session = self.session()
        try:
            graph_ids_subquery = (
                select(GraphMappingBlockchain.graph_id)
                .join(
                    NomadRouterReceive,
                    NomadRouterReceive.transaction_hash == GraphMappingBlockchain.tx_hash,
                )
                .where(
                    GraphMappingBlockchain.cctx_graph_id.is_(None),
                    GraphMappingBlockchain.bridge == "nomad",
                    GraphMappingBlockchain.label != "anomaly",
                    NomadRouterReceive.src_blockchain.notin_(["ethereum", "moonbeam"]),
                )
            )
            graph_ids = [row[0] for row in session.execute(graph_ids_subquery).fetchall()]

            updated_count = (
                session.query(GraphMappingBlockchain)
                .filter(GraphMappingBlockchain.graph_id.in_(graph_ids))
                .update({GraphMappingBlockchain.discard_flag: 1}, synchronize_session=False)
            )
            self.discard_associated_nodes_and_edges(session, graph_ids)
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

        return updated_count

    def clear_polynetwork_unlinked_destination_graphs(self):
        session = self.session()
        try:
            graph_ids_subquery = (
                select(GraphMappingBlockchain.graph_id)
                .join(
                    PolynetworkVerifyHeaderAndExecuteTxEvent,
                    PolynetworkVerifyHeaderAndExecuteTxEvent.transaction_hash
                    == GraphMappingBlockchain.tx_hash,
                )
                .where(
                    GraphMappingBlockchain.cctx_graph_id.is_(None),
                    GraphMappingBlockchain.bridge == "polynetwork",
                    GraphMappingBlockchain.label != "anomaly",
                    PolynetworkVerifyHeaderAndExecuteTxEvent.from_chain.notin_(
                        ["ethereum", "bnb", "polygon", "arbitrum", "avalanche", "gnosis", "celo"]
                    ),
                )
            )
            graph_ids = [row[0] for row in session.execute(graph_ids_subquery).fetchall()]

            updated_count = (
                session.query(GraphMappingBlockchain)
                .filter(GraphMappingBlockchain.graph_id.in_(graph_ids))
                .update({GraphMappingBlockchain.discard_flag: 1}, synchronize_session=False)
            )
            self.discard_associated_nodes_and_edges(session, graph_ids)
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

        return updated_count


if __name__ == "__main__":
    cleaner = CleanDataset()

    #ronin_count = cleaner.clean_ronin_unlinked_destination_graphs()
    #print(f"Discarded {ronin_count} unlinked ronin destination graphs")

    nomad_count = cleaner.clean_nomad_unlinked_destination_graphs()
    print(f"Discarded {nomad_count} unlinked nomad destination graphs")

    polynetwork_count = cleaner.clear_polynetwork_unlinked_destination_graphs()
    print(f"Discarded {polynetwork_count} unlinked polynetwork destination graphs")
