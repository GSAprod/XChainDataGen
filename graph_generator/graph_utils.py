
from config.constants import Bridge
from repository.graphs.repository import GraphEdgeRepository, GraphMappingBlockchainRepository, GraphMappingCrossChainRepository, GraphNodeRepository
from repository.database import DBSession

def confirm_clean_graph_data(bridge: Bridge = None):
    # Initialize GraphNodeRepository
    graph_node_repo = GraphNodeRepository(DBSession)

    graphs_num = graph_node_repo.get_nonconverted_amount_graphs(bridge.value if bridge else None)
    confirmation = input(
        f"This will permanently delete {graphs_num} graphs whose amounts are not converted to USD."
        f" Are you sure you want to proceed? (yes/no): "
    )
    if confirmation.lower() != "yes":
        print("Operation cancelled.")
        return
    
    graphs_num = graph_node_repo.clean_nonconverted_amount_nodes(bridge.value if bridge else None)
    print(f"Cleaned {len(graphs_num)} graphs.")