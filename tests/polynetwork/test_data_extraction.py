import argparse

from cli import Cli


def test_extract_data():
    args = argparse.Namespace(
        blockchains=["ethereum", "bnb", "polygon", "arbitrum"],
        bridge="polynetwork",
        start_ts=1688230800,  # 1st July 2022 17:00:00 UTC
        end_ts=1688256000,    # 2nd July 2022 00:00:00 UTC
    )

    Cli.extract_data(args)

    from repository.database import DBSession
    from repository.polynetwork.repository import (
        PolynetworkCrossChainEventRepository,
        PolynetworkCrossChainTransactionsRepository,
        PolynetworkLockEventRepository,
        PolynetworkUnlockEventRepository,
        PolynetworkVerifyHeaderAndExecuteTxEventRepository,
    )

    polynetwork_cross_chain_event = PolynetworkCrossChainEventRepository(DBSession)
    events = polynetwork_cross_chain_event.get_all()
    print(f"Number of events in PolynetworkCrossChainEvent: {len(events)}")
    assert len(events) == 389, "Expected events in PolynetworkCrossChainEvent table after extraction."

    polynetwork_lock_event_repository = PolynetworkLockEventRepository(DBSession)
    events = polynetwork_lock_event_repository.get_all()
    print(f"Number of events in PolynetworkLockEvent: {len(events)}")
    assert len(events) == 389, "Expected events in PolynetworkLockEvent table after extraction."

    polynetwork_unlock_event_repository = PolynetworkUnlockEventRepository(DBSession)
    events = polynetwork_unlock_event_repository.get_all()
    print(f"Number of events in PolynetworkUnlockEvent: {len(events)}")
    assert len(events) == 132, "Expected events in PolynetworkUnlockEvent table after extraction."

    polynetwork_verify_header_and_execute_tx_event_repository = PolynetworkVerifyHeaderAndExecuteTxEventRepository(DBSession)
    events = polynetwork_verify_header_and_execute_tx_event_repository.get_all()
    print(f"Number of events in PolynetworkVerifyHeaderAndExecuteTxEvent: {len(events)}")
    assert len(events) == 132, "Expected events in PolynetworkVerifyHeaderAndExecuteTxEvent table after extraction."

    args = argparse.Namespace(
        bridge="polynetwork",
    )
    Cli.generate_data(args)

    #! TODO UNCOMMENT AFTER IMPLEMENTING GENERATION
    # Here we can check if the data was generated correctly
    # from repository.database import DBSession
    # from repository.nomad.repository import NomadCrossChainTransactionRepository

    # nomad_cctx_repo = NomadCrossChainTransactionRepository(DBSession)
    # transactions = nomad_cctx_repo.get_all()
    # print(f"Number of transactions in NomadCrossChainTransactions: {len(transactions)}")
    # assert len(transactions) == 55, (
    #     "Expected transactions in NomadCrossChainTransactions table after generation."
    # )