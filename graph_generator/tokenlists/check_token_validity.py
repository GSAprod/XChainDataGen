import csv
import json
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, PROJECT_ROOT)

from dotenv import load_dotenv

load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

from sqlalchemy import create_engine  # noqa: E402
from sqlalchemy.orm import sessionmaker  # noqa: E402

from config.constants import BLOCKCHAIN_IDS, TOKEN_LISTS_SUPPORTED_BLOCKCHAINS  # noqa: E402
from repository.common.models import TokenMetadata  # noqa: E402

TOKENLISTS_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_PATH = os.path.join(TOKENLISTS_DIR, "token_validation.csv")

# Reverse map: integer chain ID -> blockchain name (EVM chains only)
CHAIN_ID_TO_NAME = {
    int(k): v["name"]
    for k, v in BLOCKCHAIN_IDS.items()
    if k.isdigit()
}


def _load_token_list(list_name: str) -> dict:
    path = os.path.join(TOKENLISTS_DIR, list_name + ".json")
    with open(path) as f:
        return json.load(f)


def build_valid_set() -> set[tuple[str, str]]:
    """Return a set of (blockchain_name, address_lower) pairs from all token lists."""
    valid = set()

    for list_name, supported_blockchains in TOKEN_LISTS_SUPPORTED_BLOCKCHAINS.items():
        supported_set = set(supported_blockchains)
        data = _load_token_list(list_name)

        for token in data.get("tokens", []):
            chain_id = token.get("chainId")
            address = token.get("address")
            blockchain = CHAIN_ID_TO_NAME.get(chain_id)

            if blockchain and address and blockchain in supported_set:
                valid.add((blockchain, address.lower()))

            # Uniswap tokens carry bridgeInfo with canonical addresses on other chains
            bridge_info = token.get("extensions", {}).get("bridgeInfo", {})
            for bridge_chain_id_str, bridge_data in bridge_info.items():
                bridge_blockchain = CHAIN_ID_TO_NAME.get(int(bridge_chain_id_str))
                bridge_address = bridge_data.get("tokenAddress")
                if bridge_blockchain and bridge_address and bridge_blockchain in supported_set:
                    valid.add((bridge_blockchain, bridge_address.lower()))

    return valid


def connect_to_db():
    database_url = "postgresql://user:password@localhost:5432/db_app"
    engine = create_engine(database_url)
    return sessionmaker(bind=engine)


def main():
    print("Loading token lists...")
    valid_set = build_valid_set()
    print(f"  {len(valid_set)} (blockchain, address) pairs found across all token lists")

    Session = connect_to_db()
    with Session() as session:
        rows = session.query(TokenMetadata).all()
    print(f"  {len(rows)} entries fetched from token_metadata")

    valid_count = 0
    with open(OUTPUT_PATH, "w", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter="\t")
        writer.writerow(["id", "symbol", "blockchain", "address", "state"])

        for row in rows:
            address = row.address
            if address and (row.blockchain, address.lower()) in valid_set:
                state = "VALID"
                valid_count += 1
            else:
                state = "UNKNOWN"
            writer.writerow([row.id, row.symbol, row.blockchain, address, state])

    print(f"  {valid_count} VALID, {len(rows) - valid_count} UNKNOWN")
    print(f"Output written to: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
