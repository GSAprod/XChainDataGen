import json
import os

from eth_abi import decode as abi_decode
from web3 import Web3

from utils.utils import CliColor, log_to_cli


class TokenInspector:
    def __init__(self, rpc_client, token_metadata_repo):
        self.rpc_client = rpc_client
        self.token_metadata_repo = token_metadata_repo
        self._unknown_contracts: set[str] = set()

    def ensure_metadata(self, address: str, blockchain: str):
        metadata = self.token_metadata_repo.get_token_metadata_by_contract_and_blockchain(address, blockchain)
        if metadata is not None:
            return metadata
        if address in self._unknown_contracts:
            return None
        log_to_cli(
            f"Blockchain {blockchain} - Address {address} not in token metadata. Checking if ERC20..."
        )
        if self._detect_erc20(address, blockchain):
            return self.token_metadata_repo.get_token_metadata_by_contract_and_blockchain(address, blockchain)
        self._unknown_contracts.add(address)
        return None

    def load_erc20_contract(self, address: str):
        checksum_address = Web3.to_checksum_address(address)
        abi_path = os.path.normpath(os.path.join(os.path.dirname(__file__), "ABI", "erc20_abi.json"))
        with open(abi_path, "r") as f:
            abi = json.load(f)
        return Web3().eth.contract(address=checksum_address, abi=abi)

    def _detect_erc20(self, address: str, blockchain: str) -> bool:
        function_signatures = [
            {"signature": "0x06fdde03", "name": "name",        "result": None, "resultType": "string"},
            {"signature": "0x95d89b41", "name": "symbol",      "result": None, "resultType": "string"},
            {"signature": "0x313ce567", "name": "decimals",    "result": None, "resultType": "uint8"},
            {"signature": "0x18160ddd", "name": "totalSupply", "result": None, "resultType": "uint256"},
        ]

        for func in function_signatures:
            try:
                res = self.rpc_client.function_call(blockchain, address, func["signature"], no_backoff=True)
                if res is None or res == "0x0":
                    return False
                if func["resultType"] == "string":
                    func["result"] = abi_decode(["string"], bytes.fromhex(res[2:]))[0]
                elif func["resultType"] in ("uint8", "uint256"):
                    func["result"] = int(res, 16)
                else:
                    func["result"] = res
            except Exception as e:
                log_to_cli(
                    f"Blockchain {blockchain} - [WARNING] Error calling {func['name']} on {address}: {e}",
                    CliColor.ERROR
                )
                return False

        log_to_cli(
            f"Added newly discovered ERC20: {address} "
            f"name={function_signatures[0]['result']} symbol={function_signatures[1]['result']}"
        )
        if self.token_metadata_repo.get_token_metadata_by_contract_and_blockchain(address, blockchain) is None:
            self.token_metadata_repo.create({
                "symbol": function_signatures[1]["result"],
                "name":   function_signatures[0]["result"],
                "decimals": function_signatures[2]["result"],
                "blockchain": blockchain,
                "address": address,
            })

        return True
