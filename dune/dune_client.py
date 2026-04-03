import time

import requests

from utils.utils import CustomException, log_error, log_to_cli, log_to_file


class DuneClient:
    CLASS_NAME = "DuneClient"

    QUERY_STATES = ["QUERY_STATE_PENDING", "QUERY_STATE_COMPLETED"]

    def __init__(self, api_key, bridge):
        self.api_key = api_key
        self.bridge = bridge

    def make_request(self, endpoint: str, payload: dict):
        """
        Queries the Dune API to check if any of the given transaction
        hashes have associated native token transfers on the specified blockchain.
        """
        
        backoff = 1
        while backoff <= 64:
            headers = {
                "X-Dune-Api-Key": self.api_key,
            }
            if payload is not None:
                headers["Content-Type"] = "application/json"
            try:
                response = requests.post(f"https://api.dune.com/api/{endpoint}", json=payload, headers=headers, timeout=10)
                response.raise_for_status()

                if response.json() is None or response.json()["state"] is None:
                    raise Exception()

                if response.json()["state"] not in self.QUERY_STATES:
                    raise Exception(f"Unexpected query state: {response.json()['state']}")

                return response.json()["execution_id"]
            except Exception as e:
                error_str = f"Failed to create Dune query. Retrying with backoff {backoff} seconds. Error: {e}"
                log_error(self.bridge, error_str)
                time.sleep(backoff)
                # ignore the exception and try again
                pass

        raise CustomException(f"Failed to create Dune query after multiple retries.")
    
    def execute_query(self, blockchain: str, tx_hashes: list[str]) -> str:
        endpoint = "v1/sql/execute"
        payload = {
            "sql": "SELECT * FROM from tokens.transfers " +
                "where blockchain = 'ethereum' " +
                "and token_standard = 'native' " +
                f"and tx_hash in {",".join(tx_hashes)}",
            "performance": "medium"
        }
        return self.make_request(endpoint, payload)["execution_id"]
    
    def get_execution_status(self, execution_id: str) -> str:
        endpoint = f"v1/sql/execution/{execution_id}"
        payload = None
        return self.make_request(endpoint, payload)["is_execution_finished"]

    def get_execution_results(self, execution_id: str) -> list[dict]:
        endpoint = f"v1/sql/execution/{execution_id}/results"
        payload = None
        return self.make_request(endpoint, payload)["result"]
    
    def fetch_native_transactions(self, blockchain: str, tx_hashes: list[str]) -> list[dict]:
        execution_id = self.execute_query(blockchain, tx_hashes)
        log_to_cli(self.bridge, f"Created Dune query with execution ID {execution_id} for {len(tx_hashes)} transaction hashes.")
        total_wait_time = 0
        while True:
            status = self.get_execution_status(execution_id)
            log_to_cli(self.bridge, f"Dune query execution status for execution ID {execution_id}: {status}")
            if status == "QUERY_STATE_COMPLETED":
                break
            time.sleep(3)  # Poll every 3 seconds
            total_wait_time += 3
            if total_wait_time > 300:  # Timeout after 5 minutes
                raise CustomException(f"Dune query execution timed out after 5 minutes for execution ID {execution_id}.")

        results = self.get_execution_results(execution_id)
        log_to_cli(self.bridge, f"Fetched Dune query results for execution ID {execution_id}. Number of native transactions found: {len(results)}")
        return results