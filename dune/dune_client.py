import os
import time

import requests
from dotenv import load_dotenv

from utils.utils import CustomException, log_error, log_to_cli


class DuneClient:
    CLASS_NAME = "DuneClient"

    QUERY_STATES = ["QUERY_STATE_PENDING", "QUERY_STATE_EXECUTING", "QUERY_STATE_COMPLETED"]

    def __init__(self, bridge):
        load_dotenv()
        self.api_key = os.getenv("DUNE_API_KEY")

        if not self.api_key:
            raise CustomException("DUNE_API_KEY is not set in environment variables. Please set it to use DuneClient.")
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
                    raise Exception("Invalid response from Dune API: missing 'state' field.")

                if response.json()["state"] not in self.QUERY_STATES:
                    raise Exception(f"Unexpected query state: {response.json()['state']}")

                return response.json()
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
            "sql": "SELECT * FROM tokens.transfers " +
                f"WHERE blockchain = '{blockchain}' " +
                "AND token_standard = 'native' " +
                f"AND tx_hash in ({','.join(tx_hashes)})" + 
                "ORDER BY tx_hash",
            "performance": "medium"
        }
        return self.make_request(endpoint, payload)["execution_id"]
    
    def get_execution_status(self, execution_id: str) -> str:
        endpoint = f"v1/execution/{execution_id}/status"
        payload = None
        return self.make_request(endpoint, payload)

    def get_execution_results(self, execution_id: str) -> list[dict]:
        endpoint = f"v1/execution/{execution_id}/results"
        payload = None
        return self.make_request(endpoint, payload)["result"]
    
    def fetch_native_transactions(self, blockchain: str, tx_hashes: list[str]) -> list[dict]:
        execution_id = self.execute_query(blockchain, tx_hashes)
        log_to_cli(f"Created Dune query with execution ID {execution_id} for {len(tx_hashes)} transaction hashes.")
        total_wait_time = 0
        while True:
            response = self.get_execution_status(execution_id)
            log_to_cli(f"Dune query execution status for execution ID {execution_id}: {response['state']} (wait: {total_wait_time}s)")
            if response["state"] == "QUERY_STATE_COMPLETED":
                break
            elif response["state"] == "QUERY_STATE_FAILED":
                raise CustomException(f"Dune query execution failed for execution ID {execution_id}.")
            time.sleep(5)  # Poll every 5 seconds
            total_wait_time += 5
            if total_wait_time > 300:  # Timeout after 5 minutes
                raise CustomException(f"Dune query execution timed out after 5 minutes for execution ID {execution_id}.")

        results = self.get_execution_results(execution_id)
        log_to_cli(f"Fetched Dune query results for execution ID {execution_id}. Number of native transactions found: {len(results)}")
        return results