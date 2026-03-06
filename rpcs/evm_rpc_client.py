from config.constants import (
    RPCS_CONFIG_FILE,
)
from rpcs.rpc_client import RPCClient


class EvmRPCClient(RPCClient):
    CLASS_NAME = "EvmRPCClient"

    def __init__(self, bridge, config_file: str = RPCS_CONFIG_FILE):
        super().__init__(bridge, config_file)

    def get_logs_emitted_by_contract(
        self,
        blockchain: str,
        contract: str,
        topics: list,
        start_block: str,
        end_block: str,
    ) -> list:
        method = "eth_getLogs"
        params = [
            {
                "fromBlock": hex(start_block),
                "toBlock": hex(end_block),
                "topics": [topics],
                "address": contract,
            }
        ]

        rpc = self.get_next_rpc(blockchain)
        response = self.make_request(rpc, blockchain, method, params)

        return response["result"] if response else []

    def process_transaction(self, blockchain: str, tx_hash: str, block_number: str) -> dict:
        import concurrent.futures

        if self.requires_transaction_by_hash_rpc_call:
            method_tx = "eth_getTransactionByHash"
            params_tx = [tx_hash]

        method_receipt = "eth_getTransactionReceipt"
        params_receipt = [tx_hash]

        method_block = "eth_getBlockByNumber"
        params_block = [block_number, True]

        rpc = self.get_next_rpc(blockchain)

        with concurrent.futures.ThreadPoolExecutor() as executor:
            if self.requires_transaction_by_hash_rpc_call:
                future_tx = executor.submit(
                    self.make_request, rpc, blockchain, method_tx, params_tx
                )

            future_receipt = executor.submit(
                self.make_request, rpc, blockchain, method_receipt, params_receipt
            )
            future_block = executor.submit(
                self.make_request, rpc, blockchain, method_block, params_block
            )

            if self.requires_transaction_by_hash_rpc_call:
                response_tx = future_tx.result()
            else:
                response_tx = None

            response_receipt = future_receipt.result()
            response_block = future_block.result()

            if response_receipt and response_tx:
                response_receipt["result"]["value"] = response_tx["result"]["value"]
                response_receipt["result"]["input"] = response_tx["result"]["input"]

        return response_receipt["result"] if response_receipt else {}, response_block[
            "result"
        ] if response_block else {}

    def get_transaction_receipt(self, blockchain: str, tx_hash: str) -> dict:
        method = "eth_getTransactionReceipt"
        params = [tx_hash]

        rpc = self.get_next_rpc(blockchain)
        response = self.make_request(rpc, blockchain, method, params)

        return response["result"] if response else {}

    def get_transaction_by_hash(self, blockchain: str, tx_hash: str) -> dict:
        method = "eth_getTransactionByHash"
        params = [tx_hash]

        rpc = self.get_next_rpc(blockchain)
        response = self.make_request(rpc, blockchain, method, params)

        return response["result"] if response else {}

    def get_transaction_trace(self, blockchain: str, tx_hash: str) -> dict:
        method = "trace_transaction"
        params = [tx_hash]

        rpc = self.get_next_rpc(blockchain)
        response = self.make_request(rpc, blockchain, method, params)

        return response["result"] if response else {}

    def debug_transaction(self, blockchain: str, tx_hash: str, extra_params: str) -> dict:
        method = "debug_traceTransaction"
        params = [tx_hash, extra_params] if extra_params else [tx_hash]

        rpc = self.get_next_rpc(blockchain)
        response = self.make_request(rpc, blockchain, method, params)

        return response["result"] if response else {}

    def get_block(self, blockchain: str, block_number: str = "latest", full_transactions: bool = True) -> dict:
        method = "eth_getBlockByNumber"
        params = [block_number, full_transactions]

        rpc = self.get_next_rpc(blockchain)
        response = self.make_request(rpc, blockchain, method, params)

        return response["result"] if response else {}
    
    def search_block_by_timestamp(self, blockchain: str, timestamp: int) -> int:
        """
        This function performs a binary search to find the block number corresponding to a given timestamp.
        It starts by getting the latest block and the block from 2000 blocks ago to calculate
        the average block time, and then iteratively narrows down the search 
        until it finds a block with a timestamp close to the target timestamp.
        """

        # Define a tolerance level (in seconds) for how close the block timestamp should be to the target timestamp
        # Smaller tolerances means less requests in the final iterative phase, but are more likely to
        # cause infinite loops during binary search
        tolerance = 60  # 1 minute tolerance
        
        # Step 1: Get the latest block number (x) and its timestamp
        current_block = self.get_block(blockchain, full_transactions=False)
        current_block_number = int(current_block["number"], 16)
        current_block_timestamp = int(current_block["timestamp"], 16)
        if current_block_timestamp < timestamp:
            raise Exception(f"Error: Target timestamp {timestamp} is in the future compared to the latest block timestamp {current_block_timestamp}.")

        # Step 2: Get the timestamp of the block (x - 2000) and calculate the average block time
        block_2000 = self.get_block(blockchain, hex(current_block_number - 2000), full_transactions=False)
        block_2000_timestamp = int(block_2000["timestamp"], 16)
        average_block_throughput = 2000 / (current_block_timestamp - block_2000_timestamp)

        last_used_number = current_block_number
        last_used_timestamp = current_block_timestamp
        last_estimated_number = current_block_number - 2000
        last_estimated_timestamp = block_2000_timestamp

        # Step 3: Perform a binary search to find the block number with a timestamp close to the target timestamp
        while abs(last_estimated_timestamp - timestamp) > tolerance:
            # Recalculate average block throughput if needed
            if abs(last_used_timestamp - last_estimated_timestamp) > tolerance:
                average_block_throughput = abs(last_used_number - last_estimated_number) / abs(last_used_timestamp - last_estimated_timestamp)
                
            last_used_number = last_estimated_number
            last_used_timestamp = last_estimated_timestamp

            # Get new estimated block number
            last_estimated_number = last_used_number + int((timestamp - last_used_timestamp) * average_block_throughput)
            if last_estimated_number < 1:
                last_estimated_number = 1
            elif last_estimated_number > current_block_number:
                last_estimated_number = current_block_number
            last_estimated_block = self.get_block(blockchain, hex(last_estimated_number), full_transactions=False)
            last_estimated_timestamp = int(last_estimated_block["timestamp"], 16)
            print(f"Binary search: Estimated block: {last_estimated_number}, timestamp: {last_estimated_timestamp}, target timestamp: {timestamp}")

        # Step 4: Adjust the estimated block number based on the timestamp comparison
        if last_estimated_timestamp < timestamp:
            while last_estimated_timestamp < timestamp:
                last_estimated_number += 1
                last_estimated_block = self.get_block(blockchain, hex(last_estimated_number), full_transactions=False)
                last_estimated_timestamp = int(last_estimated_block["timestamp"], 16)
                print(f"Iterative search: Estimated block: {last_estimated_number}, timestamp: {last_estimated_timestamp}, target timestamp: {timestamp}")

            # Result is the block before the target timestamp
            last_estimated_number -= 1

        elif last_estimated_timestamp > timestamp:
            while last_estimated_timestamp > timestamp and last_estimated_number > 1:
                last_estimated_number -= 1
                last_estimated_block = self.get_block(blockchain, hex(last_estimated_number), full_transactions=False)
                last_estimated_timestamp = int(last_estimated_block["timestamp"], 16)
                print(f"Iterative search: Estimated block: {last_estimated_number}, timestamp: {last_estimated_timestamp}, target timestamp: {timestamp}")


        # Result is the block before the target timestamp

        print(f"Result: Estimated block: {last_estimated_number}, timestamp: {last_estimated_timestamp}, target timestamp: {timestamp}")
        return last_estimated_number