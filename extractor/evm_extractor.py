import threading
import time

from config.constants import Bridge, BRIDGE_BLOCK_CONFIRMATIONS
from extractor.decoder import BridgeDecoder
from extractor.extractor import Extractor
from rpcs.evm_rpc_client import EvmRPCClient
from utils.utils import (
    CliColor,
    CustomException,
    build_log_message,
    log_error,
    log_to_cli,
)


class EvmExtractor(Extractor):
    CLASS_NAME = "EvmExtractor"

    def __init__(self, bridge: Bridge, blockchain: str, blockchains: list, graceful_stop = None):
        self.rpc_client = EvmRPCClient(bridge)
        # fetch a random rpc to initialize the decoder for the bridge
        self.decoder = BridgeDecoder(bridge, self.rpc_client.get_random_rpc(blockchain))
        self.graceful_stop = graceful_stop

        super().__init__(bridge, blockchain, blockchains)

    def worker(self):
        """Worker function for threads to process block ranges."""
        while not self.task_queue.empty():
            task = None
            try:
                task = self.task_queue.get()
                contract, topics, start_block, end_block = task

                if self.graceful_stop is not None and self.graceful_stop.is_set():
                    self.task_queue.task_done()
                    break

                self.work(
                    contract,
                    topics,
                    start_block,
                    end_block,
                )
            except CustomException as e:
                request_desc = (
                    f"Error processing request: {self.bridge}, {self.blockchain}, {start_block}, "
                    f"{end_block}, {contract}, {topics}. Error: {e}"
                )
                log_error(self.bridge, request_desc)
            finally:
                self.task_queue.task_done() if task is not None else None

    def work(
        self,
        contract: str,
        topics: list,
        start_block: int,
        end_block: int,
    ):
        log_to_cli(
            build_log_message(
                start_block,
                end_block,
                contract,
                self.bridge,
                self.blockchain,
                "Processing logs and transactions...",
            )
        )

        logs = self.rpc_client.get_logs_emitted_by_contract(
            self.blockchain, contract, topics, start_block, end_block
        )

        if len(logs) == 0:
            return

        decoded_logs = []
        txs = {}

        for log in logs:
            decoded_log = self.decoder.decode(contract, self.blockchain, log)

            # we take the decoded log and append more data to it, such that the handler can insert
            #  in the right DB table
            decoded_log["transaction_hash"] = log["transactionHash"]
            decoded_log["block_number"] = log["blockNumber"]
            decoded_log["contract_address"] = contract
            decoded_log["topic"] = log["topics"][0]
            decoded_logs.append(decoded_log)

        included_logs = self.handler.handle_events(
            self.blockchain, start_block, end_block, contract, topics, decoded_logs
        )

        for log in included_logs:
            tx_hash = log["transaction_hash"]

            # to avoid processing the same transaction multiple times we ignore if already in the
            #  repository
            try:
                if self.handler.does_transaction_exist_by_hash(tx_hash):
                    continue

                tx, block = self.rpc_client.process_transaction(
                    self.blockchain, log["transaction_hash"], log["block_number"]
                )

                if tx is None or block is None:
                    raise Exception(tx_hash)

                txs[tx_hash] = self.handler.create_transaction_object(
                    self.blockchain, tx, block["timestamp"]
                )

            except CustomException as e:
                request_desc = (
                    f"Error processing request: {self.blockchain}, {start_block}, {end_block}, "
                    f"{contract}, {topics}. Error: {e}"
                )
                log_error(self.bridge, request_desc)

        if len(txs) > 0:
            try:
                self.handler.handle_transactions(txs.values())
            except CustomException:
                # if there is an error while handling transactions in batch, we handle them one
                # by one to avoid the entire batch failing
                for tx in txs.values():
                    try:
                        self.handler.handle_transaction(tx)
                    except CustomException as e:
                        request_desc = (
                            f"Error processing transaction: {self.blockchain}, "
                            f"{tx['transaction_hash']}. Error: {e}"
                        )
                        log_error(self.bridge, request_desc)

    def extract_data(self, realtime: bool = False, start_block: int = None, end_block: int = None):
        """Main extraction logic."""

        # load the bridge contract addresses and topics from the configuration file
        bridge_blockchain_pairs = self.handler.get_bridge_contracts_and_topics(
            self.bridge, self.blockchain
        )

        # assuming either realtime=True and end_block is None, or realtime=False and start_block and end_block are not None
        if realtime and start_block is None:
            start_block = self.get_latest_safe_block()

        while True:
            if self.graceful_stop is not None and self.graceful_stop.is_set():
                log_to_cli("Stop signal received. Stopping extraction...", CliColor.SUCCESS)
                break

            if realtime:
                end_block = self.get_latest_safe_block()
                if start_block > end_block: # no new safe blocks
                    time.sleep(1)
                    continue

            for pair in bridge_blockchain_pairs:
                for contract in pair["contracts"]:
                    threads = []

                    start_time = time.time()
                    topics = pair["topics"]

                    total_blocks = end_block - start_block + 1
                    if total_blocks == 1:
                        block_ranges = [(start_block, start_block + 1)]
                        num_threads = 1
                    else:
                        num_threads = min(
                            self.rpc_client.max_threads_per_blockchain(self.blockchain) * 2,
                            total_blocks,
                        )

                        chunk_size = max(
                            1, min((total_blocks + num_threads - 1) // num_threads, 1000)
                        )

                        block_ranges = self.divide_range(
                            start_block,
                            end_block - 1,
                            chunk_size,
                        )

                    # Populate the task queue
                    for start, end in block_ranges:
                        self.task_queue.put((contract, topics, start, end))

                    # Create and start threads
                    worker_count = min(num_threads, len(block_ranges))
                    log_to_cli(
                        build_log_message(
                            start_block,
                            end_block,
                            contract,
                            self.bridge,
                            self.blockchain,
                            (
                                f"Launching {worker_count} threads to process {len(block_ranges)} block "
                                f"ranges...",
                            ),
                        )
                    )

                    if self.graceful_stop is not None and self.graceful_stop.is_set():
                        log_to_cli("Stop signal received. Stopping extraction...", CliColor.SUCCESS)
                        break

                    for i in range(worker_count):
                        thread = threading.Thread(target=self.worker, name=f"thread_id_{i}")
                        thread.start()
                        threads.append(thread)

                    # Wait for all threads to complete
                    self.task_queue.join()
                    for thread in threads:
                        thread.join()

                    threads.clear()

                    end_time = time.time()

                    log_to_cli(
                        build_log_message(
                            start_block,
                            end_block,
                            contract,
                            self.bridge,
                            self.blockchain,
                            (
                                f"Finished processing logs and transactions. Time taken: "
                                f"{end_time - start_time} seconds.",
                            ),
                        ),
                        CliColor.SUCCESS,
                    )
                
            if not realtime:
                return

            start_block = end_block + 1

    def get_latest_safe_block(self) -> int:
        latest_block_number = self.rpc_client.get_latest_block_number(self.blockchain)
        confirmations = BRIDGE_BLOCK_CONFIRMATIONS.get(self.blockchain, 0)
        return max(0, latest_block_number - confirmations)