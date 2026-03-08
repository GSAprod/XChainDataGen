import argparse

from config.constants import Bridge
from extractor.evm_extractor import EvmExtractor
from extractor.solana_extractor import SolanaExtractor
from generator.generator import Generator
from repository.database import create_tables
from rpcs import generate_rpc_configs
from utils.utils import (
    CliColor,
    CustomException,
    build_log_message_2,
    get_block_by_timestamp,
    get_enum_instance,
    load_module,
    log_to_cli,
)


class Cli:
    CLASS_NAME = "Cli"

    def extract_data(args):

        # prevent realtime execution for now
        if args.realtime:
            print("\n########################################################################################\n\
                  realtime execution is not supported yet. Please provide start_ts and end_ts for extraction\
                  \n########################################################################################")
            exit(0)
        blockchains = args.blockchains

        bridge = get_enum_instance(Bridge, args.bridge)

        Cli.load_db_models(bridge)

        for idx, blockchain in enumerate(blockchains):
            generate_rpc_configs(blockchain)

            if blockchain == "solana":
                solana_ranges = {}
                for item in args.solana_range:
                    program, start_sig, end_sig = item.split(":")
                    solana_ranges[program] = {
                        "start_signature": start_sig,
                        "end_signature": end_sig,
                    }

                Cli.extract_solana_data(
                    idx,
                    bridge,
                    blockchain,
                    solana_ranges,
                    blockchains,
                )
            else:
                Cli.extract_evm_data(
                    idx,
                    bridge,
                    blockchain,
                    args.start_ts,
                    args.end_ts,
                    args.realtime,
                    blockchains,
                )

    def extract_evm_data(idx, bridge, blockchain, start_ts, end_ts, realtime, blockchains):
        log_to_cli(
            build_log_message_2(
                start_ts,
                end_ts,
                bridge,
                blockchain,
                f"{idx + 1}/{len(blockchains)} Starting extraction... ",
            )
        )

        try:
            log_to_cli(
                build_log_message_2(
                    start_ts,
                    end_ts,
                    bridge,
                    blockchain,
                    "Loading contracts and ABIs...",
                )
            )
            extractor = EvmExtractor(bridge, blockchain, blockchains)

            if realtime:
                pass
            else:
                start_block = get_block_by_timestamp(int(start_ts), blockchain, extractor.rpc_client.get_block)
                end_block = get_block_by_timestamp(int(end_ts), blockchain, extractor.rpc_client.get_block)

        except Exception as e:
            log_to_cli(
                build_log_message_2(
                    start_ts,
                    end_ts,
                    bridge,
                    blockchain,
                    f"{idx + 1}/{len(blockchains)} Error: {e}",
                ),
                CliColor.ERROR,
            )
            return

        extractor.extract_data(
            start_block,
            end_block,
        )

        if idx == len(blockchains) - 1:
            extractor.post_processing()

    def extract_solana_data(idx, bridge, blockchain, signature_ranges, blockchains):
        extractor = SolanaExtractor(bridge, blockchain, blockchains)

        extractor.extract_data(signature_ranges)

    def generate_data(args):
        bridge = get_enum_instance(Bridge, args.bridge)

        Cli.load_db_models(bridge)

        generator = Generator(bridge)

        generator.generate_data()

    def cli():
        parser = argparse.ArgumentParser(description="Cross-chain Data Extraction Tool")
        subparsers = parser.add_subparsers(
            title="Actions", description="Available actions", dest="action"
        )

        # Extract action
        extract_parser = subparsers.add_parser("extract", help="Extract data from blockchains")
        extract_parser.add_argument(
            "--bridge",
            choices=[bridge.value for bridge in Bridge],
            required=True,
            help="Name of the bridge to analyze",
        )
        
        # mutually exclusive group (either --realtime or --start_ts/--end_ts)
        window_group = extract_parser.add_mutually_exclusive_group(required=False)
        window_group.add_argument("--realtime", action="store_true", help="Run extraction in realtime mode starting with the latest safe block")

        extract_parser.add_argument("--start_ts", help="Start timestamp for extraction")
        extract_parser.add_argument("--end_ts", help="End timestamp for extraction")
        
        extract_parser.add_argument(
            "--blockchains",
            choices=[
                "ethereum",
                "arbitrum",
                "polygon",
                "avalanche",
                "base",
                "optimism",
                "bnb",
                "scroll",
                "linea",
                "gnosis",
                "ronin",
                "solana",
                "unichain",
            ],
            nargs="+",
            help="List of blockchains to extract data from",
        )

        # Custom argument group for Solana-specific arguments
        solana_group = extract_parser.add_argument_group(
            "Solana-specific arguments", "Required if 'solana' is included in --blockchains"
        )
        solana_group.add_argument(
            "--solana-range",
            nargs="+",
            help="List of solana ranges in the format program:start_signature:end_signature",
        )

        def validate_solana_args(args):
            if args.blockchains and "solana" in args.blockchains:
                if not args.solana_range:
                    extract_parser.error(
                        "--solana-range is required when 'solana' is in --blockchains."
                    )
                # Validate each entry
                for entry in args.solana_range:
                    if entry.count(":") != 2:
                        extract_parser.error(
                            "Invalid --solana-range format. "
                            "Must be program:start_signature:end_signature"
                        )

        def validate_extract_args(args):
            if args.realtime:
                if args.start_ts or args.end_ts:
                    extract_parser.error("--realtime cannot be used with --start_ts/--end_ts.")
            else:
                if not args.start_ts or not args.end_ts:
                    extract_parser.error("--start_ts and --end_ts are required unless --realtime is used.")

        def validate_all_args(args):
            validate_extract_args(args)
            validate_solana_args(args)

        extract_parser.set_defaults(validate_all_args=validate_all_args)

        extract_parser.set_defaults(func=Cli.extract_data)

        # Generate action
        generate_parser = subparsers.add_parser(
            "generate", help="Generate cross-chain transactions"
        )
        generate_parser.add_argument(
            "--bridge",
            choices=[bridge.value for bridge in Bridge],
            required=True,
            help="Name of the bridge",
        )
        generate_parser.set_defaults(func=Cli.generate_data)

        args = parser.parse_args()
        if args.action:
            if hasattr(args, "validate_all_args"):
                args.validate_all_args(args)
            args.func(args)
        else:
            parser.print_help()

    def load_db_models(bridge: Bridge):
        """Dynamically loads the database models for the specified bridge."""
        func_name = "load_db_models"
        bridge_name = bridge.value

        try:
            load_module("repository.common")
            load_module(f"repository.{bridge_name}")
            create_tables()
        except Exception as e:
            raise CustomException(
                Cli.CLASS_NAME, func_name, f"Bridge {bridge_name} not supported"
            ) from e
