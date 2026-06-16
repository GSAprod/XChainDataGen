from datetime import datetime

from config.constants import BLOCKCHAIN_IDS, TOKEN_PRICING_SUPPORTED_BLOCKCHAINS
from generator.base_generator import PriceGenerator
from repository.common.models import TokenMetadata
from utils.utils import CliColor, log_to_cli


class TokenPricingService:
    def __init__(self, bridge, token_metadata_repo, token_price_repo, dune_client, fetch_timestamp_interval):
        self.bridge = bridge
        self.token_metadata_repo = token_metadata_repo
        self.token_price_repo = token_price_repo
        self.dune_client = dune_client
        self._fetch_timestamp_interval = fetch_timestamp_interval
        self._pending: list[tuple[int, str, int, float]] = []  # (node_id, symbol, timestamp, amount)
        self._unknown_symbols: set[tuple[str, str]] = set()    # (symbol, blockchain)

    def reset(self):
        self._pending.clear()

    def record_missing_price(self, node_id: int, symbol: str, timestamp: int, amount: float):
        self._pending.append((node_id, symbol, timestamp, amount))

    def maybe_fetch_prices_for_token(self, token_metadata: TokenMetadata, timestamp: int = None) -> bool:
        if token_metadata.symbol == "":
            if token_metadata.address == "0xa2327a938febf5fec13bacfb16ae10ecbc4cbdcf":
                token_metadata.symbol = "USDC"
            else:
                log_to_cli(
                    f"[WARNING] Token {token_metadata.address} on {token_metadata.blockchain} has no symbol. Skipping.",
                    CliColor.ERROR
                )
                return False

        if timestamp is not None:
            date = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d")
            if self.token_price_repo.get_token_price_by_symbol_and_date(token_metadata.symbol, date) is not None:
                return True
        elif self.token_price_repo.exists_price_for_symbol(token_metadata.symbol):
            return True

        min_ts, max_ts = self._fetch_timestamp_interval()
        min_ts = min_ts - (min_ts % 86400)  # Round down to start of day
        max_ts = max_ts - (max_ts % 86400) + 86399  # Round up to end of day

        if (token_metadata.symbol, token_metadata.blockchain) not in self._unknown_symbols:
            # First try fetching price by symbol only, as it's faster and often sufficient
            PriceGenerator.fetch_and_store_token_prices(
                self.bridge, self.token_price_repo, min_ts, max_ts,
                token_metadata.name, symbol=token_metadata.symbol
            )
            if timestamp is not None:
                if self.token_price_repo.get_token_price_by_symbol_and_date(token_metadata.symbol, date) is not None:
                    return True
            elif self.token_price_repo.exists_price_for_symbol(token_metadata.symbol):
                return True

            # If symbol-only fetch didn't work, try fetching with blockchain context to disambiguate tokens with same symbol across chains
            # (Skip if the token's blockchain is not supported for pricing, to avoid unnecessary fetching)
            if token_metadata.blockchain not in TOKEN_PRICING_SUPPORTED_BLOCKCHAINS:
                log_to_cli(f"[WARNING] Could not fetch price for {token_metadata.symbol}. Skipping...", CliColor.ERROR)
                self._unknown_symbols.add((token_metadata.symbol, token_metadata.blockchain))
                return False

            log_to_cli(f"Failed to fetch price for {token_metadata.symbol} using symbol only. Trying {token_metadata.blockchain}...")
            PriceGenerator.fetch_and_store_token_prices(
                self.bridge, self.token_price_repo, min_ts, max_ts,
                token_metadata.name, symbol=token_metadata.symbol,
                blockchain=token_metadata.blockchain, token_address=token_metadata.address
            )
            if timestamp is not None:
                if self.token_price_repo.get_token_price_by_symbol_and_date(token_metadata.symbol, date) is not None:
                    return True
            elif self.token_price_repo.exists_price_for_symbol(token_metadata.symbol):
                return True

            # If we still don't have price info, log a warning and add to unknown symbols to avoid repeated fetch attempts
            log_to_cli(f"[WARNING] Could not fetch price for {token_metadata.symbol}. Skipping...", CliColor.ERROR)
            self._unknown_symbols.add((token_metadata.symbol, token_metadata.blockchain))
        else:
            log_to_cli(f"[WARNING] Previous price-fetch attempts for {token_metadata.symbol} failed. Skipping.", CliColor.ERROR)

        return False

    def resolve_token_amount(self, token_metadata: TokenMetadata, raw_value: int, timestamp: int) -> tuple[float, int | None]:
        amount = float(raw_value) / (10 ** token_metadata.decimals)

        # Some tokens cannot be reliably identified wither by alchemy or by DUNE.
        # Hence, as a workaround, we hardcode the price conversion for these tokens here.
        if token_metadata.symbol == "CQT":
            # 10 CQT = 1 USD; result encoded as 1e18-integer
            # Price derived and used only around Nomad bridge incident
            return amount, int(raw_value * 10 ** (18 - token_metadata.decimals)) // 10
        elif token_metadata.symbol == "SDL":
            # 100 SDL = 1 USD; result encoded as 1e18-integer
            # Price derived and used only around PolyNetwork bridge incident
            return amount, int(raw_value * 10 ** (18 - token_metadata.decimals)) // 100
        elif token_metadata.symbol == "BUSD":
            # BUSD is a stablecoin that should be very close to 1 USD; result encoded as 1e18-integer
            return amount, int(raw_value * 10 ** (18 - token_metadata.decimals))
        elif token_metadata.symbol in ("WGLMR", "PWETH", ):
            # Some wrapped tokens have the same symbol as the native token but need to be unwrapped to get the correct price
            token_metadata.symbol = token_metadata.symbol[1:]

        prices_available = self.maybe_fetch_prices_for_token(token_metadata, timestamp)
        if not prices_available:
            log_to_cli(f"Price for {token_metadata.symbol} not found. Cannot convert to USD.", CliColor.ERROR)
            return amount, None

        date = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d")
        token_price = self.token_price_repo.get_token_price_by_symbol_and_date(token_metadata.symbol, date)
        if token_price is None:
            log_to_cli(f"Price for native token {token_metadata.symbol} not found. Cannot convert to USD.", CliColor.ERROR)
            return amount, None

        # amount_usd is 1e18-encoded (1 USD = 1e18).
        # Multiply raw integer by price and the decimal-padding factor to avoid float division of raw_value.
        amount_usd = int(raw_value * float(token_price.price_usd) * 10 ** (18 - token_metadata.decimals))
        return amount, amount_usd

    def resolve_native_amount(self, blockchain: str, raw_value: int, timestamp: int) -> tuple[float, int | None]:
        amount = float(raw_value) / 10 ** 18
        date = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d")

        if blockchain == "ronin":
            symbol = "RON"
        else:
            blockchain_config = next((c for c in BLOCKCHAIN_IDS.values() if c["name"] == blockchain), None)
            symbol = blockchain_config["native_token"] if blockchain_config else None
            if symbol is None:
                log_to_cli(f"Native token symbol for {blockchain} not found in BLOCKCHAIN_IDS.", CliColor.ERROR)
                return amount, None

        prices_available = self.maybe_fetch_prices_for_token(
            TokenMetadata(symbol=symbol, name=blockchain, decimals=18, address="token_native", blockchain=blockchain),
            timestamp=timestamp
        )
        if not prices_available:
            log_to_cli(f"Price for native token {symbol} not found. Cannot convert to USD.", CliColor.ERROR)
            return amount, None

        currency_price = self.token_price_repo.get_token_price_by_symbol_and_date(symbol, date)
        if currency_price is None:
            log_to_cli(f"Price for native token {symbol} not found. Cannot convert to USD.", CliColor.ERROR)
            return amount, None

        # Native tokens have 18 decimals: 10^(18-18) = 1, so amount_usd = raw_value * price
        amount_usd = int(raw_value * float(currency_price.price_usd))
        return amount, amount_usd

    def batch_resolve_pending(self, graph_node_repo):
        if not self._pending or self.dune_client is None:
            return

        symbols = list({symbol for _, symbol, _, _ in self._pending})
        min_ts = min(ts for _, _, ts, _ in self._pending)
        min_ts = min_ts - (min_ts % 86400)  # Round down to start of day
        max_ts = max(ts for _, _, ts, _ in self._pending)
        max_ts = max_ts - (max_ts % 86400) + 86399  # Round up to end of day

        log_to_cli(
            f"Querying Dune for prices of {len(symbols)} symbol(s) "
            f"to backfill {len(self._pending)} node(s) missing USD amounts..."
        )
        try:
            token_prices_dune = self.dune_client.fetch_token_prices_through_symbol(symbols, min_ts, max_ts)

            for tp in token_prices_dune["rows"]:
                date = tp["timestamp"].split(" ")[0]
                if self.token_price_repo.get_token_price_by_symbol_and_date(tp["symbol"], date) is None:
                    self.token_price_repo.create({
                        "name": "",
                        "symbol": tp["symbol"],
                        "price_usd": tp["price"],
                        "date": date,
                    })

            total_updated = 0
            for node_id, symbol, timestamp, amount in self._pending:
                date = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d")
                token_price = self.token_price_repo.get_token_price_by_symbol_and_date(symbol, date)
                if token_price is not None:
                    # amount is human-readable float; encode to 1e18-integer USD
                    amount_usd = int(amount * token_price.price_usd * 1e18)
                    graph_node_repo.update_amount_usd(node_id, amount_usd)
                    total_updated += 1
            log_to_cli(f"Backfilled USD prices for {total_updated}/{len(self._pending)} nodes using Dune data.")
        except Exception as e:
            log_to_cli(f"Error backfilling USD prices from Dune: {e}", CliColor.ERROR)
