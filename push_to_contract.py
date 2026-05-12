#!/usr/bin/env python3
"""Push the A100 weighted index price to ByteStrike CuOracle on Sepolia."""

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from dotenv import load_dotenv

from cu_oracle_client import (
    DEFAULT_A100_ASSET_ID,
    DEFAULT_CU_ORACLE_ADDRESS,
    CuOracleClient,
    OracleUpdate,
    OracleUpdateResult,
    price_to_x18,
    x18_to_usd,
)

load_dotenv()

A100_MARKET = "A100-PERP"
A100_ASSET_NAME = "A100"
A100_ASSET_ID = os.getenv("A100_ASSET_ID") or DEFAULT_A100_ASSET_ID
CU_ORACLE_ADDRESS = os.getenv("CU_ORACLE_ADDRESS") or DEFAULT_CU_ORACLE_ADDRESS
PRIVATE_KEY = os.getenv("ORACLE_UPDATER_PRIVATE_KEY") or os.getenv("PRIVATE_KEY") or os.getenv("WALLET_PRIVATE_KEY")
SEPOLIA_RPC_URL = os.getenv("SEPOLIA_RPC_URL")


def load_weighted_index_price() -> float:
    index_path = Path(__file__).with_name("a100_weighted_index.json")
    if not index_path.exists():
        raise FileNotFoundError(
            f"{index_path.name} not found. Run calculate_a100_index.py first or pass --price."
        )
    with index_path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    price = data.get("final_index_price")
    if price is None:
        raise ValueError("a100_weighted_index.json is missing final_index_price")
    return float(price)


def append_log(results: List[OracleUpdateResult], price_usd: float, client: CuOracleClient) -> None:
    log_path = Path(__file__).with_name("a100_oracle_update_log.json")
    logs = []
    if log_path.exists():
        try:
            with log_path.open("r", encoding="utf-8") as handle:
                logs = json.load(handle)
            if not isinstance(logs, list):
                logs = []
        except Exception:
            logs = []

    for result in results:
        logs.append(
            {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "network": "sepolia",
                "oracle": client.contract_address,
                "updater_address": client.address,
                "market": result.market,
                "asset": result.asset_name,
                "asset_id": result.asset_id,
                "price_usd": price_usd,
                "price_x18": result.price_x18,
                "commit_hash": result.commit_hash,
                "nonce": result.nonce,
                "commit_tx_hash": result.commit_tx_hash,
                "reveal_tx_hash": result.reveal_tx_hash,
                "commit_block": result.commit_block,
                "reveal_block": result.reveal_block,
                "commit_timestamp": result.commit_timestamp,
            }
        )

    logs = logs[-100:]
    with log_path.open("w", encoding="utf-8") as handle:
        json.dump(logs, handle, indent=2)
    print(f"Logged update to {log_path.name}")


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Commit/reveal the A100 weighted index price to ByteStrike CuOracle.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Examples:
  python push_to_contract.py --price 1.84
  python push_to_contract.py
  python push_to_contract.py --show-only

Default target:
  CuOracle: {DEFAULT_CU_ORACLE_ADDRESS}
  Market:   {A100_MARKET}
  Asset:    {A100_ASSET_NAME} ({DEFAULT_A100_ASSET_ID})
        """,
    )
    parser.add_argument("--price", type=float, help="A100 weighted index price in USD/hour")
    parser.add_argument("--show-only", action="store_true", help="Only show the current CuOracle price")
    parser.add_argument("--no-verify", action="store_true", help="Skip post-reveal verification")
    parser.add_argument(
        "--reveal-wait-seconds",
        type=int,
        default=None,
        help="Seconds to wait between commit and reveal. Defaults to ORACLE_REVEAL_WAIT_SECONDS or 3.",
    )
    parser.add_argument("--allow-high", action="store_true", help="Allow prices above $100/hr without failing")
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args()

    if not PRIVATE_KEY:
        print("ERROR: ORACLE_UPDATER_PRIVATE_KEY, PRIVATE_KEY, or WALLET_PRIVATE_KEY is required")
        sys.exit(1)

    try:
        client = CuOracleClient(
            rpc_url=SEPOLIA_RPC_URL,
            private_key=PRIVATE_KEY,
            contract_address=CU_ORACLE_ADDRESS,
        )
        client.print_context()
    except Exception as exc:
        print("=" * 60)
        print("ERROR: BLOCKCHAIN CONNECTION FAILED")
        print("=" * 60)
        print(f"   {exc}")
        print("=" * 60)
        sys.exit(1)

    try:
        supported = client.is_supported_asset(A100_ASSET_ID)
        current_price, current_timestamp = client.get_latest_price(A100_ASSET_ID)
    except Exception as exc:
        print(f"ERROR: Failed to read {A100_ASSET_NAME} from CuOracle: {exc}")
        sys.exit(1)

    print(f"{A100_MARKET} asset supported: {supported}")
    print(f"Current {A100_ASSET_NAME}: ${x18_to_usd(current_price):.6f}/hr")
    print(f"Current commit timestamp: {current_timestamp}")

    if args.show_only:
        sys.exit(0)

    try:
        price_usd = args.price if args.price is not None else load_weighted_index_price()
    except Exception as exc:
        print(f"ERROR: Failed to load A100 price: {exc}")
        sys.exit(1)

    if price_usd <= 0:
        print(f"ERROR: A100 price must be > 0, got {price_usd}")
        sys.exit(1)
    if price_usd > 100 and not args.allow_high:
        print(f"ERROR: A100 price ${price_usd:.2f}/hr is above the safety limit")
        print("Pass --allow-high if this is expected.")
        sys.exit(1)

    update = OracleUpdate(
        asset_name=A100_ASSET_NAME,
        asset_id=A100_ASSET_ID,
        market=A100_MARKET,
        price_x18=price_to_x18(price_usd),
    )

    try:
        results = client.commit_and_reveal(
            [update],
            verify=not args.no_verify,
            reveal_wait_seconds=args.reveal_wait_seconds,
        )
        append_log(results, price_usd, client)
    except Exception as exc:
        print("")
        print("=" * 60)
        print("ERROR: CUORACLE UPDATE FAILED")
        print("=" * 60)
        print(f"  {exc}")
        print("=" * 60)
        import traceback

        traceback.print_exc()
        sys.exit(1)

    print("")
    print("=" * 60)
    print("SUCCESS! A100 PRICE UPDATED ON BYTESTRIKE CUORACLE")
    print("=" * 60)
    for result in results:
        print(f"  {result.asset_name}: ${x18_to_usd(result.price_x18):.6f}/hr")
        print(f"  Commit: https://sepolia.etherscan.io/tx/{result.commit_tx_hash}")
        print(f"  Reveal: https://sepolia.etherscan.io/tx/{result.reveal_tx_hash}")
    print("=" * 60)


if __name__ == "__main__":
    main()
