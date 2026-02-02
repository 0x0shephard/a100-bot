#!/usr/bin/env python3
"""A100 GPU Oracle price updater script.

Updates the A100 hourly rental price on the MultiAssetOracle contract.

Usage:
    python scripts/update_a100_oracle.py --price 1.76
"""

import json
import os
import sys
from datetime import datetime, timezone

from dotenv import load_dotenv
from eth_account import Account
from web3 import Web3

load_dotenv()

# Configuration
SEPOLIA_RPC_URL = os.getenv("SEPOLIA_RPC_URL", "https://rpc.sepolia.org")
PRIVATE_KEY = os.getenv("ORACLE_UPDATER_PRIVATE_KEY") or os.getenv("PRIVATE_KEY") or os.getenv("WALLET_PRIVATE_KEY")
MULTI_ASSET_ORACLE = "0xB44d652354d12Ac56b83112c6ece1fa2ccEfc683"
A100_ASSET_ID = "0x2d2dcb773769dec98aac013f27fbeba7c0dfe1d4edf46e4d3bfee86443ac6cde"
PRICE_DECIMALS = 18

# ABI for MultiAssetOracle
ORACLE_ABI = [
    {
        "type": "function",
        "name": "updatePrice",
        "inputs": [
            {"name": "assetId", "type": "bytes32"},
            {"name": "newPrice", "type": "uint256"},
        ],
        "outputs": [],
        "stateMutability": "nonpayable",
    },
    {
        "type": "function",
        "name": "getPriceData",
        "inputs": [{"name": "assetId", "type": "bytes32"}],
        "outputs": [
            {"name": "price", "type": "uint256"},
            {"name": "updatedAt", "type": "uint256"},
        ],
        "stateMutability": "view",
    },
]


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Update A100 GPU price on oracle")
    parser.add_argument("--price", type=float, required=True, help="Price in USD/hour (e.g., 1.76)")
    args = parser.parse_args()

    if not PRIVATE_KEY:
        print("ERROR: Set PRIVATE_KEY environment variable")
        sys.exit(1)

    if args.price <= 0:
        print("ERROR: Price must be positive")
        sys.exit(1)

    # Connect
    w3 = Web3(Web3.HTTPProvider(SEPOLIA_RPC_URL))
    if not w3.is_connected():
        print("ERROR: Failed to connect to Sepolia")
        sys.exit(1)

    account = Account.from_key(PRIVATE_KEY)
    contract = w3.eth.contract(address=MULTI_ASSET_ORACLE, abi=ORACLE_ABI)
    asset_id_bytes = bytes.fromhex(A100_ASSET_ID[2:])

    print("=" * 50)
    print("A100 ORACLE PRICE UPDATER")
    print("=" * 50)
    print(f"Oracle: {MULTI_ASSET_ORACLE}")
    print(f"Updater: {account.address}")
    print(f"Balance: {w3.from_wei(w3.eth.get_balance(account.address), 'ether'):.4f} ETH")

    # Get current price
    try:
        price_raw, last_updated = contract.functions.getPriceData(asset_id_bytes).call()
        current_price = price_raw / 10**PRICE_DECIMALS
        print(f"Current A100 Price: ${current_price:.4f}/hr")
    except:
        print("Current A100 Price: Not set")

    # Update price
    new_price_scaled = int(args.price * 10**PRICE_DECIMALS)
    print(f"New A100 Price: ${args.price:.4f}/hr")
    print("\nSending transaction...")

    # Build and send tx
    tx = contract.functions.updatePrice(asset_id_bytes, new_price_scaled).build_transaction({
        "from": account.address,
        "nonce": w3.eth.get_transaction_count(account.address),
        "gas": 100000,
        "maxFeePerGas": w3.eth.gas_price * 2,
        "maxPriorityFeePerGas": w3.to_wei(1, "gwei"),
        "chainId": 11155111,
    })

    signed = account.sign_transaction(tx)
    raw_tx = signed.raw_transaction if hasattr(signed, "raw_transaction") else signed.rawTransaction
    tx_hash = w3.eth.send_raw_transaction(raw_tx)
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=180)

    print(f"\nSUCCESS!")
    print(f"Tx: https://sepolia.etherscan.io/tx/{tx_hash.hex()}")
    print(f"Gas used: {receipt['gasUsed']}")
    print("=" * 50)


if __name__ == "__main__":
    main()
