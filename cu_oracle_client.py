#!/usr/bin/env python3
"""Small CuOracle commit/reveal client for ByteStrike Sepolia bots."""

import os
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Dict, Iterable, List, Optional, Tuple

from eth_account import Account
from web3 import Web3
from web3.exceptions import TimeExhausted

DEFAULT_CU_ORACLE_ADDRESS = "0x97f557594bA32e51c0eA215B1886111F24E957af"
DEFAULT_A100_ASSET_ID = "0x800dc08a7e47b959d22d840c1fc554e1273853b297d50418ebe9d86e8bf7394a"
DEFAULT_FALLBACK_RPC_URL = "https://ethereum-sepolia-rpc.publicnode.com"

CU_ORACLE_ABI = [
    {
        "type": "function",
        "name": "owner",
        "inputs": [],
        "outputs": [{"name": "", "type": "address"}],
        "stateMutability": "view",
    },
    {
        "type": "function",
        "name": "allowedRoles",
        "inputs": [{"name": "", "type": "address"}],
        "outputs": [{"name": "", "type": "bool"}],
        "stateMutability": "view",
    },
    {
        "type": "function",
        "name": "supportedAssets",
        "inputs": [{"name": "", "type": "bytes32"}],
        "outputs": [{"name": "", "type": "bool"}],
        "stateMutability": "view",
    },
    {
        "type": "function",
        "name": "getLatestPrice",
        "inputs": [{"name": "_assetId", "type": "bytes32"}],
        "outputs": [
            {
                "name": "",
                "type": "tuple",
                "components": [
                    {"name": "price", "type": "uint256"},
                    {"name": "lastUpdatedAt", "type": "uint256"},
                ],
            }
        ],
        "stateMutability": "view",
    },
    {
        "type": "function",
        "name": "lastCommitTimestamp",
        "inputs": [{"name": "", "type": "bytes32"}],
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
    },
    {
        "type": "function",
        "name": "minTimeInterval",
        "inputs": [],
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
    },
    {
        "type": "function",
        "name": "minCommitRevealDelay",
        "inputs": [],
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
    },
    {
        "type": "function",
        "name": "commitPrice",
        "inputs": [
            {"name": "_assetId", "type": "bytes32"},
            {"name": "_commit", "type": "bytes32"},
        ],
        "outputs": [],
        "stateMutability": "nonpayable",
    },
    {
        "type": "function",
        "name": "updatePrices",
        "inputs": [
            {"name": "_assetId", "type": "bytes32"},
            {"name": "_price", "type": "uint256"},
            {"name": "_nonce", "type": "bytes32"},
        ],
        "outputs": [],
        "stateMutability": "nonpayable",
    },
]


@dataclass(frozen=True)
class OracleUpdate:
    asset_name: str
    asset_id: str
    market: str
    price_x18: int

    @property
    def price_usd(self) -> float:
        return x18_to_usd(self.price_x18)


@dataclass(frozen=True)
class OracleUpdateResult:
    asset_name: str
    asset_id: str
    market: str
    price_x18: int
    nonce: str
    commit_hash: str
    commit_tx_hash: str
    reveal_tx_hash: str
    commit_block: int
    reveal_block: int
    commit_timestamp: int


def price_to_x18(price_usd: float) -> int:
    return int((Decimal(str(price_usd)) * Decimal(10**18)).to_integral_value())


def x18_to_usd(price_x18: int) -> float:
    return float(Decimal(price_x18) / Decimal(10**18))


class CuOracleClient:
    def __init__(self, rpc_url: Optional[str], private_key: str, contract_address: Optional[str] = None):
        endpoints = [rpc_url, os.getenv("SEPOLIA_FALLBACK_RPC_URL"), DEFAULT_FALLBACK_RPC_URL]
        self.w3 = None
        self.rpc_url = None
        for endpoint in endpoints:
            if not endpoint:
                continue
            candidate = Web3(Web3.HTTPProvider(endpoint, request_kwargs={"timeout": 30}))
            if candidate.is_connected():
                self.w3 = candidate
                self.rpc_url = endpoint
                break

        if self.w3 is None:
            raise ConnectionError("Failed to connect to Sepolia RPC")

        self.account = Account.from_key(private_key)
        self.address = self.account.address
        self.chain_id = int(self.w3.eth.chain_id)
        self.contract_address = Web3.to_checksum_address(contract_address or DEFAULT_CU_ORACLE_ADDRESS)
        self.contract = self.w3.eth.contract(address=self.contract_address, abi=CU_ORACLE_ABI)
        self.owner = self.contract.functions.owner().call()
        self.can_commit = bool(self.contract.functions.allowedRoles(self.address).call()) or (
            self.owner.lower() == self.address.lower()
        )
        self.can_reveal = self.owner.lower() == self.address.lower()

    def print_context(self) -> None:
        balance_eth = self.w3.from_wei(self.w3.eth.get_balance(self.address), "ether")
        print("=" * 60)
        print("BYTESTRIKE CUORACLE PRICE UPDATER")
        print("=" * 60)
        print(f"RPC: {self.rpc_url}")
        print(f"Chain ID: {self.chain_id}")
        print(f"Latest block: {self.w3.eth.block_number}")
        print(f"Updater address: {self.address}")
        print(f"Balance: {balance_eth:.6f} ETH")
        print(f"CuOracle: {self.contract_address}")
        print(f"Oracle owner: {self.owner}")
        print(f"Can commit: {self.can_commit}")
        print(f"Can reveal: {self.can_reveal}")
        print("=" * 60)

    def ensure_can_update(self) -> None:
        if not self.can_commit:
            raise PermissionError(f"{self.address} cannot commit prices")
        if not self.can_reveal:
            raise PermissionError(f"{self.address} cannot reveal prices; CuOracle.updatePrices is owner-only")

    def is_supported_asset(self, asset_id: str) -> bool:
        return bool(self.contract.functions.supportedAssets(asset_id).call())

    def get_latest_price(self, asset_id: str, block_identifier: Optional[int] = None) -> Tuple[int, int]:
        if block_identifier is None:
            price, last_updated = self.contract.functions.getLatestPrice(asset_id).call()
        else:
            price, last_updated = self.contract.functions.getLatestPrice(asset_id).call(
                block_identifier=block_identifier
            )
        return int(price), int(last_updated)

    def _build_fee_fields(self, multiplier_bps: int = 10_000) -> Dict[str, int]:
        priority_gwei = float(os.getenv("ORACLE_MAX_PRIORITY_FEE_GWEI", "0.05"))
        priority_fee = int(self.w3.to_wei(priority_gwei, "gwei"))
        gas_price = int(self.w3.eth.gas_price)
        latest_block = self.w3.eth.get_block("latest")
        base_fee = int(latest_block.get("baseFeePerGas", gas_price))
        max_fee = max(base_fee * 2 + priority_fee, gas_price + priority_fee)
        return {
            "maxFeePerGas": (max_fee * multiplier_bps) // 10_000,
            "maxPriorityFeePerGas": (priority_fee * multiplier_bps) // 10_000,
        }

    def _next_nonce(self) -> int:
        try:
            return int(self.w3.eth.get_transaction_count(self.address, "pending"))
        except TypeError:
            return int(self.w3.eth.get_transaction_count(self.address))

    def _send_transaction(self, func, gas_limit: int) -> Tuple[str, dict]:
        tx_nonce = self._next_nonce()
        timeout = int(os.getenv("ORACLE_TX_TIMEOUT_SECONDS", "300"))
        max_retries = int(os.getenv("ORACLE_TX_MAX_RETRIES", "4"))
        bump_bps = int(os.getenv("ORACLE_REPLACEMENT_FEE_BUMP_BPS", "1250"))

        for attempt in range(max_retries + 1):
            multiplier_bps = 10_000 + attempt * bump_bps
            tx = func.build_transaction(
                {
                    "from": self.address,
                    "nonce": tx_nonce,
                    "gas": gas_limit,
                    "chainId": self.chain_id,
                    **self._build_fee_fields(multiplier_bps=multiplier_bps),
                }
            )
            signed = self.account.sign_transaction(tx)
            raw_tx = getattr(signed, "raw_transaction", getattr(signed, "rawTransaction", signed))
            try:
                tx_hash = self.w3.eth.send_raw_transaction(raw_tx)
                receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=timeout)
                return tx_hash.hex(), dict(receipt)
            except TimeExhausted as exc:
                raise TimeoutError(f"Transaction {tx_hash.hex()} was not mined after {timeout}s") from exc
            except Exception as exc:
                message = str(exc).lower()
                if "replacement transaction underpriced" in message and attempt < max_retries:
                    print(f"  RPC rejected nonce {tx_nonce} as underpriced; retrying with higher fees...")
                    time.sleep(2)
                    continue
                if "nonce too low" in message and attempt < max_retries:
                    tx_nonce = self._next_nonce()
                    print(f"  RPC nonce moved; retrying with nonce {tx_nonce}...")
                    time.sleep(2)
                    continue
                raise

        raise RuntimeError("unreachable transaction retry state")

    def _sleep_until_commit_allowed(self, asset_id: str) -> None:
        last_commit = int(self.contract.functions.lastCommitTimestamp(asset_id).call())
        min_interval = int(self.contract.functions.minTimeInterval().call())
        latest_ts = int(self.w3.eth.get_block("latest")["timestamp"])
        ready_at = last_commit + min_interval
        if ready_at > latest_ts:
            wait_seconds = ready_at - latest_ts + 1
            print(f"Waiting {wait_seconds}s for CuOracle minTimeInterval...")
            time.sleep(wait_seconds)

    def _commit_hash(self, price_x18: int, nonce: bytes) -> bytes:
        return Web3.solidity_keccak(["uint256", "bytes32"], [price_x18, nonce])

    def _verify_price(self, update: OracleUpdate, reveal_block: int) -> Tuple[int, int]:
        for _ in range(5):
            try:
                price, updated_at = self.get_latest_price(update.asset_id, block_identifier=reveal_block)
                if price == update.price_x18:
                    return price, updated_at
            except Exception:
                pass
            price, updated_at = self.get_latest_price(update.asset_id)
            if price == update.price_x18:
                return price, updated_at
            time.sleep(3)
        return self.get_latest_price(update.asset_id)

    def commit_and_reveal(
        self,
        updates: Iterable[OracleUpdate],
        verify: bool = True,
        reveal_wait_seconds: Optional[int] = None,
    ) -> List[OracleUpdateResult]:
        self.ensure_can_update()
        prepared = list(updates)
        if not prepared:
            return []

        min_delay = int(self.contract.functions.minCommitRevealDelay().call())
        configured_wait = int(os.getenv("ORACLE_REVEAL_WAIT_SECONDS", "3"))
        wait_seconds = max(min_delay, reveal_wait_seconds if reveal_wait_seconds is not None else configured_wait)

        pending = []
        print("Prepared CuOracle updates:")
        for update in prepared:
            if not self.is_supported_asset(update.asset_id):
                raise ValueError(f"{update.asset_name} asset is not supported: {update.asset_id}")
            current_price, last_updated = self.get_latest_price(update.asset_id)
            current_usd = x18_to_usd(current_price)
            delta_pct = ((update.price_x18 - current_price) / current_price) * 100 if current_price else 0.0
            print(
                f"  {update.asset_name} ({update.market}): "
                f"${current_usd:.6f}/hr -> ${update.price_usd:.6f}/hr ({delta_pct:+.2f}%)"
            )
            if last_updated:
                print(f"    Current commit timestamp age: {int(time.time()) - int(last_updated)}s")

        print("Committing prices...")
        for update in prepared:
            self._sleep_until_commit_allowed(update.asset_id)
            nonce = os.urandom(32)
            commit_hash = self._commit_hash(update.price_x18, nonce)
            tx_hash, receipt = self._send_transaction(
                self.contract.functions.commitPrice(update.asset_id, commit_hash),
                gas_limit=130_000,
            )
            print(f"  commit {update.asset_name}: {tx_hash} (gas {receipt['gasUsed']:,})")
            pending.append((update, nonce, commit_hash, tx_hash, receipt))

        print(f"Waiting {wait_seconds}s before reveal...")
        time.sleep(wait_seconds)

        print("Revealing prices...")
        results: List[OracleUpdateResult] = []
        for update, nonce, commit_hash, commit_tx_hash, commit_receipt in pending:
            tx_hash, receipt = self._send_transaction(
                self.contract.functions.updatePrices(update.asset_id, update.price_x18, nonce),
                gas_limit=170_000,
            )
            print(f"  reveal {update.asset_name}: {tx_hash} (gas {receipt['gasUsed']:,})")

            commit_timestamp = 0
            if verify:
                verified_price, commit_timestamp = self._verify_price(update, int(receipt["blockNumber"]))
                if verified_price != update.price_x18:
                    raise RuntimeError(
                        f"Verification failed for {update.asset_name}: "
                        f"expected {update.price_x18}, got {verified_price}"
                    )
                print(
                    f"  verified {update.asset_name}: "
                    f"${x18_to_usd(verified_price):.6f}/hr at commit timestamp {commit_timestamp}"
                )

            results.append(
                OracleUpdateResult(
                    asset_name=update.asset_name,
                    asset_id=update.asset_id,
                    market=update.market,
                    price_x18=update.price_x18,
                    nonce=f"0x{nonce.hex()}",
                    commit_hash=commit_hash.hex(),
                    commit_tx_hash=commit_tx_hash,
                    reveal_tx_hash=tx_hash,
                    commit_block=int(commit_receipt["blockNumber"]),
                    reveal_block=int(receipt["blockNumber"]),
                    commit_timestamp=int(commit_timestamp),
                )
            )

        return results
