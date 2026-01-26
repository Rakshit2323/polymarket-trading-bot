#!/usr/bin/env -S uv --quiet run --active --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "py-builder-relayer-client",
#   "py-builder-signing-sdk",
#   "poly-eip712-structs",
#   "requests",
#   "web3",
# ]
# ///

import os
import re
import signal
import sys
import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

import requests
from py_builder_relayer_client.client import RelayClient
from py_builder_relayer_client.models import OperationType, SafeTransaction
from py_builder_signing_sdk.config import BuilderConfig
from py_builder_signing_sdk.sdk_types import BuilderApiKeyCreds, RemoteBuilderConfig
from web3 import Web3

USER_AGENT = "Mozilla/5.0"
DEFAULT_DATA_API_URL = "https://data-api.polymarket.com"
DEFAULT_RELAYER_URL = "https://relayer-v2.polymarket.com"
DEFAULT_CHAIN_ID = 137
DEFAULT_CLAIM_LIMIT = 500
MAX_CLAIM_OFFSET = 10000
PARENT_COLLECTION_ID = "0x" + ("0" * 64)

CONTRACTS_BY_CHAIN: Dict[int, Dict[str, str]] = {
    137: {
        "collateral": "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
        "ctf": "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045",
    },
    80002: {
        "collateral": "0x9c4e1703476e875070ee25b56a58b008cfb8fa78",
        "ctf": "0x69308FB512518e39F9b16112fA8d994F4e2Bf8bB",
    },
}

CTF_ABI = [
    {
        "inputs": [
            {"internalType": "address", "name": "collateralToken", "type": "address"},
            {"internalType": "bytes32", "name": "parentCollectionId", "type": "bytes32"},
            {"internalType": "bytes32", "name": "conditionId", "type": "bytes32"},
            {"internalType": "uint256[]", "name": "indexSets", "type": "uint256[]"},
        ],
        "name": "redeemPositions",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    }
]


@dataclass
class ClaimItem:
    condition_id: str
    index_sets: List[int]
    positions: List[Dict]


@dataclass
class Config:
    data_api_url: str
    claim_every: Optional[float]
    claim_size_threshold: float
    claim_limit: int
    enable_claims: bool
    private_key: Optional[str]
    funder: str
    relayer_url: str
    relayer_chain_id: int
    relayer_deploy_safe: bool
    collateral_token: str
    ctf_address: str


def env(name: str) -> Optional[str]:
    value = os.getenv(name)
    if value is None:
        return None
    value = value.strip()
    return value or None


def first_non_empty(*values: Optional[str]) -> Optional[str]:
    for value in values:
        if value:
            return value
    return None


def parse_bool(value: Optional[str], default: bool = False) -> bool:
    if value is None:
        return default
    val = value.strip().lower()
    if val in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if val in {"0", "false", "f", "no", "n", "off"}:
        return False
    raise ValueError(f"invalid boolean: {value}")


def parse_int(value: Optional[str], default: int) -> int:
    if value is None:
        return default
    return int(value.strip())


def parse_float(value: Optional[str], default: float) -> float:
    if value is None:
        return default
    return float(value.strip())


def parse_duration(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    raw = value.strip()
    if raw == "":
        return None
    pattern = re.compile(r"(\d+(?:\.\d+)?)(ms|s|m|h|d)")
    total = 0.0
    idx = 0
    for match in pattern.finditer(raw):
        if match.start() != idx:
            raise ValueError(f"invalid duration: {value}")
        amount = float(match.group(1))
        unit = match.group(2)
        if unit == "ms":
            total += amount / 1000.0
        elif unit == "s":
            total += amount
        elif unit == "m":
            total += amount * 60.0
        elif unit == "h":
            total += amount * 3600.0
        elif unit == "d":
            total += amount * 86400.0
        else:
            raise ValueError(f"invalid duration unit: {unit}")
        idx = match.end()
    if idx != len(raw):
        raise ValueError(f"invalid duration: {value}")
    return total


def normalize_address(value: str, label: str) -> str:
    if not value:
        raise ValueError(f"{label} is required")
    if not Web3.is_address(value):
        raise ValueError(f"invalid {label} address: {value}")
    return Web3.to_checksum_address(value)


def resolve_contracts(chain_id: int) -> Tuple[str, str]:
    override_collateral = env("COLLATERAL_TOKEN")
    override_ctf = env("CTF_ADDRESS")
    if override_collateral and override_ctf:
        return normalize_address(override_collateral, "COLLATERAL_TOKEN"), normalize_address(override_ctf, "CTF_ADDRESS")
    defaults = CONTRACTS_BY_CHAIN.get(chain_id)
    if not defaults:
        raise ValueError("missing COLLATERAL_TOKEN/CTF_ADDRESS for chain_id")
    collateral = override_collateral or defaults["collateral"]
    ctf = override_ctf or defaults["ctf"]
    return normalize_address(collateral, "collateral"), normalize_address(ctf, "ctf")


def validate_wallet_type() -> None:
    raw = first_non_empty(env("RELAYER_TX_TYPE"), env("RELAYER_WALLET_TYPE"))
    if raw:
        lowered = raw.strip().lower()
        if lowered in {"safe", "gnosis_safe", "poly_gnosis_safe", "2"}:
            return
        raise ValueError(
            "RELAYER_TX_TYPE=PROXY is not supported by this Python relayer client; "
            "use the JS relayer client for proxy wallets"
        )

    sig_type_raw = first_non_empty(env("CLOB_SIGNATURE_TYPE"), env("SIGNATURE_TYPE"))
    if sig_type_raw:
        sig_type = int(sig_type_raw)
        if sig_type == 1:
            raise ValueError(
                "CLOB_SIGNATURE_TYPE=1 indicates a proxy wallet; "
                "use the JS relayer client for proxy wallets"
            )


def load_config() -> Config:
    data_api_url = env("DATA_API_URL") or DEFAULT_DATA_API_URL
    claim_every = parse_duration(env("CLAIM_EVERY"))
    claim_size_threshold = parse_float(env("CLAIM_SIZE_THRESHOLD"), 0.0)

    claim_limit = parse_int(env("CLAIM_LIMIT"), DEFAULT_CLAIM_LIMIT)
    if claim_limit <= 0:
        claim_limit = DEFAULT_CLAIM_LIMIT
    if claim_limit > DEFAULT_CLAIM_LIMIT:
        claim_limit = DEFAULT_CLAIM_LIMIT

    enable_claims = parse_bool(first_non_empty(env("ENABLE_CLAIMS"), env("ENABLE_TRADING")), False)

    private_key = first_non_empty(env("CLOB_PRIVATE_KEY"), env("PRIVATE_KEY"))
    funder = first_non_empty(env("CLOB_FUNDER"), env("FUNDER"))
    if not funder:
        raise ValueError("FUNDER/CLOB_FUNDER is required for claim lookup")
    funder = normalize_address(funder, "funder")

    relayer_url = env("RELAYER_URL") or DEFAULT_RELAYER_URL
    relayer_chain_id = parse_int(first_non_empty(env("RELAYER_CHAIN_ID"), env("POLYGON_CHAIN_ID")), DEFAULT_CHAIN_ID)
    validate_wallet_type()
    relayer_deploy_safe = parse_bool(env("RELAYER_DEPLOY_SAFE"), False)

    collateral_token, ctf_address = resolve_contracts(relayer_chain_id)

    return Config(
        data_api_url=data_api_url,
        claim_every=claim_every,
        claim_size_threshold=claim_size_threshold,
        claim_limit=claim_limit,
        enable_claims=enable_claims,
        private_key=private_key,
        funder=funder,
        relayer_url=relayer_url,
        relayer_chain_id=relayer_chain_id,
        relayer_deploy_safe=relayer_deploy_safe,
        collateral_token=collateral_token,
        ctf_address=ctf_address,
    )


def build_builder_config() -> BuilderConfig:
    remote_url = first_non_empty(env("BUILDER_SIGNING_URL"), env("POLY_BUILDER_SIGNING_URL"))
    if remote_url:
        token = first_non_empty(env("BUILDER_SIGNING_TOKEN"), env("POLY_BUILDER_SIGNING_TOKEN"))
        return BuilderConfig(
            remote_builder_config=RemoteBuilderConfig(
                url=remote_url,
                token=token,
            )
        )

    key = first_non_empty(env("POLY_BUILDER_API_KEY"), env("BUILDER_API_KEY"))
    secret = first_non_empty(env("POLY_BUILDER_SECRET"), env("BUILDER_SECRET"))
    passphrase = first_non_empty(
        env("POLY_BUILDER_PASSPHRASE"),
        env("BUILDER_PASSPHRASE"),
        env("BUILDER_PASS_PHRASE"),
    )
    if not key or not secret:
        raise ValueError("missing builder credentials: set POLY_BUILDER_API_KEY/POLY_BUILDER_SECRET or BUILDER_SIGNING_URL")
    creds = BuilderApiKeyCreds(key=key, secret=secret, passphrase=passphrase or "")
    return BuilderConfig(local_builder_creds=creds)


def build_relayer_client(cfg: Config) -> RelayClient:
    if not cfg.private_key:
        raise ValueError("PRIVATE_KEY/CLOB_PRIVATE_KEY is required to sign relayer transactions")
    builder_config = build_builder_config()
    return RelayClient(
        cfg.relayer_url,
        cfg.relayer_chain_id,
        cfg.private_key,
        builder_config,
    )


def fetch_redeemable_positions(cfg: Config) -> List[Dict]:
    session = requests.Session()
    session.headers.update({
        "Accept": "application/json",
        "User-Agent": USER_AGENT,
    })

    base_url = cfg.data_api_url.rstrip("/")
    out: List[Dict] = []
    offset = 0
    while True:
        params = {
            "user": cfg.funder,
            "redeemable": "true",
            "sizeThreshold": str(cfg.claim_size_threshold),
            "limit": str(cfg.claim_limit),
            "offset": str(offset),
        }
        resp = session.get(f"{base_url}/positions", params=params, timeout=12)
        if resp.status_code != 200:
            body = (resp.text or "")[:8192]
            raise RuntimeError(f"data api {resp.url}: status={resp.status_code} body={body!r}")
        batch = resp.json()
        if not isinstance(batch, list):
            raise RuntimeError(f"data api unexpected response: {type(batch)}")
        out.extend(batch)
        if len(batch) < cfg.claim_limit:
            break
        offset += len(batch)
        if offset >= MAX_CLAIM_OFFSET:
            break
    return out


def parse_condition_id(raw: str) -> Optional[str]:
    if not raw:
        return None
    raw = raw.strip()
    if not raw.startswith("0x"):
        return None
    hex_str = raw[2:]
    if len(hex_str) != 64:
        return None
    try:
        bytes.fromhex(hex_str)
    except ValueError:
        return None
    return raw


def build_claim_items(positions: Iterable[Dict]) -> Tuple[List[ClaimItem], int, int]:
    buckets: Dict[str, Dict] = {}
    skipped_neg = 0
    skipped_invalid = 0

    for pos in positions:
        cond = parse_condition_id(str(pos.get("conditionId") or ""))
        if not cond:
            skipped_invalid += 1
            continue
        outcome_index = pos.get("outcomeIndex")
        try:
            outcome_index = int(outcome_index)
        except (TypeError, ValueError):
            skipped_invalid += 1
            continue
        if outcome_index < 0 or outcome_index > 255:
            skipped_invalid += 1
            continue

        key = cond.lower()
        bucket = buckets.get(key)
        if bucket is None:
            bucket = {"negative_risk": False, "index_sets": set(), "positions": [], "condition_id": cond}
            buckets[key] = bucket

        if bool(pos.get("negativeRisk")):
            bucket["negative_risk"] = True
            bucket["index_sets"].clear()
            bucket["positions"].clear()
            skipped_neg += 1
            continue
        if bucket["negative_risk"]:
            skipped_neg += 1
            continue

        bucket["index_sets"].add(1 << outcome_index)
        bucket["positions"].append(pos)

    items: List[ClaimItem] = []
    for bucket in buckets.values():
        if bucket["negative_risk"]:
            continue
        index_sets = sorted(bucket["index_sets"])
        if not index_sets or not bucket["positions"]:
            continue
        items.append(ClaimItem(bucket["condition_id"], index_sets, bucket["positions"]))

    items.sort(key=lambda item: item.condition_id)
    return items, skipped_neg, skipped_invalid


def summarize_positions(positions: List[Dict]) -> Tuple[str, str, float]:
    if not positions:
        return "", "", 0.0
    title = str(positions[0].get("title") or "").strip()
    outcome = str(positions[0].get("outcome") or "").strip()
    total = 0.0
    for pos in positions:
        try:
            total += float(pos.get("size") or 0.0)
        except (TypeError, ValueError):
            continue
        if not title:
            title = str(pos.get("title") or "").strip()
        if not outcome:
            outcome = str(pos.get("outcome") or "").strip()
    return title, outcome, total


def format_index_sets(index_sets: List[int]) -> str:
    return "[" + ",".join(str(v) for v in index_sets) + "]"


def encode_redeem_data(ctf_address: str, collateral: str, condition_id: str, index_sets: List[int]) -> str:
    contract = Web3().eth.contract(address=ctf_address, abi=CTF_ABI)
    return contract.functions.redeemPositions(
        collateral,
        PARENT_COLLECTION_ID,
        condition_id,
        index_sets,
    )._encode_transaction_data()


def redeem_positions(cfg: Config, items: List[ClaimItem]) -> None:
    client = build_relayer_client(cfg)

    expected_safe = client.get_expected_safe()
    if cfg.funder.lower() != expected_safe.lower():
        raise RuntimeError(
            f"funder {cfg.funder} does not match expected safe {expected_safe}; "
            "check PRIVATE_KEY/CLOB_PRIVATE_KEY"
        )

    if cfg.relayer_deploy_safe:
        response = client.deploy()
        result = response.wait()
        proxy_address = getattr(result, "proxy_address", None)
        if proxy_address:
            print(f"[claim] safe deployed proxy={proxy_address}")

    for item in items:
        data = encode_redeem_data(cfg.ctf_address, cfg.collateral_token, item.condition_id, item.index_sets)
        tx = SafeTransaction(
            to=cfg.ctf_address,
            operation=OperationType.Call,
            data=data,
            value="0",
        )
        description = f"Redeem {item.condition_id}"
        try:
            response = client.execute([tx], description)
            result = response.wait()
            tx_hash = getattr(response, "transaction_hash", None) or getattr(response, "hash", None)
            if isinstance(result, dict):
                tx_hash = result.get("transactionHash") or result.get("transaction_hash") or tx_hash
            if tx_hash:
                print(f"[claim] sent tx={tx_hash} condition={item.condition_id}")
            else:
                print(f"[claim] sent condition={item.condition_id}")
        except Exception as exc:
            print(f"[warn] relayer exec failed condition={item.condition_id}: {exc}")


def run_once(cfg: Config) -> None:
    positions = fetch_redeemable_positions(cfg)
    items, skipped_neg, skipped_invalid = build_claim_items(positions)
    if not items:
        print(f"[claim] user={cfg.funder} redeemable=0 skipped_neg={skipped_neg} skipped_invalid={skipped_invalid}")
        return

    print(
        f"[claim] user={cfg.funder} redeemable={len(positions)} buckets={len(items)} "
        f"skipped_neg={skipped_neg} skipped_invalid={skipped_invalid}"
    )
    for item in items:
        title, outcome, size = summarize_positions(item.positions)
        print(
            f"[claim] ready condition={item.condition_id} indexSets={format_index_sets(item.index_sets)} "
            f"title={title!r} outcome={outcome!r} size={size:.6f}"
        )

    if not cfg.enable_claims:
        print("[claim] dry-run: set ENABLE_CLAIMS=true to submit transactions")
        return

    redeem_positions(cfg, items)


def load_dotenv_file(path: str = ".env") -> None:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            lines = handle.readlines()
    except FileNotFoundError:
        return

    for line in lines:
        raw = line.strip()
        if not raw or raw.startswith("#"):
            continue
        if raw.startswith("export "):
            raw = raw[len("export ") :].lstrip()
        if "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        key = key.strip()
        if not key or key in os.environ:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", "\""}:
            value = value[1:-1]
        os.environ[key] = value


def main() -> None:
    load_dotenv_file()
    cfg = load_config()

    stop = False

    def handle_signal(_sig, _frame):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    if not cfg.claim_every or cfg.claim_every <= 0:
        run_once(cfg)
        return

    while not stop:
        run_once(cfg)
        if stop:
            break
        time.sleep(cfg.claim_every)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"[fatal] {exc}")
        sys.exit(1)
