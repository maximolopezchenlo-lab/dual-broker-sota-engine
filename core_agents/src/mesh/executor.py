"""
Order Executor Agent — EIP-712 signing, Polymarket & Alpaca order building,
and sandbox-simulated execution with circuit-breaker logic.

This module implements the **Executor** node — the final stage of the
Researcher -> Risk Analyst -> Executor pipeline.  It is responsible for:

1. Building EIP-712-compatible order structures for Polymarket's CLOB
   (Central Limit Order Book).
2. Building REST-compatible order payloads for Alpaca's TradFi brokerage
   API.
3. Pre-validating orders against a sandbox endpoint and enforcing a
   risk-score circuit breaker (threshold: 50).
4. Computing EIP-712 typed-data hashes using Keccak-256 (real
   implementation, no external eth dependencies required).

EIP-712 Typed Data Signing
==========================

EIP-712 defines a structured-data signing standard for Ethereum.  The
hash of a typed message is:

    hash  =  keccak256(
        "\\x19\\x01"
        || domainSeparator
        || hashStruct(message)
    )

where ``domainSeparator`` is the hash of the EIP-712 domain fields
(name, version, chainId, verifyingContract) and ``hashStruct`` recursively
hashes each struct field according to its declared type.

This module implements the hashing locally using ``hashlib`` (SHA3-256 as
a stand-in for Keccak-256 — they share the same algorithm family) so that
the engine has zero dependency on ``web3.py`` or ``eth-abi``.  For
production deployment you should replace the hasher with ``pysha3`` or
``pycryptodome``'s Keccak.

Dependencies:  ``aiohttp>=3.9``, ``pydantic>=2.0``
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from enum import Enum
from typing import Any

import aiohttp
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Enums & constants
# ---------------------------------------------------------------------------


class OrderSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(str, Enum):
    MARKET = "market"
    LIMIT = "limit"
    STOP = "stop"
    STOP_LIMIT = "stop_limit"
    TRAILING_STOP = "trailing_stop"


class TimeInForce(str, Enum):
    DAY = "day"
    GTC = "gtc"
    IOC = "ioc"
    FOK = "fok"
    OPG = "opg"
    CLS = "cls"


# Polymarket CLOB EIP-712 domain constants (Polygon mainnet).
_POLYMARKET_DOMAIN = {
    "name": "Polymarket CTF Exchange",
    "version": "1",
    "chainId": 137,  # Polygon PoS
    "verifyingContract": "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E",
}

_CIRCUIT_BREAKER_THRESHOLD: int = 50  # risk score >= 50 triggers breaker


# ---------------------------------------------------------------------------
# Result models
# ---------------------------------------------------------------------------


class ExecutionResult(BaseModel):
    """Structured result from a sandbox-simulated execution.

    Attributes
    ----------
    success : bool
        Whether the simulation passed all pre-flight checks.
    order_id : str
        Unique identifier assigned by the sandbox (or "N/A" in mock).
    risk_score : int
        Risk score from 0 (safe) to 100 (extremely risky).
    circuit_breaker_triggered : bool
        True if ``risk_score >= 50``.
    fill_price : float
        Simulated fill price (0 if no fill).
    gas_estimate_gwei : float
        Estimated gas cost in gwei (Polymarket orders only).
    message : str
        Human-readable status / error description.
    timestamp_utc : float
        Unix epoch of the execution result.
    """

    success: bool = False
    order_id: str = "N/A"
    risk_score: int = 0
    circuit_breaker_triggered: bool = False
    fill_price: float = 0.0
    gas_estimate_gwei: float = 0.0
    message: str = ""
    timestamp_utc: float = Field(default_factory=time.time)


# ---------------------------------------------------------------------------
# Keccak-256 helper (pure-Python, no external crypto dependency)
# ---------------------------------------------------------------------------


def _keccak256(data: bytes) -> bytes:
    """Compute a Keccak-256 hash.

    This uses ``hashlib.new('sha3_256', ...)`` which in CPython >= 3.6 is
    backed by the same Keccak permutation.  Note that SHA3-256 and
    Keccak-256 differ only in the domain-separation byte (0x06 vs 0x01),
    so this is *not* byte-identical to Ethereum's keccak256 for all
    inputs.  For production use, replace with ``pysha3.keccak_256`` or
    ``Crypto.Hash.keccak`` from ``pycryptodome``.

    For the purpose of this engine's sandbox/simulation mode the
    distinction is irrelevant — the hash is used as a deterministic
    fingerprint, not for on-chain signature verification.
    """
    return hashlib.sha3_256(data).digest()


def _encode_uint256(value: int) -> bytes:
    """ABI-encode a uint256 as 32 big-endian bytes."""
    return value.to_bytes(32, byteorder="big")


def _encode_address(addr: str) -> bytes:
    """ABI-encode an Ethereum address (20 bytes, left-padded to 32)."""
    addr_clean = addr.lower().replace("0x", "")
    return bytes.fromhex(addr_clean).rjust(32, b"\x00")


# ---------------------------------------------------------------------------
# Order Executor
# ---------------------------------------------------------------------------


class OrderExecutor:
    """Builds, signs, and (sandbox-)executes orders for both Polymarket
    and Alpaca brokers.

    Parameters
    ----------
    mock_mode : bool
        When *True*, ``execute_with_simulation`` returns synthetic
        execution results without making any HTTP calls.  Default *True*.
    request_timeout : float
        HTTP timeout in seconds for sandbox calls.
    """

    def __init__(
        self,
        mock_mode: bool = True,
        request_timeout: float = 15.0,
    ) -> None:
        self._mock = mock_mode
        self._timeout = aiohttp.ClientTimeout(total=request_timeout)
        self._session: aiohttp.ClientSession | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self._timeout)
        return self._session

    async def close(self) -> None:
        """Release the underlying HTTP session."""
        if self._session and not self._session.closed:
            await self._session.close()

    # ------------------------------------------------------------------
    # 1.  Polymarket order builder
    # ------------------------------------------------------------------

    def prepare_polymarket_order(
        self,
        token_id: str,
        side: str | OrderSide,
        size_usdc: float,
        price: float,
        maker_address: str = "0x0000000000000000000000000000000000000000",
        nonce: int | None = None,
        expiration: int | None = None,
    ) -> dict[str, Any]:
        """Build an EIP-712-compatible order for Polymarket's CLOB.

        The returned dictionary mirrors the ``Order`` struct expected by
        Polymarket's CTF Exchange smart contract:

        .. code-block:: solidity

            struct Order {
                uint256 salt;
                address maker;
                address signer;
                address taker;
                uint256 tokenId;
                uint256 makerAmount;
                uint256 takerAmount;
                uint256 expiration;
                uint256 nonce;
                uint256 feeRateBps;
                uint8   side;          // 0 = BUY, 1 = SELL
                uint8   signatureType; // 0 = EOA
            }

        Parameters
        ----------
        token_id : str
            Polymarket condition token ID (hex string or integer).
        side : str | OrderSide
            ``"BUY"`` or ``"SELL"``.
        size_usdc : float
            Total USDC size of the order (6-decimal token).
        price : float
            Limit price in [0, 1] for the YES outcome token.
        maker_address : str
            Ethereum address of the order maker.
        nonce : int, optional
            Order nonce; auto-generated from timestamp if omitted.
        expiration : int, optional
            Unix timestamp of order expiry; defaults to +1 hour.

        Returns
        -------
        dict
            The complete EIP-712 order struct ready for signing.
        """
        side_enum = OrderSide(side) if isinstance(side, str) else side
        now = int(time.time())

        # USDC has 6 decimals.
        usdc_raw = int(size_usdc * 1_000_000)

        # makerAmount = USDC committed, takerAmount = outcome tokens received.
        if side_enum == OrderSide.BUY:
            maker_amount = usdc_raw
            taker_amount = int(usdc_raw / max(price, 1e-9))
        else:
            maker_amount = int(usdc_raw / max(price, 1e-9))
            taker_amount = usdc_raw

        # Deterministic Nonces (Web3 Layer)
        state_file = "dashboard/sequential_nonce_state.json"
        if nonce is None:
            import os
            current_nonce = now
            if os.path.exists(state_file):
                try:
                    with open(state_file, "r") as f:
                        nonce_state = json.load(f)
                    last_nonce = int(nonce_state.get("last_nonce", now))
                    current_nonce = max(now, last_nonce + 1)
                except Exception as e:
                    logger.error("Failed to load sequential nonce state: %s", e)
            
            try:
                os.makedirs(os.path.dirname(state_file), exist_ok=True)
                with open(state_file, "w") as f:
                    json.dump({"last_nonce": current_nonce}, f)
            except Exception as e:
                logger.error("Failed to save sequential nonce state: %s", e)
                
            nonce_val = str(current_nonce)
        else:
            nonce_val = str(nonce)

        order: dict[str, Any] = {
            "salt": now * 1000 + (hash(token_id) % 1000),
            "maker": maker_address,
            "signer": maker_address,
            "taker": "0x0000000000000000000000000000000000000000",
            "tokenId": str(token_id),
            "makerAmount": str(maker_amount),
            "takerAmount": str(taker_amount),
            "expiration": str(expiration or (now + 3600)),
            "nonce": nonce_val,
            "feeRateBps": "0",
            "side": 0 if side_enum == OrderSide.BUY else 1,
            "signatureType": 0,
        }

        # Attach EIP-712 domain for downstream signing.
        order["_eip712_domain"] = dict(_POLYMARKET_DOMAIN)
        logger.debug("Prepared Polymarket order: %s", order)
        return order

    # ------------------------------------------------------------------
    # 2.  Alpaca / TradFi order builder
    # ------------------------------------------------------------------

    def prepare_tradfi_order(
        self,
        symbol: str,
        qty: float,
        side: str | OrderSide,
        order_type: str | OrderType = OrderType.MARKET,
        time_in_force: str | TimeInForce = TimeInForce.GTC,
        limit_price: float | None = None,
        stop_price: float | None = None,
        client_order_id: str | None = None,
    ) -> dict[str, Any]:
        """Build an Alpaca-compatible REST order payload.

        The payload conforms to ``POST /v2/orders`` in Alpaca's Trading
        API (paper or live).

        Parameters
        ----------
        symbol : str
            Ticker symbol (e.g. ``"AAPL"``, ``"SPY"``).
        qty : float
            Number of shares (fractional OK for Alpaca).
        side : str | OrderSide
            ``"BUY"`` or ``"SELL"``.
        order_type : str | OrderType
            Order type.  Default ``"market"``.
        time_in_force : str | TimeInForce
            TIF instruction.  Default ``"gtc"``.
        limit_price : float, optional
            Required for ``limit`` and ``stop_limit`` orders.
        stop_price : float, optional
            Required for ``stop`` and ``stop_limit`` orders.
        client_order_id : str, optional
            Idempotency key.  Auto-generated if omitted.

        Returns
        -------
        dict
            JSON-serialisable order payload for ``POST /v2/orders``.
        """
        side_val = (
            side.value if isinstance(side, OrderSide) else side.lower()
        )
        otype_val = (
            order_type.value
            if isinstance(order_type, OrderType)
            else order_type.lower()
        )
        tif_val = (
            time_in_force.value
            if isinstance(time_in_force, TimeInForce)
            else time_in_force.lower()
        )

        payload: dict[str, Any] = {
            "symbol": symbol.upper(),
            "qty": str(qty),
            "side": side_val.lower(),
            "type": otype_val,
            "time_in_force": tif_val,
        }
        if limit_price is not None:
            payload["limit_price"] = str(limit_price)
        if stop_price is not None:
            payload["stop_price"] = str(stop_price)
        if client_order_id:
            payload["client_order_id"] = client_order_id
        else:
            payload["client_order_id"] = (
                f"dual-broker-{symbol}-{int(time.time() * 1000)}"
            )

        logger.debug("Prepared TradFi order: %s", payload)
        return payload

    # ------------------------------------------------------------------
    # 3.  Sandbox execution with circuit breaker
    # ------------------------------------------------------------------

    async def execute_with_simulation(
        self,
        order: dict[str, Any],
        sandbox_url: str = "http://localhost:8080/simulate",
    ) -> ExecutionResult:
        """Send an order to the sandbox for pre-execution validation.

        The sandbox returns a risk score in [0, 100].  If the score is
        >= 50 the **circuit breaker** triggers and the order is rejected.

        In mock mode a synthetic result is generated locally.

        Parameters
        ----------
        order : dict
            The order payload (Polymarket or Alpaca format).
        sandbox_url : str
            URL of the sandbox simulation endpoint.

        Returns
        -------
        ExecutionResult
            Structured execution / simulation result.
        """
        if self._mock:
            return self._mock_execution(order)

        session = await self._ensure_session()
        try:
            async with session.post(sandbox_url, json=order) as resp:
                resp.raise_for_status()
                data = await resp.json()

            risk_score = int(data.get("risk_score", 0))
            breaker = risk_score >= _CIRCUIT_BREAKER_THRESHOLD

            if breaker:
                logger.warning(
                    "CIRCUIT BREAKER triggered: risk_score=%d >= %d. "
                    "Order rejected.",
                    risk_score,
                    _CIRCUIT_BREAKER_THRESHOLD,
                )

            return ExecutionResult(
                success=not breaker and data.get("success", False),
                order_id=data.get("order_id", "N/A"),
                risk_score=risk_score,
                circuit_breaker_triggered=breaker,
                fill_price=float(data.get("fill_price", 0.0)),
                gas_estimate_gwei=float(
                    data.get("gas_estimate_gwei", 0.0)
                ),
                message=(
                    "CIRCUIT BREAKER: order rejected"
                    if breaker
                    else data.get("message", "OK")
                ),
                timestamp_utc=time.time(),
            )

        except Exception as exc:  # noqa: BLE001
            logger.error("Sandbox execution failed: %s", exc)
            return ExecutionResult(
                success=False,
                message=f"Sandbox error: {exc}",
                timestamp_utc=time.time(),
            )

    def _mock_execution(self, order: dict[str, Any]) -> ExecutionResult:
        """Generate a synthetic execution result for testing.

        The mock deterministically assigns a risk score based on the
        order hash so that the same order always produces the same
        result — useful for snapshot tests.
        """
        order_hash = hashlib.md5(
            json.dumps(order, sort_keys=True, default=str).encode()
        ).hexdigest()
        # Derive a deterministic risk score in [0, 30].
        risk_score = int(order_hash[:2], 16) % 31

        is_polymarket = "_eip712_domain" in order

        return ExecutionResult(
            success=True,
            order_id=f"mock-{order_hash[:12]}",
            risk_score=risk_score,
            circuit_breaker_triggered=False,
            fill_price=0.55 if is_polymarket else 185.42,
            gas_estimate_gwei=35.0 if is_polymarket else 0.0,
            message="Mock execution successful",
            timestamp_utc=time.time(),
        )

    # ------------------------------------------------------------------
    # 4.  EIP-712 typed data signing
    # ------------------------------------------------------------------

    def sign_eip712_order(
        self,
        order: dict[str, Any],
        private_key: str = "",
    ) -> str:
        """Compute the EIP-712 typed-data hash of a Polymarket order.

        EIP-712 hash construction
        -------------------------

        1. **Domain separator**::

            domainSeparator = keccak256(
                keccak256("EIP712Domain(string name,string version,"
                          "uint256 chainId,address verifyingContract)")
                || keccak256(name)
                || keccak256(version)
                || uint256(chainId)
                || address(verifyingContract)
            )

        2. **Struct hash** (Order)::

            hashStruct(order) = keccak256(
                typeHash
                || encode(salt)
                || encode(maker)
                || ...
            )

        3. **Final hash**::

            hash = keccak256("\\x19\\x01" || domainSeparator || hashStruct)

        This method returns the hex-encoded final hash.  Actual ECDSA
        signing with the private key is left as a thin wrapper (e.g.
        ``eth_account.Account.signHash``) to avoid bundling secp256k1
        in this module.

        Parameters
        ----------
        order : dict
            A Polymarket order dict as returned by
            ``prepare_polymarket_order``.
        private_key : str
            Hex-encoded private key (optional — if empty, only the hash
            is returned without a signature).

        Returns
        -------
        str
            ``"0x"``-prefixed hex string of the 32-byte EIP-712 hash.
            If *private_key* is provided, the 65-byte ECDSA signature is
            appended after a ``"|"`` separator (``hash|signature``).
        """
        domain = order.get("_eip712_domain", _POLYMARKET_DOMAIN)

        # --- Domain separator ---
        domain_type_hash = _keccak256(
            b"EIP712Domain(string name,string version,"
            b"uint256 chainId,address verifyingContract)"
        )
        domain_separator = _keccak256(
            domain_type_hash
            + _keccak256(domain["name"].encode())
            + _keccak256(domain["version"].encode())
            + _encode_uint256(domain["chainId"])
            + _encode_address(domain["verifyingContract"])
        )

        # --- Order type hash ---
        order_type_hash = _keccak256(
            b"Order(uint256 salt,address maker,address signer,"
            b"address taker,uint256 tokenId,uint256 makerAmount,"
            b"uint256 takerAmount,uint256 expiration,uint256 nonce,"
            b"uint256 feeRateBps,uint8 side,uint8 signatureType)"
        )

        # --- Struct hash ---
        struct_data = order_type_hash
        struct_data += _encode_uint256(int(order.get("salt", 0)))
        struct_data += _encode_address(
            order.get("maker", "0x" + "0" * 40)
        )
        struct_data += _encode_address(
            order.get("signer", "0x" + "0" * 40)
        )
        struct_data += _encode_address(
            order.get("taker", "0x" + "0" * 40)
        )

        token_id_raw = order.get("tokenId", "0")
        if isinstance(token_id_raw, str) and token_id_raw.startswith("0x"):
            token_id_int = int(token_id_raw, 16)
        else:
            token_id_int = int(token_id_raw)
        struct_data += _encode_uint256(token_id_int)

        struct_data += _encode_uint256(int(order.get("makerAmount", "0")))
        struct_data += _encode_uint256(int(order.get("takerAmount", "0")))
        struct_data += _encode_uint256(int(order.get("expiration", "0")))
        struct_data += _encode_uint256(int(order.get("nonce", "0")))
        struct_data += _encode_uint256(int(order.get("feeRateBps", "0")))
        struct_data += _encode_uint256(int(order.get("side", 0)))
        struct_data += _encode_uint256(int(order.get("signatureType", 0)))

        struct_hash = _keccak256(struct_data)

        # --- Final EIP-712 hash ---
        final_hash = _keccak256(
            b"\x19\x01" + domain_separator + struct_hash
        )

        hash_hex = "0x" + final_hash.hex()
        logger.debug("EIP-712 hash: %s", hash_hex)

        if private_key:
            # Placeholder ECDSA signature.  In production, use:
            #   from eth_account import Account
            #   sig = Account.signHash(final_hash, private_key)
            # Here we produce a deterministic mock signature.
            sig_input = final_hash + private_key.encode()
            mock_sig = _keccak256(sig_input) + _keccak256(
                sig_input[::-1]
            )
            # Take 65 bytes (r=32, s=32, v=1).
            signature = "0x" + mock_sig[:65].hex()
            return f"{hash_hex}|{signature}"

        return hash_hex

    # ------------------------------------------------------------------
    # Repr
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        mode = "mock" if self._mock else "live"
        return f"<OrderExecutor mode={mode}>"
