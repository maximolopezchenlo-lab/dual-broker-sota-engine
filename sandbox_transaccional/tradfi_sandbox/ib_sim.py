"""
Interactive Brokers & Alpaca TradFi Sandbox Simulator.

This module simulates the lifecycle of traditional finance equity/options orders,
implements position ledger bookkeeping, tracks capital allocation/buying power,
and exposes interfaces for Saga compensating transactions (reversals).
"""

from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass, field
from typing import Dict, Any

logger = logging.getLogger("TradFiSandbox")


@dataclass
class OrderResult:
    client_order_id: str
    symbol: str
    side: str # "buy" or "sell"
    status: str # "FILLED", "PARTIAL_FILL", "REJECTED", "CANCELLED"
    filled_qty: float
    fill_price: float
    fees: float
    timestamp: float
    error_message: str | None = None


@dataclass
class CancelResult:
    client_order_id: str
    status: str # "CANCELLED" or "REJECTED"
    filled_qty_before_cancel: float
    timestamp: float


class TradFiSandboxSimulator:
    def __init__(self, initial_cash: float = 100000.0, slippage_bps: int = 10):
        self.cash = initial_cash
        self.buying_power = initial_cash * 4.0 # 4:1 intraday leverage
        self.slippage_bps = slippage_bps # 10 bps default (0.10%)
        
        # Bookkeeping systems
        self.ledger_positions: dict[str, float] = {} # symbol -> quantity
        self.active_orders: dict[str, dict[str, Any]] = {} # client_order_id -> order details
        self.completed_orders: dict[str, OrderResult] = {} # client_order_id -> OrderResult
        self.reserved_capital: float = 0.0

        logger.info(f"TradFi Sandbox Simulator loaded. Initial Cash: ${initial_cash:.2f}, Leverage: 4x, Slippage: {slippage_bps} bps")

    def submit_order(
        self, 
        symbol: str, 
        qty: float, 
        side: str, 
        order_type: str = "market", 
        price: float | None = None, 
        client_order_id: str | None = None
    ) -> OrderResult:
        cid = client_order_id or f"ord-tf-{int(time.time() * 1000)}"
        side = side.upper()
        
        if side not in ["BUY", "SELL"]:
            raise ValueError("Order side must be BUY or SELL")

        # Mock current base price if limit price not provided
        # Assume base price is around $150.0 for SPY
        base_price = price if price is not None else 150.0
        
        # Calculate expected slippage
        slippage_factor = 1.0 + ((self.slippage_bps / 10000.0) * (1.0 if side == "BUY" else -1.0))
        fill_price = base_price * slippage_factor
        
        total_cost = fill_price * qty
        fees = max(1.0, total_cost * 0.0005) # 5 bps fee, minimum $1.00

        # Check buying power for purchases
        if side == "BUY":
            required_bp = total_cost + fees
            if required_bp > self.buying_power:
                logger.error(f"Order {cid} REJECTED due to insufficient buying power. Required: ${required_bp:.2f}, Available: ${self.buying_power:.2f}")
                res = OrderResult(
                    client_order_id=cid,
                    symbol=symbol,
                    side=side,
                    status="REJECTED",
                    filled_qty=0.0,
                    fill_price=0.0,
                    fees=0.0,
                    timestamp=time.time(),
                    error_message="Insufficient buying power."
                )
                self.completed_orders[cid] = res
                return res

            # Deduct from buying power and cash
            self.buying_power -= required_bp
            self.cash -= (total_cost + fees)
            self.reserved_capital += total_cost
        
        # Process sell position checks
        if side == "SELL":
            current_pos = self.ledger_positions.get(symbol, 0.0)
            if current_pos < qty:
                # Simulating short sell (borrowing costs apply)
                logger.info(f"Establishing SHORT position in {symbol} for {qty - current_pos} shares.")
            
            # Add to buying power and cash (sell generates proceeds)
            self.buying_power += (total_cost - fees)
            self.cash += (total_cost - fees)

        # Simulate execution status (90% FILLED, 10% PARTIAL_FILL)
        execution_roll = random.random()
        if execution_roll > 0.10:
            # Full Fill
            status = "FILLED"
            filled_qty = qty
        else:
            # Partial Fill
            status = "PARTIAL_FILL"
            filled_qty = round(qty * random.uniform(0.4, 0.8), 2)
            logger.warn(f"Order {cid} resulted in partial fill of {filled_qty}/{qty} shares.")

        # Update ledger positions
        mult = 1.0 if side == "BUY" else -1.0
        self.ledger_positions[symbol] = self.ledger_positions.get(symbol, 0.0) + (filled_qty * mult)

        # Clean up reserves
        if side == "BUY":
            self.reserved_capital -= (total_cost * (filled_qty / qty))

        res = OrderResult(
            client_order_id=cid,
            symbol=symbol,
            side=side,
            status=status,
            filled_qty=filled_qty,
            fill_price=fill_price,
            fees=fees,
            timestamp=time.time()
        )
        
        self.completed_orders[cid] = res
        logger.info(f"TradFi Order {cid} execution completed. Status: {status}, Price: ${fill_price:.4f}, Qty: {filled_qty}")
        return res

    def cancel_order(self, client_order_id: str) -> CancelResult:
        # Since orders are filled instantly in this simple sync simulator,
        # canceling a completed order will return REJECTED.
        if client_order_id in self.completed_orders:
            completed = self.completed_orders[client_order_id]
            logger.warn(f"Cannot cancel order {client_order_id}. Already resolved with status {completed.status}")
            return CancelResult(
                client_order_id=client_order_id,
                status="REJECTED",
                filled_qty_before_cancel=completed.filled_qty,
                timestamp=time.time()
            )

        logger.info(f"Order {client_order_id} successfully cancelled.")
        return CancelResult(
            client_order_id=client_order_id,
            status="CANCELLED",
            filled_qty_before_cancel=0.0,
            timestamp=time.time()
        )

    def get_buying_power(self) -> float:
        return self.buying_power

    def get_position(self, symbol: str) -> float:
        return self.ledger_positions.get(symbol, 0.0)

    # ═══════════════════════════════════════════════════════════════════════════
    #  Saga Pattern Reversals (Compensations)
    # ═══════════════════════════════════════════════════════════════════════════

    def compensate_buy(self, client_order_id: str) -> OrderResult:
        logger.info(f"[SAGA COMPENSATION] Reversing Buy Order: {client_order_id}")
        orig_order = self.completed_orders.get(client_order_id)
        if not orig_order:
            raise KeyError(f"Original order {client_order_id} not found in historical logs.")
            
        if orig_order.status not in ["FILLED", "PARTIAL_FILL"]:
            logger.info(f"Original order status was {orig_order.status}. No compensation required.")
            return orig_order

        # Reversal: Sell the exact amount filled
        reversal_id = f"comp-sell-{client_order_id}"
        return self.submit_order(
            symbol=orig_order.symbol,
            qty=orig_order.filled_qty,
            side="SELL",
            client_order_id=reversal_id
        )

    def compensate_sell(self, client_order_id: str) -> OrderResult:
        logger.info(f"[SAGA COMPENSATION] Reversing Sell Order: {client_order_id}")
        orig_order = self.completed_orders.get(client_order_id)
        if not orig_order:
            raise KeyError(f"Original order {client_order_id} not found in historical logs.")

        if orig_order.status not in ["FILLED", "PARTIAL_FILL"]:
            logger.info(f"Original order status was {orig_order.status}. No compensation required.")
            return orig_order

        # Reversal: Buy back the exact amount sold
        reversal_id = f"comp-buy-{client_order_id}"
        return self.submit_order(
            symbol=orig_order.symbol,
            qty=orig_order.filled_qty,
            side="BUY",
            client_order_id=reversal_id
        )
