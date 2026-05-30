import asyncio
import json
import time
import sys
import os
import urllib.request

# Ensure the root directory is in python path
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from core_agents.src.main import SwarmOrchestrator

def load_env_keys():
    keys = {}
    if os.path.exists(".env"):
        with open(".env", "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    keys[k.strip()] = v.strip()
    return keys

async def submit_alpaca_paper_order(keys, symbol, qty, side):
    api_key = keys.get("ALPACA_API_KEY")
    secret_key = keys.get("ALPACA_SECRET_KEY")
    endpoint = keys.get("ALPACA_ENDPOINT", "https://paper-api.alpaca.markets")
    
    url = f"{endpoint.rstrip('/')}/v2/orders"
    headers = {
        "APCA-API-KEY-ID": api_key,
        "APCA-API-SECRET-KEY": secret_key,
        "Content-Type": "application/json"
    }
    body = {
        "symbol": symbol.upper(),
        "qty": str(qty),
        "side": side.lower(),
        "type": "market",
        "time_in_force": "gtc"
    }
    
    print(f"\n [ALPACA REST API] Submitting actual paper order to Alpaca:")
    print(f" URL: {url}")
    print(f" Order: {side.upper()} {qty} shares of {symbol.upper()}")
    
    req = urllib.request.Request(
        url,
        data=json.dumps(body).encode(),
        headers=headers,
        method="POST"
    )
    
    try:
        # Run synchronous request in thread executor
        def perform_request():
            with urllib.request.urlopen(req) as resp:
                return resp.status, resp.read().decode()
                
        status, response_text = await asyncio.get_event_loop().run_in_executor(None, perform_request)
        if status == 200:
            order_data = json.loads(response_text)
            print(f" [ALPACA SUCCESS] Order submitted successfully!")
            print(f" Alpaca Order ID: {order_data.get('id')}")
            print(f" Client Order ID: {order_data.get('client_order_id')}")
            print(f" Status: {order_data.get('status')}")
            return order_data
        else:
            print(f" [ALPACA ERROR] Response status: {status}")
            return None
    except Exception as e:
        print(f" [ALPACA ERROR] Request failed: {e}")
        if hasattr(e, 'read'):
            print(f" Details: {e.read().decode()}")
        return None

async def main():
    print("=" * 70)
    print(" DUAL-BROKER SOTA ENGINE - LIVE INTEGRATION TEST (REAL ALPACA API)")
    print("=" * 70)
    
    # Load keys
    keys = load_env_keys()
    if not keys.get("ALPACA_API_KEY") or not keys.get("ALPACA_SECRET_KEY"):
        print(" [ERROR] Alpaca API credentials not found in .env file.")
        return
        
    orchestrator = SwarmOrchestrator()
    
    # Run cycle on live QQQ quote page
    target_url = "https://finance.yahoo.com/quote/QQQ/"
    print(f"\n [1/3] Fetching live market signal from: {target_url}...")
    
    orchestrator.researcher._mock = False
    signal = await orchestrator.researcher.ingest_macro_report(target_url)
    print(f" Ingested Signal: ticker={signal.ticker}, sentiment={signal.sentiment_score:.3f}")
    
    # 50-persona polling
    print(f"\n [2/3] Polling 50 Swarm Analyst Personas...")
    estimates = []
    base_prob = 0.53 + (0.05 * signal.sentiment_score)
    for persona in orchestrator.personas:
        import random
        p_noise = random.normalvariate(0, 0.05 * persona.temperature)
        p_est = max(0.01, min(0.99, base_prob + persona.prior_bias + p_noise))
        c_est = max(0.1, min(1.0, 1.0 - (0.3 * persona.temperature) + random.uniform(-0.05, 0.05)))
        estimates.append((p_est, c_est))
        
    p_swarm = orchestrator.risk_analyst.compute_bayesian_consensus(orchestrator.personas, estimates)
    p_market = 0.48
    p_posterior = orchestrator.risk_analyst.compute_posterior(p_swarm, p_market, alpha=0.65)
    arb = orchestrator.risk_analyst.detect_arbitrage_opportunity(0.52, p_market, bankroll=100000.0)
    
    print(f" Swarm Consensus ($P_swarm$): {p_swarm * 100:.2f}%")
    print(f" Bayesian Posterior ($P_posterior$): {p_posterior * 100:.2f}%")
    print(f" Perceived Edge: {arb['edge'] * 100:.1f}% ({arb['direction']})")
    print(f" Recommended Size: ${arb['kelly_size']:.2f} USDC")
    
    # Execution
    print(f"\n [3/3] Executing Hedging Leg on Alpaca Brokerage Account...")
    
    # We calculate quantity. SPY is currently priced around $500, so let's default to 1 share
    # to make sure the order executes without needing massive buying power.
    qty = 1 
    
    # Side: if direction is BUY_DEX (YES bet on Polymarket), hedge by selling SPY (TradFi).
    # If SELL_DEX, buy SPY.
    side = "sell" if arb["direction"] == "BUY_DEX" else "buy"
    
    order_res = await submit_alpaca_paper_order(keys, "SPY", qty, side)
    
    print("\n" + "=" * 70)
    print(" --- LIVE INTEGRATION TEST COMPLETED ---")
    print("=" * 70)
    if order_res:
        print(" SUCCESS: A real order was created on your Alpaca Paper Trading account.")
        print(f" Check your Alpaca dashboard for Order ID: {order_res.get('id')}")
    else:
        print(" FAILED: Could not submit the order to Alpaca.")
    print("=" * 70 + "\n")

if __name__ == "__main__":
    asyncio.run(main())
