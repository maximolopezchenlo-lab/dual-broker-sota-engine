import asyncio
import json
import logging
from core_agents.src.main import SwarmOrchestrator

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

async def main():
    # Instantiate the orchestrator
    orchestrator = SwarmOrchestrator()
    
    # Configure the researcher to run in live mode (mock_mode=False)
    # This will trigger our newly implemented HTTP GET fallback to ingest the real BLS report!
    orchestrator.researcher._mock = False
    
    print("\n" + "=" * 65)
    print(" [INFO] Starting Live Production Simulation Run")
    print(" Target: Real live Yahoo Finance SPY Quote Page")
    print(" URL: https://finance.yahoo.com/quote/SPY/")
    print("=" * 65 + "\n")
    
    # Run the cycle on the real live Yahoo Finance SPY page
    report_url = "https://finance.yahoo.com/quote/SPY/"
    result = await orchestrator.run_analysis_cycle(
        market_id="fed-rate-cut-june",
        target_url=report_url
    )
    
    print("\n" + "=" * 65)
    print(" --- PRODUCTION RUN CYCLE SUMMARY ---")
    print("=" * 65)
    print(json.dumps(result, indent=2))
    
    # Close session
    await orchestrator.researcher.close()

if __name__ == "__main__":
    asyncio.run(main())
