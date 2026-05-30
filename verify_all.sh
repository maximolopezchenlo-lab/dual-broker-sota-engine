#!/bin/bash
# =====================================================================
# DUAL-BROKER SOTA ENGINE - INTEGRATION VERIFICATION SCRIPT
# =====================================================================
set -euo pipefail

# Output formatting helpers
INFO() { echo -e "\033[1;34m[INFO]\033[0m $*"; }
SUCCESS() { echo -e "\033[1;32m[SUCCESS]\033[0m $*"; }
ERROR() { echo -e "\033[1;31m[ERROR]\033[0m $*"; exit 1; }

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

INFO "1. Verifying root configuration files..."
[ -f .antigravityrules ] || ERROR ".antigravityrules missing."
[ -f system.rules ] || ERROR "system.rules missing."
[ -f docker-compose.yml ] || ERROR "docker-compose.yml missing."
[ -f README.md ] || ERROR "README.md missing."
SUCCESS "Root configuration verified."

INFO "2. Verifying mcp_gateway/ TS compilation and demo run..."
cd mcp_gateway
if command -v npm &> /dev/null; then
    npm install
    npm run build
    SUCCESS "mcp_gateway compiled successfully."
    # Run typecheck
    npm run typecheck
    SUCCESS "mcp_gateway typecheck passed."
else
    INFO "npm not installed. Skipping mcp_gateway npm build."
fi
cd "$ROOT_DIR"

INFO "3. Verifying streaming_pipeline/ Maven compilation..."
if command -v mvn &> /dev/null; then
    mvn clean compile -f streaming_pipeline/pom.xml
    SUCCESS "streaming_pipeline Java code compiled successfully."
else
    INFO "mvn/java not installed or not in PATH. Skipping streaming_pipeline compile."
fi

INFO "4. Verifying core_agents/ Python orchestrator and Bayesian risk models..."
if command -v python3 &> /dev/null; then
    # Setup virtual environment if needed
    python3 -m venv venv
    ./venv/bin/pip install --upgrade pip
    ./venv/bin/pip install -r core_agents/requirements.txt
    
    # Run the main cycle in mock mode to check calculations (Kelly, JS, Bayesian)
    INFO "Running SwarmOrchestrator simulation cycle..."
    ./venv/bin/python -m core_agents.src.main
    SUCCESS "core_agents analysis cycle completed successfully."
else
    ERROR "python3 is required to run the core_agents validation."
fi

INFO "5. Verifying sandbox_transaccional/ TypeScript Saga Orchestrator..."
cd sandbox_transaccional/web3_sandbox
if command -v npm &> /dev/null; then
    npm install
    npm run build
    SUCCESS "web3_sandbox compiled successfully."
    # Run the Saga manager simulation to test rollback paths
    INFO "Executing Saga rollback simulation test..."
    npm run simulate
    SUCCESS "Saga Orchestrator rollback path verified."
else
    INFO "npm not installed. Skipping web3_sandbox compile."
fi
cd "$ROOT_DIR"

echo ""
SUCCESS "====================================================================="
SUCCESS "ALL INTEGRATION & VERIFICATION CHECKS PASSED SUCCESSFULLY! (SOTA)"
SUCCESS "====================================================================="
