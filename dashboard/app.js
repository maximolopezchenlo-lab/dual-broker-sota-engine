/* =====================================================================
   DUAL-BROKER SOTA ENGINE - BUSINESS LOGIC (app.js)
   ===================================================================== */

class SotaDashboardOrchestrator {
  constructor() {
    this.personas = [];
    this.currentCycle = 0;
    this.isExecuting = false;
    this.totalTrades = 0;
    this.accumulatedPnl = 0.0;
    this.rollbackCount = 0;
    this.autoIntervalId = null;
    this.tooltipTimeout = null;
    this.isDemoModeActive = false;
    
    // Scenarios data matching core models
    this.scenarios = {
      cpi_release: {
        ticker: 'GLD',
        sentiment: -0.714,
        direction: 'BEARISH',
        confidence: 0.537,
        snippet: 'Core PCE came in at 2.6% YoY, slightly below consensus 2.7%. Fed Chair Powell signalled data-dependent cuts possible in Q3, but inflation remains persistent, capping gold momentum.',
        edge: 0.04,
        directionArb: 'BUY_DEX', // Buy YES on DEX, sell underlying
        kellySize: 1923.08,
        jsd: 0.0008,
        sagaSucceeds: true,
      },
      fomc_meeting: {
        ticker: 'SPY',
        sentiment: 0.850,
        direction: 'BULLISH',
        confidence: 0.890,
        snippet: 'Federal Open Market Committee votes unanimously to lower interest rates by 25 basis points. Equities indices react bullishly to expansionary monetary guidance.',
        edge: 0.12,
        directionArb: 'BUY_DEX',
        kellySize: 4500.00,
        jsd: 0.0025,
        sagaSucceeds: false, // Triggers Saga Rollback demo!
      },
      crypto_dump: {
        ticker: 'WETH',
        sentiment: -0.920,
        direction: 'BEARISH',
        confidence: 0.940,
        snippet: 'Smart contract exploit in major lending market triggers $200M liquidation waterfall. WETH spot prices cascade downward as volatility spikes across DeFi platforms.',
        edge: 0.18,
        directionArb: 'SELL_DEX', // Sell outcome, buy spot
        kellySize: 8200.00,
        jsd: 0.0048,
        sagaSucceeds: false, // Triggers Saga Rollback
      },
      earnings_beat: {
        ticker: 'QQQ',
        sentiment: 0.450,
        direction: 'BULLISH',
        confidence: 0.620,
        snippet: 'Semiconductor manufacturers beat Q1 consensus EPS guidelines by 14%. Cloud computing demand signals robust capital expenditures entering next quarter.',
        edge: 0.03,
        directionArb: 'BUY_DEX',
        kellySize: 1200.00,
        jsd: 0.0004,
        sagaSucceeds: true,
      }
    };

    this.init();
  }

  formatCurrency(val, includeSign = false, maxDecimals = 2) {
    if (val === undefined || val === null) return '-';
    const sign = val < 0 ? '-' : (includeSign && val > 0 ? '+' : '');
    const absVal = Math.abs(val);
    return `${sign}$${absVal.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: maxDecimals })}`;
  }

  init() {
    this.generatePersonas();
    this.renderPersonas();
    this.setupEventListeners();
    this.setupScrollFallback();
    this.setupLivePolling();
    
    this.addLog('System initialized in Demonstration Mode.', 'system');
    this.addLog('Click "Execute Swarm Cycle" to simulate macro-arbitrage loops.', 'system');
  }

  setupLivePolling() {
    setInterval(() => this.pollLiveHistory(), 3000);
    this.pollLiveHistory();
  }

  async pollLiveHistory() {
    if (this.isExecuting) return;
    if (this.isDemoModeActive) return;
    try {
      const resp = await fetch('live_trades_history.json');
      if (resp.ok) {
        const data = await resp.json();
        
        // Sync badge state to Live Monitor if successfully fetching
        const modeBadge = document.getElementById('mode-badge');
        if (modeBadge) {
          modeBadge.innerText = 'Live Monitor';
          modeBadge.style.background = 'rgba(0, 242, 254, 0.1)';
          modeBadge.style.border = '1px solid rgba(0, 242, 254, 0.3)';
          modeBadge.style.color = '#00f2fe';
          modeBadge.title = 'Click to force sync with live backend';
        }
        
        // Update metrics
        this.accumulatedPnl = data.accumulated_pnl;
        this.grossPnl = data.gross_pnl !== undefined ? data.gross_pnl : data.accumulated_pnl;
        this.accumulatedCosts = data.accumulated_costs !== undefined ? data.accumulated_costs : 0.0;
        this.totalTrades = data.total_trades;
        this.rollbackCount = data.rollback_count;
        
        const pnlVal = document.getElementById('val-pnl');
        if (pnlVal) {
          pnlVal.innerText = this.formatCurrency(this.accumulatedPnl, true);
          pnlVal.className = `value ${this.accumulatedPnl >= 0 ? 'success' : 'danger'}`;
        }
        
        const costsVal = document.getElementById('val-costs');
        if (costsVal) {
          costsVal.innerText = this.formatCurrency(this.accumulatedCosts);
        }
        
        const grossVal = document.getElementById('val-gross');
        if (grossVal) {
          grossVal.innerText = this.formatCurrency(this.grossPnl, true);
          grossVal.className = `value ${this.grossPnl >= 0 ? 'success' : 'danger'}`;
        }
        
        const tradesVal = document.getElementById('val-trades');
        if (tradesVal) tradesVal.innerText = this.totalTrades;
        
        const rollbackVal = document.getElementById('val-rollback-count');
        if (rollbackVal) rollbackVal.innerText = this.rollbackCount;
        
        // Update session countdown timer badge
        const sessionBadge = document.getElementById('session-time-badge');
        if (sessionBadge) {
          if (data.session_remaining !== undefined && data.session_remaining !== null && data.session_remaining > 0) {
            const secs = data.session_remaining;
            const hours = Math.floor(secs / 3600);
            const mins = Math.floor((secs % 3600) / 60);
            const remainingSecs = secs % 60;
            
            const formatTime = (val) => String(val).padStart(2, '0');
            sessionBadge.innerText = `Session: ${formatTime(hours)}:${formatTime(mins)}:${formatTime(remainingSecs)} remaining`;
            sessionBadge.style.display = 'inline-block';
          } else {
            sessionBadge.style.display = 'none';
          }
        }
        
        // Update sub-costs breakdown if present
        if (data.cost_breakdown) {
          const llmCostVal = document.getElementById('val-cost-llm');
          if (llmCostVal) {
            if (data.cost_breakdown.aiml_real_spent !== undefined && data.cost_breakdown.aiml_real_spent > 0) {
              llmCostVal.innerText = this.formatCurrency(data.cost_breakdown.aiml_real_spent, false, 4) + ' (Real)';
            } else if (data.cost_breakdown.llm !== undefined) {
              llmCostVal.innerText = this.formatCurrency(data.cost_breakdown.llm, false, 4);
            }
          }
          const bdCostVal = document.getElementById('val-cost-brightdata');
          if (bdCostVal && data.cost_breakdown.bright_data !== undefined) {
            bdCostVal.innerText = this.formatCurrency(data.cost_breakdown.bright_data, false, 4);
          }
          const brokerCostVal = document.getElementById('val-cost-broker');
          if (brokerCostVal && data.cost_breakdown.broker !== undefined) {
            brokerCostVal.innerText = this.formatCurrency(data.cost_breakdown.broker, false, 4);
          }
          
          // Show and update AI/ML API Key Balance if available
          const aimlBalanceRow = document.getElementById('aiml-balance-row');
          const aimlBalanceVal = document.getElementById('val-aiml-balance');
          if (aimlBalanceRow && aimlBalanceVal && data.cost_breakdown.aiml_balance !== undefined && data.cost_breakdown.aiml_balance !== null) {
            aimlBalanceVal.innerText = this.formatCurrency(data.cost_breakdown.aiml_balance, false, 4);
            aimlBalanceRow.style.display = 'flex';
          } else if (aimlBalanceRow) {
            aimlBalanceRow.style.display = 'none';
          }
        }
        
        // Update config inputs if present and not currently active
        if (data.config) {
          const c = data.config;
          const setVal = (id, val) => {
            const el = document.getElementById(id);
            if (el && document.activeElement !== el && val !== undefined && val !== null) {
              if (el.type === 'checkbox') {
                el.checked = val;
              } else {
                el.value = val;
              }
            }
          };
          setVal('cfg-bankroll', c.bankroll);
          setVal('cfg-session-duration', c.session_duration);
          setVal('cfg-tickers', c.tickers);
          setVal('cfg-alpaca-key', c.alpaca_key);
          setVal('cfg-alpaca-secret', c.alpaca_secret);
          setVal('cfg-aiml-key', c.aiml_key);
          setVal('cfg-web3-key', c.web3_key);
          setVal('cfg-simulation', c.simulation_mode);
        }
        
        // Update Saga Leg Visualizer dynamically in real-time
        if (data.saga_state && !this.isExecuting) {
          const s = data.saga_state;
          const legPoly = document.getElementById('leg-poly');
          const dotPoly = document.getElementById('dot-leg-poly');
          if (legPoly) legPoly.className = `saga-leg ${s.leg_poly_status}`;
          if (dotPoly) {
            dotPoly.className = `badge-dot ${s.leg_poly_status === 'active' ? 'executing' : s.leg_poly_status}`;
          }
          const lpAction = document.getElementById('leg-poly-action');
          if (lpAction) lpAction.innerText = s.leg_poly_action;
          const lpSize = document.getElementById('leg-poly-size');
          if (lpSize) lpSize.innerText = s.leg_poly_size;
          const lpFill = document.getElementById('leg-poly-fill');
          if (lpFill) lpFill.innerText = s.leg_poly_fill;
          const lpGas = document.getElementById('leg-poly-gas');
          if (lpGas) lpGas.innerText = s.leg_poly_gas;

          const connector = document.getElementById('saga-flow-line');
          if (connector) connector.className = `connector-line ${s.connector_status}`;

          const legTradfi = document.getElementById('leg-tradfi');
          const dotTradfi = document.getElementById('dot-leg-tradfi');
          if (legTradfi) legTradfi.className = `saga-leg ${s.leg_tradfi_status}`;
          if (dotTradfi) {
            dotTradfi.className = `badge-dot ${s.leg_tradfi_status === 'active' ? 'executing' : s.leg_tradfi_status}`;
          }
          const ltAction = document.getElementById('leg-tradfi-action');
          if (ltAction) ltAction.innerText = s.leg_tradfi_action;
          const ltSymbol = document.getElementById('leg-tradfi-symbol');
          if (ltSymbol) ltSymbol.innerText = s.leg_tradfi_symbol;
          const ltQty = document.getElementById('leg-tradfi-qty');
          if (ltQty) ltQty.innerText = s.leg_tradfi_qty;
          const ltStatus = document.getElementById('leg-tradfi-status');
          if (ltStatus) {
            ltStatus.innerText = s.leg_tradfi_status === 'success' ? 'FILLED' : 
                                 (s.leg_tradfi_status === 'failed' ? 'REJECTED' : 
                                 (s.leg_tradfi_status === 'active' ? 'PENDING' : '-'));
          }
        }
        
        // Update latest signal if present
        if (data.latest_signal) {
          const sig = data.latest_signal;
          const sigTicker = document.getElementById('sig-ticker');
          if (sigTicker) sigTicker.innerText = sig.ticker || '-';
          
          const sigSentiment = document.getElementById('sig-sentiment');
          if (sigSentiment) sigSentiment.innerText = sig.sentiment_score !== undefined ? sig.sentiment_score.toFixed(3) : '-';
          
          const sigDirection = document.getElementById('sig-direction');
          if (sigDirection) {
            sigDirection.innerText = sig.direction || '-';
            sigDirection.className = `value ${sig.direction === 'BULLISH' ? 'success' : 'danger'}`;
          }
          
          const sigConfidence = document.getElementById('sig-confidence');
          if (sigConfidence) sigConfidence.innerText = sig.confidence !== undefined ? `${(sig.confidence * 100).toFixed(1)}%` : '-';
          
          const sigHash = document.getElementById('sig-hash');
          if (sigHash) sigHash.innerText = sig.source_verification || '-';
          
          const sigSnippet = document.getElementById('sig-snippet');
          if (sigSnippet) sigSnippet.innerText = sig.raw_snippet || '-';
        }
        
        // Update latest consensus if present
        if (data.latest_consensus) {
          const con = data.latest_consensus;
          
          const swarmVal = document.getElementById('val-swarm');
          if (swarmVal) swarmVal.innerText = `${(con.p_swarm * 100).toFixed(2)}%`;
          
          const postVal = document.getElementById('val-posterior');
          if (postVal) postVal.innerText = `${(con.p_posterior * 100).toFixed(2)}%`;
          
          const edgeVal = document.getElementById('val-edge');
          if (edgeVal) edgeVal.innerText = `${(con.edge * 100).toFixed(2)}%`;
          
          const kellyVal = document.getElementById('val-kelly');
          if (kellyVal) kellyVal.innerText = this.formatCurrency(con.kelly_size);
          
          const jsdVal = document.getElementById('val-jsd');
          if (jsdVal) jsdVal.innerText = con.jsd !== undefined ? con.jsd.toFixed(4) : '0.0000';
        }
        
        // Update persona matrix nodes if persona_estimates is present
        if (data.persona_estimates && data.persona_estimates.length === 50) {
          data.persona_estimates.forEach((est, idx) => {
            const p_est = est[0];
            const c_est = est[1];
            const persona = this.personas[idx];
            if (persona) {
              persona.lastProb = p_est;
              persona.lastConf = c_est;
              
              const node = document.getElementById(`node-${persona.id}`);
              if (node) {
                this.updateNodeTooltip(node, persona);
                
                let colorClass = 'var(--color-muted)';
                if (p_est > 0.54) {
                  colorClass = `rgba(0, 230, 118, ${c_est.toFixed(2)})`;
                  node.style.boxShadow = `0 0 8px rgba(0, 230, 118, ${c_est / 2})`;
                } else if (p_est < 0.46) {
                  colorClass = `rgba(127, 0, 255, ${c_est.toFixed(2)})`;
                  node.style.boxShadow = `0 0 8px rgba(127, 0, 255, ${c_est / 2})`;
                } else {
                  colorClass = `rgba(94, 107, 124, ${c_est.toFixed(2)})`;
                  node.style.boxShadow = 'none';
                }
                node.style.backgroundColor = colorClass;
                node.style.opacity = '1.0';
              }
            }
          });
        }
        
        // Manage controls
        const runBtn = document.getElementById('btn-run-cycle');
        const intervalSelect = document.getElementById('interval-select');
        const scenarioSelect = document.getElementById('scenario-select');
        
        if (data.live_mode) {
          const isManual = (data.loop_interval === 'manual');
          if (runBtn) {
            if (data.cycle_running) {
              runBtn.disabled = true;
              runBtn.innerText = 'Live Cycle Executing...';
            } else if (isManual) {
              runBtn.disabled = false;
              runBtn.innerText = 'Execute Swarm Cycle';
            } else {
              runBtn.disabled = true;
              runBtn.innerText = 'Live Loop Running';
            }
          }
          if (intervalSelect) {
            intervalSelect.disabled = false; // Always keep auto-interval enabled in live mode so the user can start/stop the loop
            if (data.loop_interval !== undefined && document.activeElement !== intervalSelect) {
              intervalSelect.value = data.loop_interval;
            }
          }
          if (scenarioSelect) scenarioSelect.disabled = data.cycle_running;
          
          // Render logs
          if (data.latest_logs && data.latest_logs.length > 0) {
            const container = document.getElementById('saga-log-container');
            if (container) {
              container.innerHTML = '';
              data.latest_logs.forEach(log => {
                const entry = document.createElement('div');
                entry.className = `log-entry ${log.type}`;
                entry.innerText = `[${log.time}] ${log.message}`;
                container.appendChild(entry);
              });
              container.scrollTop = container.scrollHeight;
            }
          }
        } else {
          // Live mode ended - restore controls if they were disabled
          if (runBtn && (runBtn.innerText === 'Live Agent Mode Active' || runBtn.innerText === 'Live Loop Running' || runBtn.disabled)) {
            runBtn.disabled = false;
            runBtn.innerText = 'Execute Swarm Cycle';
            if (intervalSelect) intervalSelect.disabled = false;
            if (scenarioSelect) scenarioSelect.disabled = false;
          }
        }
      } else {
        const modeBadge = document.getElementById('mode-badge');
        if (modeBadge) {
          modeBadge.innerText = 'Standalone Sandbox';
          modeBadge.style.background = 'rgba(255, 171, 0, 0.1)';
          modeBadge.style.border = '1px solid rgba(255, 171, 0, 0.3)';
          modeBadge.style.color = '#ffab00';
          modeBadge.title = 'Backend offline or standalone static demo';
        }
      }
    } catch (e) {
      const modeBadge = document.getElementById('mode-badge');
      if (modeBadge) {
        modeBadge.innerText = 'Standalone Sandbox';
        modeBadge.style.background = 'rgba(255, 171, 0, 0.1)';
        modeBadge.style.border = '1px solid rgba(255, 171, 0, 0.3)';
        modeBadge.style.color = '#ffab00';
        modeBadge.title = 'Backend offline or standalone static demo';
      }
    }
  }

  // Generate 50 diversified personas (matches core_agents main.py)
  generatePersonas() {
    this.personas = [];
    for (let i = 0; i < 50; i++) {
      const temp = Number((0.1 + Math.random() * 0.8).toFixed(2));
      const bias = Number((-0.15 + Math.random() * 0.3).toFixed(3));
      const brier = Number((0.12 + Math.random() * 0.38).toFixed(3));
      const gamma = Number((1.0 + Math.random() * 2.0).toFixed(1));

      this.personas.push({
        id: `persona-${String(i).padStart(2, '0')}`,
        temperature: temp,
        priorBias: bias,
        brierScore: brier,
        gamma: gamma,
        lastProb: 0.5,
        lastConf: 0.5,
      });
    }
  }

  // Render the 50 persona matrix
  renderPersonas() {
    const container = document.getElementById('swarm-matrix-container');
    container.innerHTML = '';
    const tooltip = document.getElementById('shared-tooltip');
    const tooltipContent = tooltip ? tooltip.querySelector('.tooltip-content') : null;

    this.personas.forEach((p, idx) => {
      const node = document.createElement('div');
      node.className = 'persona-node';
      node.id = `node-${p.id}`;
      node.tabIndex = 0; // Make focusable for keyboard accessibility
      
      // Initial styling
      node.style.backgroundColor = 'var(--color-muted)';
      node.style.opacity = '0.5';
      
      // Stitch Tooltip data
      this.updateNodeTooltip(node, p);
      
      // Show/Hide Tooltip handlers using Popover API and Anchor Positioning
      const showTooltip = () => {
        if (!tooltip || !tooltipContent) return;

        if (this.tooltipTimeout) {
          clearTimeout(this.tooltipTimeout);
          this.tooltipTimeout = null;
        }

        this.activeTooltipNode = node;
        // Set dynamic anchor-name on the hovered node
        node.style.anchorName = '--active-anchor';
        
        // Update tooltip content
        const text = node.getAttribute('data-tooltip');
        tooltipContent.innerText = text;
        
        // Feature detect native Anchor Positioning. If missing, use JS coordinates fallback.
        const supportsAnchor = window.CSS && CSS.supports('anchor-name', '--test');
        if (!supportsAnchor) {
          const rect = node.getBoundingClientRect();
          tooltip.style.left = `${rect.left + rect.width / 2}px`;
          tooltip.style.top = `${rect.top}px`;
          tooltip.style.transform = 'translate(-50%, -100%) translateY(-8px)';
          tooltip.style.position = 'fixed';
        } else {
          // Clear any dynamic coordinates when native CSS anchor positioning is supported
          tooltip.style.left = '';
          tooltip.style.top = '';
          tooltip.style.transform = '';
          tooltip.style.position = '';
        }
        
        try {
          tooltip.showPopover();
        } catch (e) {
          tooltip.style.display = 'block';
        }
      };

      const hideTooltip = () => {
        if (!tooltip) return;
        if (this.activeTooltipNode === node) {
          this.tooltipTimeout = setTimeout(() => {
            this.activeTooltipNode = null;
            node.style.anchorName = '';
            try {
              tooltip.hidePopover();
            } catch (e) {
              tooltip.style.display = 'none';
            }
          }, 100);
        }
      };

      // Add event listeners for both mouse hover and keyboard focus
      node.addEventListener('mouseenter', showTooltip);
      node.addEventListener('focus', showTooltip);
      node.addEventListener('mouseleave', hideTooltip);
      node.addEventListener('blur', hideTooltip);
      
      container.appendChild(node);
    });
  }

  updateNodeTooltip(element, persona) {
    const tooltipText = `ID: ${persona.id}\n` +
      `Temp: ${persona.temperature}\n` +
      `Prior Bias: ${persona.priorBias > 0 ? '+' : ''}${persona.priorBias}\n` +
      `Brier Score: ${persona.brierScore}\n` +
      `Gamma: ${persona.gamma}\n` +
      `Last Prob: ${(persona.lastProb * 100).toFixed(1)}%\n` +
      `Last Conf: ${(persona.lastConf * 100).toFixed(0)}%`;
    element.setAttribute('data-tooltip', tooltipText);
  }

  setupEventListeners() {
    // Mode badge click event (allows toggling back to Live mode or refreshing)
    const modeBadge = document.getElementById('mode-badge');
    if (modeBadge) {
      modeBadge.addEventListener('click', () => {
        if (this.isDemoModeActive) {
          this.isDemoModeActive = false;
          this.addLog('Resuming Live Monitoring mode...', 'system');
          this.pollLiveHistory(); // Force poll immediately
        } else {
          this.addLog('Refreshing live data from backend...', 'system');
          this.pollLiveHistory();
        }
      });
    }

    document.getElementById('btn-run-cycle').addEventListener('click', async () => {
      this.isDemoModeActive = false; // Disable demo mode when running real cycles
      
      // Try to trigger live cycle on Python backend
      try {
        const scenarioVal = document.getElementById('scenario-select').value;
        const resp = await fetch('api/trigger', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ scenario: scenarioVal })
        });
        if (resp.ok) {
          this.addLog(`Live cycle triggered on backend with scenario '${scenarioVal}'. Monitoring execution...`, 'system');
          return;
        }
      } catch (e) {
        // Fallback to local simulation if backend API is offline
      }
      this.runAnalysisCycle();
    });

    const demoBtn = document.getElementById('btn-demo-sim');
    if (demoBtn) {
      demoBtn.addEventListener('click', () => {
        this.isDemoModeActive = true;
        
        // Update mode-badge immediately to show simulated demo mode
        const modeBadgeEl = document.getElementById('mode-badge');
        if (modeBadgeEl) {
          modeBadgeEl.innerText = 'Demo Sandbox (Click to return to Live)';
          modeBadgeEl.style.background = 'rgba(0, 230, 118, 0.1)';
          modeBadgeEl.style.border = '1px solid rgba(0, 230, 118, 0.3)';
          modeBadgeEl.style.color = '#00e676';
          modeBadgeEl.title = 'Click to switch back to Live Monitoring mode';
        }
        
        this.addLog('Demo Simulation triggered: running frontend-only sandboxed cycle...', 'system');
        this.runAnalysisCycle();
      });
    }

    document.getElementById('console-clear').addEventListener('click', async () => {
      const container = document.getElementById('saga-log-container');
      if (container) container.innerHTML = '';
      this.addLog('Log cleared.', 'system');
      try {
        await fetch('api/config', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ clear_logs: true })
        });
      } catch (e) {}
    });

    // Handle Auto-Execution Interval Selector
    const intervalSelect = document.getElementById('interval-select');
    if (intervalSelect) {
      intervalSelect.addEventListener('change', async (e) => {
        const val = e.target.value;
        
        // Try updating interval on the backend
        try {
          const resp = await fetch('api/config', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ interval: val })
          });
          if (resp.ok) {
            this.addLog(`Auto-execution interval updated on backend: ${val === 'manual' ? 'manual' : val / 1000 + 's'}.`, 'success');
            return;
          }
        } catch (err) {
          // Fallback if backend API is offline
        }
        
        if (this.autoIntervalId) {
          clearInterval(this.autoIntervalId);
          this.autoIntervalId = null;
          this.addLog('Auto-execution disabled.', 'system');
        }
        
        const runBtn = document.getElementById('btn-run-cycle');
        if (val !== 'manual') {
          const ms = Number(val);
          this.addLog(`Auto-execution enabled. Interval: ${ms / 1000}s.`, 'system');
          if (runBtn) {
            runBtn.disabled = true;
            runBtn.innerText = 'Auto-Cycling...';
          }
          this.runAnalysisCycle();
          this.autoIntervalId = setInterval(() => this.runAnalysisCycle(), ms);
        } else {
          if (runBtn) {
            runBtn.disabled = false;
            runBtn.innerText = 'Execute Swarm Cycle';
          }
        }
      });
    }

    // Handle settings form submission (popover overlay)
    const settingsForm = document.getElementById('settings-form');
    if (settingsForm) {
      settingsForm.addEventListener('submit', async (e) => {
        e.preventDefault();
        const bankroll = Number(document.getElementById('cfg-bankroll').value);
        const tickers = document.getElementById('cfg-tickers').value;
        const alpacaKey = document.getElementById('cfg-alpaca-key').value;
        const alpacaSecret = document.getElementById('cfg-alpaca-secret').value;
        const aimlKey = document.getElementById('cfg-aiml-key').value;
        const web3Key = document.getElementById('cfg-web3-key').value;
        const simulationMode = document.getElementById('cfg-simulation').checked;
        const sessionDuration = document.getElementById('cfg-session-duration').value;
        
        // Prepare request body, avoiding overwriting keys with placeholder masks
        const configData = {
          bankroll: bankroll,
          session_duration: sessionDuration,
          tickers: tickers,
          simulation_mode: simulationMode
        };
        if (alpacaKey !== '********') configData.alpaca_api_key = alpacaKey;
        if (alpacaSecret !== '********') configData.alpaca_secret_key = alpacaSecret;
        if (aimlKey !== '********') configData.aiml_api_key = aimlKey;
        if (web3Key !== '********') configData.polymarket_private_key = web3Key;
        
        // Try updating credentials on the backend
        try {
          const resp = await fetch('api/config', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(configData)
          });
          if (resp.ok) {
            this.addLog(`Configurations saved & synced to backend. Allocated Capital: $${bankroll.toLocaleString()} USDC, Session: ${sessionDuration === 'infinite' ? 'Infinite' : sessionDuration + ' minutes'}.`, 'success');
          }
        } catch (err) {
          this.addLog(`Configurations saved locally: bankroll=$${bankroll.toLocaleString()} tickers=[${tickers}]`, 'success');
        }
        
        // Dynamically recalculate mock trade sizing ratios based on new bankroll limit
        this.scenarios.cpi_release.kellySize = bankroll * 0.0192;
        this.scenarios.fomc_meeting.kellySize = bankroll * 0.045;
        this.scenarios.crypto_dump.kellySize = bankroll * 0.082;
        this.scenarios.earnings_beat.kellySize = bankroll * 0.012;
        
        // Close popover dialog
        const panel = document.getElementById('settings-panel');
        if (panel && typeof panel.hidePopover === 'function') {
          panel.hidePopover();
        }
      });
    }
  }

  addLog(message, type = '') {
    const container = document.getElementById('saga-log-container');
    const entry = document.createElement('div');
    entry.className = `log-entry ${type}`;
    
    const time = new Date().toLocaleTimeString();
    entry.innerText = `[${time}] ${message}`;
    
    container.appendChild(entry);
    container.scrollTop = container.scrollHeight;

    if (this.checkScrollState) {
      setTimeout(this.checkScrollState, 50);
    }
  }

  setupScrollFallback() {
    const consoleBody = document.getElementById('saga-log-container');
    if (consoleBody) {
      const checkScroll = () => {
        const canScrollUp = consoleBody.scrollTop > 0;
        const canScrollDown = consoleBody.scrollTop + consoleBody.clientHeight < consoleBody.scrollHeight - 2;
        consoleBody.classList.toggle('can-scroll-up', canScrollUp);
        consoleBody.classList.toggle('can-scroll-down', canScrollDown);
      };
      consoleBody.addEventListener('scroll', checkScroll);
      this.checkScrollState = checkScroll;
      setTimeout(checkScroll, 100);
    }
  }

  // Animated metric counter
  animateValue(id, start, end, duration, formatFn) {
    const obj = document.getElementById(id);
    const range = end - start;
    let current = start;
    const increment = range / (duration / 16);
    const timer = setInterval(() => {
      current += increment;
      if ((increment > 0 && current >= end) || (increment < 0 && current <= end)) {
        clearInterval(timer);
        current = end;
      }
      obj.innerText = formatFn(current);
    }, 16);
  }

  // Compute a mock sha256 hash for verification visualizer
  generateHash(text) {
    let hash = 0;
    for (let i = 0; i < text.length; i++) {
      const char = text.charCodeAt(i);
      hash = (hash << 5) - hash + char;
      hash |= 0;
    }
    return '0x' + Math.abs(hash).toString(16).repeat(4).slice(0, 40);
  }

  async runAnalysisCycle() {
    if (this.isExecuting) return;
    this.isExecuting = true;
    
    const scenarioKey = document.getElementById('scenario-select').value;
    const data = this.scenarios[scenarioKey];
    
    const runBtn = document.getElementById('btn-run-cycle');
    const demoBtn = document.getElementById('btn-demo-sim');
    if (runBtn) {
      runBtn.disabled = true;
      runBtn.innerText = 'Processing Swarm Consensus...';
    }
    if (demoBtn) {
      demoBtn.disabled = true;
      demoBtn.innerText = 'Processing Demo...';
    }

    this.addLog(`=== Starting Swarm Analysis Cycle (ID: cycle-${this.currentCycle}) ===`, 'system');

    // Reset visuals
    this.resetSagaVisuals();
    this.resetMetricCards();

    // --- STEP 1: Ingest Signal (Researcher) ---
    this.addLog('Step 1: Ingesting macroeconomic context via Bright Data MCP...', 'system');
    document.getElementById('node-brightdata').querySelector('.pulse-indicator').className = 'pulse-indicator warning';
    
    await this.sleep(1200); // Simulate ingestion delay
    
    const generatedHash = this.generateHash(data.snippet + this.currentCycle);
    document.getElementById('sig-ticker').innerText = data.ticker;
    document.getElementById('sig-sentiment').innerText = data.sentiment.toFixed(3);
    document.getElementById('sig-direction').innerText = data.direction;
    document.getElementById('sig-direction').className = `value ${data.direction === 'BULLISH' ? 'success' : 'danger'}`;
    document.getElementById('sig-confidence').innerText = `${(data.confidence * 100).toFixed(1)}%`;
    document.getElementById('sig-hash').innerText = generatedHash;
    document.getElementById('sig-snippet').innerText = data.snippet;
    
    document.getElementById('node-brightdata').querySelector('.pulse-indicator').className = 'pulse-indicator success';
    this.addLog(`Ingested signal: ticker='${data.ticker}' direction='${data.direction}' score=${data.sentiment}`, 'success');

    // --- STEP 2: Poll 50 Personas (Consensus calculation) ---
    this.addLog('Step 2: Polling 50 diversified personas for probability estimates...', 'system');
    
    // Base probability shift
    const baseProb = 0.52 + (0.05 * data.sentiment);
    let cumulativeProb = 0;
    let activeNodesCount = 0;

    // Visual polling sequence
    for (let i = 0; i < 50; i++) {
      const node = document.getElementById(`node-${this.personas[i].id}`);
      
      // Shift persona calculations
      const p_noise = (Math.random() - 0.5) * 0.1 * this.personas[i].temperature;
      let p_est = baseProb + this.personas[i].priorBias + p_noise;
      p_est = Math.max(0.01, Math.min(0.99, p_est));
      
      const c_est = Math.max(0.1, Math.min(1.0, 1.0 - (0.3 * this.personas[i].temperature) + (Math.random() - 0.5) * 0.1));
      
      this.personas[i].lastProb = p_est;
      this.personas[i].lastConf = c_est;
      
      cumulativeProb += (p_est * c_est);
      activeNodesCount += c_est;
      
      // Update Node tooltips
      this.updateNodeTooltip(node, this.personas[i]);

      // Apply Stitch Color guidelines
      // YES/Bullish bias: Cyan/Green, NO/Bearish bias: Violet/Red
      let colorClass = 'var(--color-muted)';
      if (p_est > 0.54) {
        colorClass = `rgba(0, 230, 118, ${c_est.toFixed(2)})`; // Green YES
        node.style.boxShadow = `0 0 8px rgba(0, 230, 118, ${c_est / 2})`;
      } else if (p_est < 0.46) {
        colorClass = `rgba(127, 0, 255, ${c_est.toFixed(2)})`; // Violet NO
        node.style.boxShadow = `0 0 8px rgba(127, 0, 255, ${c_est / 2})`;
      } else {
        colorClass = `rgba(94, 107, 124, ${c_est.toFixed(2)})`; // Neutral grey
        node.style.boxShadow = 'none';
      }
      
      node.style.backgroundColor = colorClass;
      node.style.opacity = '1.0';
      node.style.transform = 'scale(1.15)';
      
      // Small sequential animation for parallel polling feeling
      if (i % 5 === 0) await this.sleep(40);
    }

    // Reset scaling after polling finishes
    setTimeout(() => {
      for (let i = 0; i < 50; i++) {
        document.getElementById(`node-${this.personas[i].id}`).style.transform = 'none';
      }
    }, 400);

    // Compute Consensus Math
    const p_swarm = cumulativeProb / activeNodesCount;
    const p_market = 0.48; // Baseline market pricing
    
    // Log odds fusion: P_posterior = (p_swarm^alpha * p_market^(1-alpha)) / ...
    const alpha = 0.65;
    const swarmOdds = p_swarm / (1 - p_swarm);
    const marketOdds = p_market / (1 - p_market);
    const posteriorOdds = Math.pow(swarmOdds, alpha) * Math.pow(marketOdds, 1 - alpha);
    const p_posterior = posteriorOdds / (1 + posteriorOdds);

    this.addLog(`Swarm consensus computed: P_swarm = ${(p_swarm * 100).toFixed(2)}%`, 'success');
    this.addLog(`Bayesian posterior computed: P_posterior = ${(p_posterior * 100).toFixed(2)}%`, 'success');

    // Update main metrics
    this.animateValue('val-swarm', 0, p_swarm * 100, 800, (v) => `${v.toFixed(2)}%`);
    this.animateValue('val-posterior', 0, p_posterior * 100, 800, (v) => `${v.toFixed(2)}%`);
    this.animateValue('val-edge', 0, data.edge * 100, 800, (v) => `${v.toFixed(2)}%`);
    this.animateValue('val-kelly', 0, data.kellySize, 800, (v) => `$${v.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`);
    this.animateValue('val-jsd', 0, data.jsd, 800, (v) => v.toFixed(4));

    // --- STEP 3: Execute Transactions & Saga ---
    this.addLog(`Step 3: Edge detected (${(data.edge * 100).toFixed(1)}%). Launching Dual-Broker transaction...`, 'system');
    await this.sleep(800);

    const directionText = data.directionArb === 'BUY_DEX' ? 'BUY YES' : 'SELL YES';
    const hedgingText = data.directionArb === 'BUY_DEX' ? 'SHORT HEDGE' : 'LONG HEDGE';
    
    // -- LEG 1: Polymarket Trade --
    this.addLog('[SAGA] Executing Leg 1: Submit Polymarket Bet...', 'system');
    const legPoly = document.getElementById('leg-poly');
    const dotPoly = document.getElementById('dot-leg-poly');
    
    legPoly.className = 'saga-leg active';
    dotPoly.className = 'badge-dot executing';
    
    document.getElementById('leg-poly-action').innerText = directionText;
    document.getElementById('leg-poly-size').innerText = `$${data.kellySize.toFixed(2)} USDC`;
    document.getElementById('leg-poly-fill').innerText = '0.48';
    document.getElementById('leg-poly-gas').innerText = '35.2 Gwei';

    await this.sleep(1500); // Wait for block submission
    
    legPoly.className = 'saga-leg success';
    dotPoly.className = 'badge-dot success';
    this.addLog('[SAGA][LEG 1] Polymarket transaction filled. Gas used: 154,204 units.', 'success');

    // Activate Connector
    const connector = document.getElementById('saga-flow-line');
    connector.className = 'connector-line active';

    await this.sleep(600);

    // -- LEG 2: TradFi Hedge Trade --
    this.addLog('[SAGA] Executing Leg 2: Submit Alpaca Hedging Order...', 'system');
    const legTradfi = document.getElementById('leg-tradfi');
    const dotTradfi = document.getElementById('dot-leg-tradfi');
    
    legTradfi.className = 'saga-leg active';
    dotTradfi.className = 'badge-dot executing';
    
    document.getElementById('leg-tradfi-action').innerText = hedgingText;
    document.getElementById('leg-tradfi-symbol').innerText = data.ticker;
    document.getElementById('leg-tradfi-qty').innerText = String(Math.floor(data.kellySize / 100));
    document.getElementById('leg-tradfi-status').innerText = 'PENDING';

    await this.sleep(1800); // Simulate brokerage routing latency

    if (data.sagaSucceeds) {
      // -- CASE A: SAGA SUCCESS --
      legTradfi.className = 'saga-leg success';
      dotTradfi.className = 'badge-dot success';
      document.getElementById('leg-tradfi-status').innerText = 'FILLED';
      this.addLog('[SAGA][LEG 2] TradFi Hedge filled. Order matched at avg price.', 'success');
      this.addLog('[SAGA] Dual-Broker Arbitrage Cycle completed successfully.', 'success');
      
      const profit = Number((data.kellySize * data.edge).toFixed(2));
      this.addLog(`[SAGA SUCCESS][SIMULATED] Position closed. Simulated realized profit: +$${profit.toFixed(2)} USDC`, 'success');
      this.addLog(`[SIMULATION NOTE] Theoretical metrics are excluded from the main dashboard P&L per guidelines. Only real Alpaca trades are recorded.`, 'system');
    } else {
      // -- CASE B: SAGA FAILURE & ROLLBACK --
      legTradfi.className = 'saga-leg failed';
      dotTradfi.className = 'badge-dot failed';
      document.getElementById('leg-tradfi-status').innerText = 'REJECTED';
      
      this.addLog('[SAGA][LEG 2] TradFi Hedge REJECTED: Lack of asset liquidity.', 'failed');
      this.addLog('[SAGA] Leg 2 execution failed. Initiating compensating rollback path...', 'warning');
      
      connector.className = 'connector-line compensated';
      await this.sleep(1000);
      
      // Reverse Leg 1
      legPoly.className = 'saga-leg compensated';
      dotPoly.className = 'badge-dot compensated';
      this.addLog('[SAGA-FACTORY] Reversing Web3 Bet Leg: Submitting Polymarket opposite order.', 'warning');
      await this.sleep(1200);
      
      this.addLog('[SAGA] Leg 1 reversed. Billetera resguardada sin delta unhedged.', 'warning');
      this.addLog('[SAGA] Saga transaction rolled back. Status: COMPENSATED.', 'warning');
      
      this.addLog(`[SAGA COMPENSATED][SIMULATED] Capital protected. Simulated gas transaction fee: -$15.00 USDC`, 'warning');
      this.addLog(`[SIMULATION NOTE] Theoretical metrics are excluded from the main dashboard P&L per guidelines. Only real Alpaca trades are recorded.`, 'system');
    }

    this.currentCycle++;
    this.isExecuting = false;
    
    // Manage execute button state based on whether auto-cycling is active
    const isAuto = document.getElementById('interval-select').value !== 'manual';
    if (runBtn) {
      if (isAuto) {
        runBtn.innerText = 'Auto-Cycling...';
        runBtn.disabled = true;
      } else {
        runBtn.disabled = false;
        runBtn.innerText = 'Execute Swarm Cycle';
      }
    }
    if (demoBtn) {
      demoBtn.disabled = isAuto;
      demoBtn.innerText = 'Run Demo Simulation';
    }
  }

  resetSagaVisuals() {
    document.getElementById('leg-poly').className = 'saga-leg';
    document.getElementById('dot-leg-poly').className = 'badge-dot';
    document.getElementById('leg-poly-action').innerText = '-';
    document.getElementById('leg-poly-size').innerText = '-';
    document.getElementById('leg-poly-fill').innerText = '-';
    document.getElementById('leg-poly-gas').innerText = '-';

    document.getElementById('saga-flow-line').className = 'connector-line';

    document.getElementById('leg-tradfi').className = 'saga-leg';
    document.getElementById('dot-leg-tradfi').className = 'badge-dot';
    document.getElementById('leg-tradfi-action').innerText = '-';
    document.getElementById('leg-tradfi-symbol').innerText = '-';
    document.getElementById('leg-tradfi-qty').innerText = '-';
    document.getElementById('leg-tradfi-status').innerText = '-';
  }

  resetMetricCards() {
    const metrics = ['val-swarm', 'val-posterior', 'val-edge', 'val-kelly', 'val-jsd'];
    metrics.forEach(id => {
      document.getElementById(id).innerText = id === 'val-kelly' ? '$0.00' : (id === 'val-jsd' ? '0.0000' : '0.00%');
    });
  }

  sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

// Instantiate dashboard on load
window.addEventListener('DOMContentLoaded', () => {
  window.orchestrator = new SotaDashboardOrchestrator();
});
