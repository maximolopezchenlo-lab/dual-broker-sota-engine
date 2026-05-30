/**
 * @module router
 * 
 * Dynamic MCP Router for the Dual-Broker SOTA Engine.
 * Handles resilient routing for web scraping (Scraping Browser -> Web Unlocker -> SERP)
 * and trade execution via Alpaca MCP. Implements a circuit breaker to block operations
 * after 3 consecutive failures.
 * 
 * Evasion and WAF Bypass Architecture:
 * 1. Scraping Browser: Launches full headless Chromium via CDP (Chrome DevTools Protocol).
 *    Bypasses Cloudflare/Datadome by executing real JS, rendering DOM, and handling canvas/WebGL probes.
 * 2. Web Unlocker: Employs rotating residential proxies. Emulates TLS fingerprints (JA4/JA3)
 *    and HTTP/2 headers to match browser profiles.
 * 3. SERP API: Acts as a fallback for news/macro data by reading cached index results.
 */

import { UnifiedMCPClient } from './client.js';
import type { MCPToolResult } from './client.js';
import { loadConfig } from './config/brightdata.js';

const config = loadConfig();

export interface ScrapeResult {
  status: 'success' | 'fallback' | 'failed';
  source: 'scraping_browser' | 'web_unlocker' | 'serp_api' | 'none';
  data: any;
  confidence: number; // 1.0 (browser), 0.8 (unlocker), 0.5 (serp)
  errors?: string[];
}

export interface TradeResult {
  status: 'filled' | 'accepted' | 'rejected' | 'failed';
  orderId?: string;
  clientOrderId?: string;
  symbol: string;
  qty: number;
  side: 'buy' | 'sell';
  price?: number;
  error?: string;
}

export class MCPDynamicRouter {
  private client: UnifiedMCPClient;
  private consecutiveFailures = 0;
  private isCircuitBroken = false;
  private lastCircuitBreakTime = 0;

  constructor(client: UnifiedMCPClient) {
    this.client = client;
  }

  /**
   * Routes extraction requests through the 3-tier failover mechanism.
   * Leverages exponential backoff for Web Unlocker.
   */
  public async scrapeData(url: string): Promise<ScrapeResult> {
    this.checkCircuitBreaker();
    if (this.isCircuitBroken) {
      throw new Error('Circuit Breaker is ACTIVE. Scraping requests are blocked.');
    }

    const errors: string[] = [];

    // --- LEVEL 1: Scraping Browser via CDP (1.5s timeout for HFT latency) ---
    try {
      console.log(`[ROUTER] Attempting Level 1: Scraping Browser for ${url}...`);
      const response = await this.client.callTool('brightdata', 'scraping_browser', {
        url,
        timeout: 1500, // 1.5s limit
      });
      const parsed = this.parseJsonText(response);
      this.resetFailureCount();
      return {
        status: 'success',
        source: 'scraping_browser',
        data: parsed,
        confidence: 1.0,
      };
    } catch (e: any) {
      console.warn(`[ROUTER] Level 1 (Browser) failed: ${e.message}. Falling back to Level 2.`);
      errors.push(`Level 1 error: ${e.message}`);
    }

    // --- LEVEL 2: Web Unlocker with exponential backoff (3 retries, doubling timeout from 15s) ---
    let webUnlockerTimeout = 15000;
    for (let attempt = 1; attempt <= 3; attempt++) {
      try {
        console.log(`[ROUTER] Attempting Level 2: Web Unlocker for ${url} (Attempt ${attempt}/3, timeout: ${webUnlockerTimeout}ms)...`);
        const response = await this.client.callTool('brightdata', 'web_unlocker', {
          url,
          timeout: webUnlockerTimeout,
        });
        const parsed = this.parseJsonText(response);
        this.resetFailureCount();
        return {
          status: 'fallback',
          source: 'web_unlocker',
          data: parsed,
          confidence: 0.8,
          errors,
        };
      } catch (e: any) {
        console.warn(`[ROUTER] Level 2 attempt ${attempt} failed: ${e.message}`);
        errors.push(`Level 2 attempt ${attempt} error: ${e.message}`);
        webUnlockerTimeout *= 2; // Double timeout for exponential backoff
      }
    }

    // --- LEVEL 3: SERP API contingency (lower confidence, last resort) ---
    try {
      console.log(`[ROUTER] Attempting Level 3: SERP API fallback for ${url}...`);
      const response = await this.client.callTool('brightdata', 'serp', {
        query: `site:${new URL(url).hostname} OR news about ${url}`,
      });
      const parsed = this.parseJsonText(response);
      this.resetFailureCount();
      return {
        status: 'fallback',
        source: 'serp_api',
        data: parsed,
        confidence: 0.5,
        errors,
      };
    } catch (e: any) {
      console.error(`[ROUTER] Level 3 (SERP) failed: ${e.message}`);
      errors.push(`Level 3 error: ${e.message}`);
    }

    // All levels failed - trigger circuit breaker incremental counter
    this.recordFailure();
    return {
      status: 'failed',
      source: 'none',
      data: null,
      confidence: 0.0,
      errors,
    };
  }

  /**
   * Routes trade execution orders to Alpaca MCP.
   */
  public async executeTrade(
    symbol: string,
    qty: number,
    side: 'buy' | 'sell',
    type: 'market' | 'limit' = 'market',
    timeInForce: 'day' | 'gtc' = 'day'
  ): Promise<TradeResult> {
    this.checkCircuitBreaker();
    if (this.isCircuitBroken) {
      return {
        status: 'failed',
        symbol,
        qty,
        side,
        error: 'Circuit breaker is ACTIVE. Trading is blocked.',
      };
    }

    try {
      console.log(`[ROUTER] Sending order to Alpaca MCP: ${side} ${qty} ${symbol} (${type})...`);
      const response = await this.client.callTool('alpaca', 'place_order', {
        symbol,
        qty: qty.toString(),
        side,
        type,
        time_in_force: timeInForce,
      });

      const order = this.parseJsonText(response);
      this.resetFailureCount();
      return {
        status: order.status === 'accepted' || order.status === 'filled' ? 'accepted' : 'failed',
        orderId: order.id,
        clientOrderId: order.client_order_id,
        symbol,
        qty,
        side,
        price: order.filled_avg_price ? parseFloat(order.filled_avg_price) : undefined,
      };

    } catch (error: any) {
      console.error(`[ROUTER] Alpaca trade execution failed:`, error.message);
      this.recordFailure();
      return {
        status: 'failed',
        symbol,
        qty,
        side,
        error: error.message,
      };
    }
  }

  /**
   * Helper to parse JSON string inside CallToolResult text
   */
  private parseJsonText(result: MCPToolResult): any {
    const text = result.content[0]?.text;
    if (!text) {
      throw new Error('Empty response content received from tool.');
    }
    return JSON.parse(text);
  }

  private recordFailure(): void {
    this.consecutiveFailures++;
    console.warn(`[ROUTER] Failure recorded. Consecutive failures: ${this.consecutiveFailures}`);
    if (this.consecutiveFailures >= config.brightData.circuitBreakerThreshold) {
      this.isCircuitBroken = true;
      this.lastCircuitBreakTime = Date.now();
      console.error(`[EMERGENCY] Circuit breaker triggered! All routes halted for ${config.brightData.circuitBreakerResetMs}ms.`);
    }
  }

  private resetFailureCount(): void {
    this.consecutiveFailures = 0;
  }

  private checkCircuitBreaker(): void {
    if (this.isCircuitBroken) {
      const elapsed = Date.now() - this.lastCircuitBreakTime;
      if (elapsed > config.brightData.circuitBreakerResetMs) {
        this.isCircuitBroken = false;
        this.consecutiveFailures = 0;
        console.log('[ROUTER] Circuit breaker reset. Resuming operations.');
      }
    }
  }
}
