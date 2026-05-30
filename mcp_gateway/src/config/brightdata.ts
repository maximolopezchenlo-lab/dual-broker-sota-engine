/**
 * @module config/brightdata
 *
 * Configuration module for the Dual-Broker SOTA Engine's MCP Gateway.
 *
 * This module defines and loads configuration for two critical MCP server
 * connections that power the engine:
 *
 * 1. **Bright Data MCP** – Provides anti-detection web extraction capabilities
 *    including Scraping Browser (with full CDP control), Web Unlocker (rotating
 *    residential proxies with automatic JA4/JA3 fingerprint randomisation and
 *    WAF bypass), and SERP API (structured search-engine result pages).
 *
 * 2. **Alpaca MCP** – Connects to the Alpaca Markets trading API via an MCP
 *    server wrapper, enabling the engine to place equity/options orders, query
 *    positions, and manage account state through the same JSON-RPC 2.0
 *    transport used for scraping.
 *
 * All secrets are read from environment variables at startup – never hardcoded.
 * Each field is documented with its role in the SOTA architecture.
 *
 * @example
 * ```ts
 * import { loadConfig } from './config/brightdata.js';
 * const cfg = loadConfig();
 * console.log(cfg.brightData.webUnlockerZone); // "unlocker"
 * ```
 */

import { z } from 'zod';

/* ═══════════════════════════════════════════════════════════════════════════
 *  Zod Schemas – used both for type inference AND runtime validation
 * ═══════════════════════════════════════════════════════════════════════════ */

/**
 * Schema for the Bright Data MCP server configuration.
 *
 * The Bright Data MCP server exposes ~25 tools grouped into three tiers:
 *   • **Scraping Browser** – Headless Chromium over CDP for JavaScript-heavy
 *     SPAs (Polymarket, DeFi dashboards). Supports full DOM interaction.
 *   • **Web Unlocker** – High-volume URL fetching with automatic proxy
 *     rotation, TLS fingerprint randomisation (JA4 evasion), and built-in
 *     CAPTCHA solving. Ideal for mid-latency bulk extraction.
 *   • **SERP API** – Structured Google/Bing search results. Used as a
 *     last-resort data source when direct site scraping fails.
 */
export const BrightDataConfigSchema = z.object({
  /**
   * Bearer token for authenticating with the Bright Data API / MCP server.
   * Obtained from the Bright Data dashboard → API Tokens.
   * This token authorises access to all three scraping tiers.
   */
  apiToken: z.string().min(1, 'BRIGHT_DATA_API_TOKEN is required'),

  /**
   * Zone name for the Web Unlocker product.
   * The zone defines the proxy pool, geo-targeting rules, and unblocking
   * configuration (auto-retry, CAPTCHA solving, header normalisation).
   * Default: "unlocker" (the standard zone created on new BD accounts).
   */
  webUnlockerZone: z.string().default('unlocker'),

  /**
   * Zone name for the Scraping Browser (headless CDP) product.
   * Each zone maps to a cluster of browser instances with pre-configured
   * anti-fingerprinting: randomised Canvas/WebGL hashes, AudioContext
   * noise, navigator property spoofing, and JA4 TLS fingerprint rotation.
   * Default: "scraping_browser" (standard zone).
   */
  browserZone: z.string().default('scraping_browser'),

  /**
   * Base URL for the SERP API endpoint.
   * This RESTful endpoint returns structured JSON for search-engine queries.
   * Used as **Level 3** fallback – lowest confidence but highest availability.
   */
  serpEndpoint: z
    .string()
    .url()
    .default('https://api.brightdata.com/serp'),

  /**
   * Base timeout in milliseconds for a single MCP tool invocation.
   * For HFT-grade latency, Scraping Browser calls use 1.5 s (hardcoded
   * in the router); this value applies to Web Unlocker and SERP calls.
   * Default: 15 000 ms (15 s).
   */
  baseTimeout: z.number().int().positive().default(15_000),

  /**
   * Maximum number of retries for a single extraction operation before
   * escalating to the next failover tier.
   * The router uses exponential backoff: delay = baseTimeout × 2^attempt.
   * Default: 3.
   */
  maxRetries: z.number().int().min(0).max(10).default(3),

  /**
   * Requests-per-second rate limit for outgoing MCP calls.
   * Prevents tripping Bright Data's own rate limiter (usually 50 RPS for
   * Web Unlocker, 10 concurrent sessions for Scraping Browser).
   * Default: 20 RPS – a safe middle ground.
   */
  rateLimit: z.number().positive().default(20),

  /**
   * Enable "Pro Mode" which activates advanced anti-detection features:
   *   • JA4 fingerprint cycling per request (vs per session)
   *   • Residential IP stickiness disabled (maximum IP diversity)
   *   • Extended CAPTCHA solving (hCaptcha, Turnstile, Arkose)
   * Default: true – always run in pro mode for production HFT.
   */
  proMode: z.boolean().default(true),

  /**
   * Number of consecutive failures allowed before the circuit breaker trips.
   * Default: 3.
   */
  circuitBreakerThreshold: z.number().int().positive().default(3),

  /**
   * Time in milliseconds to wait before attempting to reset the circuit breaker.
   * Default: 60000 ms (1 minute).
   */
  circuitBreakerResetMs: z.number().int().positive().default(60_000),
});

/**
 * Schema for the Alpaca Markets MCP server configuration.
 *
 * The Alpaca MCP server wraps the Alpaca Trading API v2 and exposes tools
 * such as `place_order`, `get_positions`, `get_account`, and `cancel_order`
 * through JSON-RPC 2.0 over WebSocket. This allows the engine to execute
 * TradFi trades through the same unified MCP transport as web scraping.
 */
export const AlpacaMCPConfigSchema = z.object({
  /**
   * Alpaca API key ID (APCA-API-KEY-ID header).
   * Paper trading keys start with "PK"; live keys start with "AK".
   */
  alpacaApiKey: z.string().min(1, 'ALPACA_API_KEY is required'),

  /**
   * Alpaca API secret key (APCA-API-SECRET-KEY header).
   * Never log this value.
   */
  alpacaSecretKey: z.string().min(1, 'ALPACA_SECRET_KEY is required'),

  /**
   * Base REST endpoint for the Alpaca API.
   * Paper: https://paper-api.alpaca.markets
   * Live:  https://api.alpaca.markets
   */
  alpacaEndpoint: z
    .string()
    .url()
    .default('https://paper-api.alpaca.markets'),

  /**
   * WebSocket URI of the Alpaca MCP server.
   * The MCP server is a thin JSON-RPC 2.0 wrapper around the Alpaca REST
   * API, typically running locally or on the same VPC for minimal latency.
   */
  alpacaMCPServerUri: z
    .string()
    .default('ws://localhost:3001'),
});

/* ═══════════════════════════════════════════════════════════════════════════
 *  Inferred TypeScript Interfaces
 * ═══════════════════════════════════════════════════════════════════════════ */

/** Configuration for Bright Data MCP connectivity. */
export type BrightDataConfig = z.infer<typeof BrightDataConfigSchema>;

/** Configuration for Alpaca MCP connectivity. */
export type AlpacaMCPConfig = z.infer<typeof AlpacaMCPConfigSchema>;

/**
 * Combined gateway configuration containing both MCP server configs
 * plus gateway-level operational settings.
 */
export interface GatewayConfig {
  /** Bright Data MCP server connection & behaviour configuration. */
  readonly brightData: BrightDataConfig;

  /** Alpaca Markets MCP server connection configuration. */
  readonly alpaca: AlpacaMCPConfig;

  /**
   * When true, the gateway operates in simulation mode:
   *   • MCP calls return realistic mock data instead of hitting real servers
   *   • Useful for backtesting, CI pipelines, and local development
   *   • No API tokens are required
   */
  readonly simulationMode: boolean;

  /**
   * ISO 8601 timestamp of when this configuration was loaded.
   * Used for cache-busting and audit trails.
   */
  readonly loadedAt: string;

  /**
   * Identifies the execution environment.
   * Affects logging verbosity and error-reporting behaviour.
   */
  readonly environment: 'development' | 'staging' | 'production';
}

/* ═══════════════════════════════════════════════════════════════════════════
 *  Configuration Loader
 * ═══════════════════════════════════════════════════════════════════════════ */

/**
 * Loads and validates the complete gateway configuration from environment
 * variables with sensible defaults for development.
 *
 * **Environment Variables (Bright Data)**:
 * | Variable                     | Required | Default                              |
 * |------------------------------|----------|--------------------------------------|
 * | `BRIGHT_DATA_API_TOKEN`      | Yes*     | –                                    |
 * | `BRIGHT_DATA_UNLOCKER_ZONE`  | No       | `"unlocker"`                         |
 * | `BRIGHT_DATA_BROWSER_ZONE`   | No       | `"scraping_browser"`                 |
 * | `BRIGHT_DATA_SERP_ENDPOINT`  | No       | `"https://api.brightdata.com/serp"`  |
 * | `BRIGHT_DATA_BASE_TIMEOUT`   | No       | `15000`                              |
 * | `BRIGHT_DATA_MAX_RETRIES`    | No       | `3`                                  |
 * | `BRIGHT_DATA_RATE_LIMIT`     | No       | `20`                                 |
 * | `BRIGHT_DATA_PRO_MODE`       | No       | `true`                               |
 *
 * **Environment Variables (Alpaca)**:
 * | Variable                     | Required | Default                              |
 * |------------------------------|----------|--------------------------------------|
 * | `ALPACA_API_KEY`             | Yes*     | –                                    |
 * | `ALPACA_SECRET_KEY`          | Yes*     | –                                    |
 * | `ALPACA_ENDPOINT`            | No       | `"https://paper-api.alpaca.markets"` |
 * | `ALPACA_MCP_SERVER_URI`      | No       | `"ws://localhost:3001"`              |
 *
 * **Gateway-level Variables**:
 * | Variable                     | Required | Default                              |
 * |------------------------------|----------|--------------------------------------|
 * | `MCP_SIMULATION_MODE`        | No       | `false` (true if no tokens present)  |
 * | `NODE_ENV`                   | No       | `"development"`                      |
 *
 * *Required fields are only enforced when `MCP_SIMULATION_MODE` is `false`.
 *
 * @returns Fully validated {@link GatewayConfig} object.
 * @throws {z.ZodError} if required environment variables are missing and
 *         simulation mode is disabled.
 *
 * @example
 * ```ts
 * // Production – all env vars set
 * const config = loadConfig();
 *
 * // Development – auto-enters simulation mode
 * process.env.MCP_SIMULATION_MODE = 'true';
 * const devConfig = loadConfig();
 * ```
 */
export function loadConfig(): GatewayConfig {
  const env = process.env;

  /* ── Determine execution environment ────────────────────────────── */
  const rawEnv = (env['NODE_ENV'] ?? 'development').toLowerCase();
  const environment: GatewayConfig['environment'] =
    rawEnv === 'production'
      ? 'production'
      : rawEnv === 'staging'
        ? 'staging'
        : 'development';

  /* ── Determine simulation mode ──────────────────────────────────── */
  const explicitSimulation = env['MCP_SIMULATION_MODE'];
  const hasTokens =
    Boolean(env['BRIGHT_DATA_API_TOKEN']) &&
    Boolean(env['ALPACA_API_KEY']) &&
    Boolean(env['ALPACA_SECRET_KEY']);

  const simulationMode =
    explicitSimulation !== undefined
      ? explicitSimulation === 'true' || explicitSimulation === '1'
      : !hasTokens; // auto-enable simulation when tokens are absent

  /* ── Parse numeric env vars safely ──────────────────────────────── */
  const parseIntSafe = (raw: string | undefined, fallback: number): number => {
    if (raw === undefined || raw === '') return fallback;
    const parsed = Number.parseInt(raw, 10);
    return Number.isNaN(parsed) ? fallback : parsed;
  };

  const parseFloatSafe = (raw: string | undefined, fallback: number): number => {
    if (raw === undefined || raw === '') return fallback;
    const parsed = Number.parseFloat(raw);
    return Number.isNaN(parsed) ? fallback : parsed;
  };

  const parseBoolSafe = (raw: string | undefined, fallback: boolean): boolean => {
    if (raw === undefined || raw === '') return fallback;
    return raw === 'true' || raw === '1';
  };

  /* ── Build raw config objects ───────────────────────────────────── */
  const rawBrightData = {
    apiToken: env['BRIGHT_DATA_API_TOKEN'] ?? (simulationMode ? 'sim_token_brightdata' : ''),
    webUnlockerZone: env['BRIGHT_DATA_UNLOCKER_ZONE'] ?? 'unlocker',
    browserZone: env['BRIGHT_DATA_BROWSER_ZONE'] ?? 'scraping_browser',
    serpEndpoint: env['BRIGHT_DATA_SERP_ENDPOINT'] ?? 'https://api.brightdata.com/serp',
    baseTimeout: parseIntSafe(env['BRIGHT_DATA_BASE_TIMEOUT'], 15_000),
    maxRetries: parseIntSafe(env['BRIGHT_DATA_MAX_RETRIES'], 3),
    rateLimit: parseFloatSafe(env['BRIGHT_DATA_RATE_LIMIT'], 20),
    proMode: parseBoolSafe(env['BRIGHT_DATA_PRO_MODE'], true),
    circuitBreakerThreshold: parseIntSafe(env['BRIGHT_DATA_CIRCUIT_BREAKER_THRESHOLD'], 3),
    circuitBreakerResetMs: parseIntSafe(env['BRIGHT_DATA_CIRCUIT_BREAKER_RESET_MS'], 60_000),
  };

  const rawAlpaca = {
    alpacaApiKey: env['ALPACA_API_KEY'] ?? (simulationMode ? 'sim_key_alpaca' : ''),
    alpacaSecretKey: env['ALPACA_SECRET_KEY'] ?? (simulationMode ? 'sim_secret_alpaca' : ''),
    alpacaEndpoint: env['ALPACA_ENDPOINT'] ?? 'https://paper-api.alpaca.markets',
    alpacaMCPServerUri: env['ALPACA_MCP_SERVER_URI'] ?? 'ws://localhost:3001',
  };

  /* ── Validate through Zod ───────────────────────────────────────── */
  const brightData = BrightDataConfigSchema.parse(rawBrightData);
  const alpaca = AlpacaMCPConfigSchema.parse(rawAlpaca);

  const config: GatewayConfig = {
    brightData,
    alpaca,
    simulationMode,
    loadedAt: new Date().toISOString(),
    environment,
  };

  /* ── Log configuration summary (redacting secrets) ──────────────── */
  const redact = (s: string): string =>
    s.length <= 8 ? '***' : `${s.slice(0, 4)}…${s.slice(-4)}`;

  console.info('[mcp-gateway:config] Configuration loaded:', {
    environment: config.environment,
    simulationMode: config.simulationMode,
    loadedAt: config.loadedAt,
    brightData: {
      apiToken: redact(config.brightData.apiToken),
      webUnlockerZone: config.brightData.webUnlockerZone,
      browserZone: config.brightData.browserZone,
      serpEndpoint: config.brightData.serpEndpoint,
      baseTimeout: `${config.brightData.baseTimeout}ms`,
      maxRetries: config.brightData.maxRetries,
      rateLimit: `${config.brightData.rateLimit} RPS`,
      proMode: config.brightData.proMode,
    },
    alpaca: {
      apiKey: redact(config.alpaca.alpacaApiKey),
      endpoint: config.alpaca.alpacaEndpoint,
      mcpServer: config.alpaca.alpacaMCPServerUri,
    },
  });

  return config;
}

/**
 * Re-exports a frozen default config for use in tests and quick prototyping.
 * Always operates in simulation mode.
 */
export const DEFAULT_SIMULATION_CONFIG: Readonly<GatewayConfig> = Object.freeze({
  brightData: Object.freeze({
    apiToken: 'sim_token_brightdata',
    webUnlockerZone: 'unlocker',
    browserZone: 'scraping_browser',
    serpEndpoint: 'https://api.brightdata.com/serp',
    baseTimeout: 15_000,
    maxRetries: 3,
    rateLimit: 20,
    proMode: true,
    circuitBreakerThreshold: 3,
    circuitBreakerResetMs: 60_000,
  }),
  alpaca: Object.freeze({
    alpacaApiKey: 'sim_key_alpaca',
    alpacaSecretKey: 'sim_secret_alpaca',
    alpacaEndpoint: 'https://paper-api.alpaca.markets',
    alpacaMCPServerUri: 'ws://localhost:3001',
  }),
  simulationMode: true,
  loadedAt: '1970-01-01T00:00:00.000Z',
  environment: 'development' as const,
});
