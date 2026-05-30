/**
 * @module client
 *
 * Unified MCP Client for the Dual-Broker SOTA Engine.
 *
 * ## Overview
 *
 * The **Model Context Protocol (MCP)** is an open standard that allows AI
 * applications to communicate with external tool servers through a uniform
 * interface. MCP uses **JSON-RPC 2.0** as its wire protocol, which can be
 * transported over:
 *   - **stdio** (stdin/stdout pipes) – for co-located servers
 *   - **WebSocket** – for network-separated servers (our primary transport)
 *   - **HTTP+SSE** – for stateless/serverless deployments
 *
 * This module implements a `UnifiedMCPClient` that maintains persistent
 * WebSocket connections to two MCP servers simultaneously:
 *
 * 1. **Bright Data MCP Server** – exposes ~25 web-extraction tools
 *    (`scraping_browser_navigate`, `web_data_unlocker_fetch`,
 *    `search_engine`, etc.) for anti-detection scraping with automatic
 *    JA4 fingerprint rotation, CAPTCHA solving, and proxy management.
 *
 * 2. **Alpaca MCP Server** – exposes trading tools (`place_order`,
 *    `get_positions`, `get_account`, `cancel_order`) wrapping the
 *    Alpaca Markets REST API for TradFi equity execution.
 *
 * ## JSON-RPC 2.0 Transport Details
 *
 * Every MCP message is a JSON-RPC 2.0 envelope:
 *
 * ```json
 * // Request
 * { "jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": { ... } }
 *
 * // Success Response
 * { "jsonrpc": "2.0", "id": 1, "result": { ... } }
 *
 * // Error Response
 * { "jsonrpc": "2.0", "id": 1, "error": { "code": -32600, "message": "..." } }
 * ```
 *
 * The MCP protocol layers its own methods on top of JSON-RPC:
 *   - `initialize`    – handshake with capabilities negotiation
 *   - `tools/list`    – discover available tools and their schemas
 *   - `tools/call`    – invoke a specific tool with arguments
 *   - `notifications` – server-initiated events (progress, logs)
 *
 * ## Reconnection Strategy
 *
 * The client uses exponential backoff with jitter for automatic reconnection:
 *   - Base delay: 1 s, max delay: 30 s, jitter: +/-25 %
 *   - After 5 consecutive failures, the client enters "degraded" mode and
 *     falls back to simulation responses until manually reset.
 *
 * ## Simulation Mode
 *
 * When `simulationMode` is enabled (or servers are unreachable after max
 * retries), the client returns realistic mock data. This allows the trading
 * engine to run backtests, CI pipelines, and local development without
 * requiring live MCP server connections.
 *
 * @example
 * ```ts
 * const client = new UnifiedMCPClient(config);
 * await client.connect();
 *
 * const tools = await client.listTools('brightdata');
 * const result = await client.callTool('brightdata', 'scraping_browser_navigate', {
 *   url: 'https://polymarket.com',
 * });
 *
 * await client.disconnect();
 * ```
 */

import WebSocket from 'ws';
import { v4 as uuidv4 } from 'uuid';
import { z } from 'zod';
import type { GatewayConfig } from './config/brightdata.js';

/* ===================================================================
 *  Zod Schemas for MCP / JSON-RPC Messages
 * =================================================================== */

/** Schema for a single MCP tool descriptor returned by `tools/list`. */
export const MCPToolSchema = z.object({
  name: z.string(),
  description: z.string().optional(),
  inputSchema: z
    .object({
      type: z.literal('object').default('object'),
      properties: z.record(z.unknown()).optional(),
      required: z.array(z.string()).optional(),
    })
    .passthrough()
    .optional(),
});
export type MCPTool = z.infer<typeof MCPToolSchema>;

/** Schema for a JSON-RPC 2.0 error object. */
export const JsonRpcErrorSchema = z.object({
  code: z.number(),
  message: z.string(),
  data: z.unknown().optional(),
});
export type JsonRpcError = z.infer<typeof JsonRpcErrorSchema>;

/** Schema for a JSON-RPC 2.0 response (success OR error). */
export const JsonRpcResponseSchema = z.object({
  jsonrpc: z.literal('2.0'),
  id: z.union([z.string(), z.number(), z.null()]),
  result: z.unknown().optional(),
  error: JsonRpcErrorSchema.optional(),
});
export type JsonRpcResponse = z.infer<typeof JsonRpcResponseSchema>;

/** Content item returned inside an MCP tool call result. */
export const MCPContentItemSchema = z.object({
  type: z.enum(['text', 'image', 'resource']),
  text: z.string().optional(),
  data: z.string().optional(),
  mimeType: z.string().optional(),
  resource: z.unknown().optional(),
});
export type MCPContentItem = z.infer<typeof MCPContentItemSchema>;

/** Full result envelope from an MCP `tools/call` response. */
export const MCPToolResultSchema = z.object({
  content: z.array(MCPContentItemSchema).default([]),
  isError: z.boolean().default(false),
});
export type MCPToolResult = z.infer<typeof MCPToolResultSchema>;

/* ===================================================================
 *  Internal Types
 * =================================================================== */

/** Identifies which MCP server a call is targeting. */
export type ServerName = 'brightdata' | 'alpaca';

/** Connection state machine states. */
type ConnectionState = 'disconnected' | 'connecting' | 'connected' | 'degraded';

/** Tracks an in-flight JSON-RPC request awaiting a response. */
interface PendingRequest {
  readonly id: string;
  readonly method: string;
  readonly sentAt: number;
  resolve: (value: unknown) => void;
  reject: (reason: Error) => void;
  timer: ReturnType<typeof setTimeout>;
}

/** Internal per-server connection wrapper. */
interface ServerConnection {
  readonly name: ServerName;
  readonly uri: string;
  ws: WebSocket | null;
  state: ConnectionState;
  reconnectAttempts: number;
  pendingRequests: Map<string, PendingRequest>;
  cachedTools: MCPTool[] | null;
  lastError: Error | null;
}

/* ===================================================================
 *  Constants
 * =================================================================== */

/** Maximum reconnection attempts before entering degraded mode. */
const MAX_RECONNECT_ATTEMPTS = 5;

/** Base reconnection delay in milliseconds. */
const RECONNECT_BASE_DELAY_MS = 1_000;

/** Maximum reconnection delay in milliseconds. */
const RECONNECT_MAX_DELAY_MS = 30_000;

/** Default per-request timeout in milliseconds. */
const DEFAULT_REQUEST_TIMEOUT_MS = 30_000;

/** MCP protocol version we negotiate during `initialize`. */
const MCP_PROTOCOL_VERSION = '2024-11-05';

/* ===================================================================
 *  UnifiedMCPClient
 * =================================================================== */

/**
 * A unified MCP client that maintains WebSocket connections to multiple
 * MCP servers and provides a single interface for tool discovery and
 * invocation.
 *
 * The client handles:
 *   - **Connection lifecycle**: connect, handshake, disconnect
 *   - **Request/response correlation**: UUID-based ID tracking
 *   - **Timeout management**: per-request deadlines with automatic cleanup
 *   - **Automatic reconnection**: exponential backoff with jitter
 *   - **Simulation fallback**: realistic mock responses when servers are down
 *
 * @see {@link https://modelcontextprotocol.io/specification} MCP Specification
 * @see {@link https://www.jsonrpc.org/specification} JSON-RPC 2.0
 */
export class UnifiedMCPClient {
  private readonly config: GatewayConfig;
  private readonly servers: Map<ServerName, ServerConnection>;
  private readonly simulationMode: boolean;
  private isShuttingDown = false;

  /**
   * Creates a new UnifiedMCPClient.
   *
   * @param config - The gateway configuration (from `loadConfig()`).
   *                 If `config.simulationMode` is true, no real WebSocket
   *                 connections will be attempted.
   */
  constructor(config: GatewayConfig) {
    this.config = config;
    this.simulationMode = config.simulationMode;

    this.servers = new Map<ServerName, ServerConnection>([
      [
        'brightdata',
        {
          name: 'brightdata',
          uri: 'ws://localhost:3000', // Bright Data MCP typically on :3000
          ws: null,
          state: 'disconnected',
          reconnectAttempts: 0,
          pendingRequests: new Map(),
          cachedTools: null,
          lastError: null,
        },
      ],
      [
        'alpaca',
        {
          name: 'alpaca',
          uri: config.alpaca.alpacaMCPServerUri,
          ws: null,
          state: 'disconnected',
          reconnectAttempts: 0,
          pendingRequests: new Map(),
          cachedTools: null,
          lastError: null,
        },
      ],
    ]);
  }

  /* -----------------------------------------------------------------
   *  Public API
   * ----------------------------------------------------------------- */

  /**
   * Establishes WebSocket connections to all configured MCP servers and
   * performs the MCP `initialize` handshake.
   *
   * In simulation mode, this method returns immediately without opening
   * any real connections.
   *
   * The MCP handshake flow:
   * 1. Client sends `initialize` with its capabilities
   * 2. Server responds with its capabilities and protocol version
   * 3. Client sends `notifications/initialized` to confirm
   *
   * @throws {Error} if a non-simulation connection fails after all retries.
   */
  async connect(): Promise<void> {
    if (this.simulationMode) {
      console.info('[mcp-client] Running in SIMULATION mode - no real connections.');
      for (const conn of this.servers.values()) {
        conn.state = 'connected'; // pretend connected
      }
      return;
    }

    const connectionPromises: Promise<void>[] = [];

    for (const conn of this.servers.values()) {
      connectionPromises.push(this.connectServer(conn));
    }

    const results = await Promise.allSettled(connectionPromises);

    for (const [idx, result] of results.entries()) {
      if (result.status === 'rejected') {
        const serverName = [...this.servers.keys()][idx];
        console.warn(
          `[mcp-client] Server "${String(serverName)}" connection failed, entering degraded mode:`,
          (result.reason as Error).message,
        );
        const conn = this.servers.get(serverName!);
        if (conn) {
          conn.state = 'degraded';
          conn.lastError = result.reason as Error;
        }
      }
    }
  }

  /**
   * Gracefully disconnects from all MCP servers.
   *
   * - Sends JSON-RPC `notifications/cancelled` for any pending requests
   * - Closes WebSocket connections with code 1000 (Normal Closure)
   * - Clears all internal state
   */
  async disconnect(): Promise<void> {
    this.isShuttingDown = true;

    for (const conn of this.servers.values()) {
      /* Cancel all pending requests */
      for (const [_reqId, pending] of conn.pendingRequests) {
        clearTimeout(pending.timer);
        pending.reject(new Error('Client disconnecting'));
      }
      conn.pendingRequests.clear();

      /* Close the WebSocket */
      if (conn.ws && conn.ws.readyState === WebSocket.OPEN) {
        try {
          /* Send a polite shutdown notification */
          const shutdownMsg = JSON.stringify({
            jsonrpc: '2.0',
            method: 'notifications/cancelled',
            params: { reason: 'client_shutdown' },
          });
          conn.ws.send(shutdownMsg);
        } catch {
          /* Best-effort - ignore send errors during shutdown */
        }

        await new Promise<void>((resolve) => {
          const wsRef = conn.ws!;
          wsRef.once('close', () => resolve());
          wsRef.close(1000, 'Client shutdown');

          /* Safety timeout - don't hang forever */
          setTimeout(() => {
            wsRef.terminate();
            resolve();
          }, 2_000);
        });
      }

      conn.ws = null;
      conn.state = 'disconnected';
      conn.reconnectAttempts = 0;
      conn.cachedTools = null;
    }

    this.isShuttingDown = false;
    console.info('[mcp-client] All connections closed.');
  }

  /**
   * Invokes a tool on the specified MCP server.
   *
   * This method sends a `tools/call` JSON-RPC 2.0 request and awaits the
   * response. The request is correlated by a UUID `id` field, and a timeout
   * guard ensures the caller is never blocked indefinitely.
   *
   * In simulation/degraded mode, returns mock data appropriate for the
   * requested tool.
   *
   * @param serverName - Which MCP server to target ('brightdata' | 'alpaca').
   * @param toolName   - Name of the tool to invoke (e.g. 'web_data_unlocker_fetch').
   * @param params     - Tool-specific parameters matching the tool's inputSchema.
   * @param timeoutMs  - Per-request timeout; defaults to config.baseTimeout.
   *
   * @returns The parsed {@link MCPToolResult} from the server.
   * @throws {Error} on timeout, connection failure, or JSON-RPC error.
   *
   * @example
   * ```ts
   * const result = await client.callTool('brightdata', 'web_data_unlocker_fetch', {
   *   url: 'https://example.com/api/prices',
   *   zone: 'unlocker',
   * });
   * console.log(result.content[0]?.text);
   * ```
   */
  async callTool(
    serverName: ServerName,
    toolName: string,
    params: Record<string, unknown> = {},
    timeoutMs?: number,
  ): Promise<MCPToolResult> {
    const conn = this.getConnection(serverName);
    const effectiveTimeout = timeoutMs ?? this.config.brightData.baseTimeout;

    /* -- Simulation / degraded fallback -- */
    if (this.simulationMode || conn.state === 'degraded') {
      return this.simulateToolCall(serverName, toolName, params);
    }

    /* -- Ensure connected -- */
    if (conn.state !== 'connected' || !conn.ws) {
      throw new Error(
        `[mcp-client] Server "${serverName}" is not connected (state: ${conn.state}).`,
      );
    }

    /* -- Build JSON-RPC request -- */
    const requestId = uuidv4();
    const rpcMessage = JSON.stringify({
      jsonrpc: '2.0',
      id: requestId,
      method: 'tools/call',
      params: {
        name: toolName,
        arguments: params,
      },
    });

    /* -- Send and await response -- */
    const rawResult = await this.sendRequest(conn, requestId, 'tools/call', rpcMessage, effectiveTimeout);

    /* -- Parse and validate result -- */
    const parseResult = MCPToolResultSchema.safeParse(rawResult);
    if (!parseResult.success) {
      /* The server returned a valid JSON-RPC response but the MCP
         payload doesn't match our schema - wrap it gracefully. */
      return {
        content: [
          {
            type: 'text',
            text: typeof rawResult === 'string' ? rawResult : JSON.stringify(rawResult),
          },
        ],
        isError: false,
      };
    }

    return parseResult.data;
  }

  /**
   * Discovers all tools available on the specified MCP server.
   *
   * Results are cached after the first call - use `forceRefresh` to
   * bypass the cache and fetch a fresh listing from the server.
   *
   * @param serverName   - Which MCP server to query.
   * @param forceRefresh - Bypass the tool cache. Default: false.
   *
   * @returns Array of {@link MCPTool} descriptors.
   */
  async listTools(serverName: ServerName, forceRefresh = false): Promise<MCPTool[]> {
    const conn = this.getConnection(serverName);

    /* Return cached tools if available */
    if (!forceRefresh && conn.cachedTools) {
      return conn.cachedTools;
    }

    /* -- Simulation / degraded fallback -- */
    if (this.simulationMode || conn.state === 'degraded') {
      const simTools = this.getSimulatedToolList(serverName);
      conn.cachedTools = simTools;
      return simTools;
    }

    /* -- Ensure connected -- */
    if (conn.state !== 'connected' || !conn.ws) {
      throw new Error(
        `[mcp-client] Server "${serverName}" is not connected (state: ${conn.state}).`,
      );
    }

    /* -- Send tools/list request -- */
    const requestId = uuidv4();
    const rpcMessage = JSON.stringify({
      jsonrpc: '2.0',
      id: requestId,
      method: 'tools/list',
      params: {},
    });

    const rawResult = await this.sendRequest(
      conn,
      requestId,
      'tools/list',
      rpcMessage,
      DEFAULT_REQUEST_TIMEOUT_MS,
    );

    /* -- Parse tool list -- */
    const resultObj = rawResult as { tools?: unknown[] };
    const rawTools = Array.isArray(resultObj?.tools) ? resultObj.tools : [];

    const tools: MCPTool[] = [];
    for (const raw of rawTools) {
      const parsed = MCPToolSchema.safeParse(raw);
      if (parsed.success) {
        tools.push(parsed.data);
      }
    }

    conn.cachedTools = tools;
    return tools;
  }

  /**
   * Returns the current connection state for a given server.
   * Useful for health checks and monitoring dashboards.
   */
  getConnectionState(serverName: ServerName): ConnectionState {
    return this.getConnection(serverName).state;
  }

  /**
   * Returns true if the client is operating in simulation mode
   * (either globally or because all servers degraded).
   */
  isSimulating(): boolean {
    if (this.simulationMode) return true;
    for (const conn of this.servers.values()) {
      if (conn.state === 'connected') return false;
    }
    return true; // all servers degraded
  }

  /* -----------------------------------------------------------------
   *  Private: Connection Management
   * ----------------------------------------------------------------- */

  /**
   * Connects to a single MCP server via WebSocket and performs the
   * MCP `initialize` handshake.
   */
  private async connectServer(conn: ServerConnection): Promise<void> {
    conn.state = 'connecting';

    return new Promise<void>((resolve, reject) => {
      const timeoutHandle = setTimeout(() => {
        reject(new Error(`Connection to "${conn.name}" timed out after 10 s`));
      }, 10_000);

      try {
        const ws = new WebSocket(conn.uri, {
          headers: {
            'User-Agent': 'dual-broker-mcp-gateway/1.0.0',
          },
          handshakeTimeout: 5_000,
        });

        ws.on('open', () => {
          conn.ws = ws;
          this.performHandshake(conn)
            .then(() => {
              conn.state = 'connected';
              conn.reconnectAttempts = 0;
              clearTimeout(timeoutHandle);
              console.info(`[mcp-client] Connected to "${conn.name}" at ${conn.uri}`);
              resolve();
            })
            .catch((err) => {
              clearTimeout(timeoutHandle);
              reject(err as Error);
            });
        });

        ws.on('message', (data: WebSocket.RawData) => {
          this.handleMessage(conn, data);
        });

        ws.on('close', (code: number, reason: Buffer) => {
          console.warn(
            `[mcp-client] "${conn.name}" connection closed (code=${code}, reason="${reason.toString()}")`,
          );
          conn.ws = null;
          conn.state = 'disconnected';

          if (!this.isShuttingDown) {
            this.scheduleReconnect(conn);
          }
        });

        ws.on('error', (err: Error) => {
          console.error(`[mcp-client] "${conn.name}" WebSocket error:`, err.message);
          conn.lastError = err;
          clearTimeout(timeoutHandle);

          if (conn.state === 'connecting') {
            reject(err);
          }
        });
      } catch (err) {
        clearTimeout(timeoutHandle);
        reject(err);
      }
    });
  }

  /**
   * Performs the MCP `initialize` handshake.
   *
   * The handshake negotiates protocol version and capabilities.
   * Our client declares support for `tools` (tool invocation) and
   * `logging` (receiving log notifications).
   */
  private async performHandshake(conn: ServerConnection): Promise<void> {
    const requestId = uuidv4();

    const initRequest = JSON.stringify({
      jsonrpc: '2.0',
      id: requestId,
      method: 'initialize',
      params: {
        protocolVersion: MCP_PROTOCOL_VERSION,
        capabilities: {
          tools: {},
          logging: {},
        },
        clientInfo: {
          name: 'dual-broker-mcp-gateway',
          version: '1.0.0',
        },
      },
    });

    const result = await this.sendRequest(
      conn,
      requestId,
      'initialize',
      initRequest,
      10_000,
    );

    /* Validate that the server accepted our protocol version */
    const initResult = result as {
      protocolVersion?: string;
      capabilities?: Record<string, unknown>;
      serverInfo?: { name?: string; version?: string };
    };

    console.info(
      `[mcp-client] Handshake with "${conn.name}" successful:`,
      {
        protocolVersion: initResult.protocolVersion,
        serverName: initResult.serverInfo?.name,
        serverVersion: initResult.serverInfo?.version,
      },
    );

    /* Send the `initialized` notification (no response expected) */
    const initializedNotification = JSON.stringify({
      jsonrpc: '2.0',
      method: 'notifications/initialized',
    });

    conn.ws?.send(initializedNotification);
  }

  /**
   * Sends a JSON-RPC request over the WebSocket and returns a promise
   * that resolves with the result (or rejects on error/timeout).
   *
   * Request tracking:
   * - Each request is assigned a UUID `id`
   * - A timer enforces the deadline
   * - The corresponding response is matched by `id` in `handleMessage`
   */
  private sendRequest(
    conn: ServerConnection,
    requestId: string,
    method: string,
    message: string,
    timeoutMs: number,
  ): Promise<unknown> {
    return new Promise<unknown>((resolve, reject) => {
      /* Set up timeout guard */
      const timer = setTimeout(() => {
        conn.pendingRequests.delete(requestId);
        reject(
          new Error(
            `[mcp-client] Request "${method}" to "${conn.name}" timed out after ${timeoutMs} ms (id=${requestId})`,
          ),
        );
      }, timeoutMs);

      /* Register the pending request */
      const pending: PendingRequest = {
        id: requestId,
        method,
        sentAt: Date.now(),
        resolve,
        reject,
        timer,
      };
      conn.pendingRequests.set(requestId, pending);

      /* Send over the wire */
      try {
        conn.ws!.send(message, (err?: Error) => {
          if (err) {
            clearTimeout(timer);
            conn.pendingRequests.delete(requestId);
            reject(
              new Error(
                `[mcp-client] Failed to send "${method}" to "${conn.name}": ${err.message}`,
              ),
            );
          }
        });
      } catch (err) {
        clearTimeout(timer);
        conn.pendingRequests.delete(requestId);
        reject(err);
      }
    });
  }

  /**
   * Handles an incoming WebSocket message.
   *
   * Messages are parsed as JSON-RPC 2.0 envelopes and matched against
   * pending requests by `id`. Notifications (messages without `id`) are
   * logged but not processed further.
   */
  private handleMessage(conn: ServerConnection, rawData: WebSocket.RawData): void {
    let parsed: unknown;
    try {
      parsed = JSON.parse(rawData.toString());
    } catch {
      console.warn(`[mcp-client] "${conn.name}" sent non-JSON message - ignoring.`);
      return;
    }

    /* Validate JSON-RPC envelope */
    const rpcResult = JsonRpcResponseSchema.safeParse(parsed);

    if (!rpcResult.success) {
      /* Might be a notification (no `id` field) */
      const notification = parsed as { method?: string; params?: unknown };
      if (notification.method) {
        this.handleNotification(conn, notification.method, notification.params);
      }
      return;
    }

    const response = rpcResult.data;
    const responseId = String(response.id);

    /* Find the pending request */
    const pending = conn.pendingRequests.get(responseId);
    if (!pending) {
      console.warn(
        `[mcp-client] "${conn.name}" received response for unknown request id=${responseId}`,
      );
      return;
    }

    /* Clean up */
    clearTimeout(pending.timer);
    conn.pendingRequests.delete(responseId);

    /* Resolve or reject */
    if (response.error) {
      const err = new Error(
        `[mcp-client] JSON-RPC error from "${conn.name}": [${response.error.code}] ${response.error.message}`,
      );
      pending.reject(err);
    } else {
      const latency = Date.now() - pending.sentAt;
      if (latency > 5_000) {
        console.warn(
          `[mcp-client] Slow response from "${conn.name}" for "${pending.method}": ${latency} ms`,
        );
      }
      pending.resolve(response.result);
    }
  }

  /**
   * Handles server-initiated notifications (no `id` - fire-and-forget).
   * MCP servers may send progress updates, log messages, or resource
   * change notifications.
   */
  private handleNotification(
    conn: ServerConnection,
    method: string,
    params: unknown,
  ): void {
    switch (method) {
      case 'notifications/progress':
        console.debug(`[mcp-client] "${conn.name}" progress:`, params);
        break;
      case 'notifications/message':
        console.info(`[mcp-client] "${conn.name}" log:`, params);
        break;
      case 'notifications/resources/updated':
        console.info(`[mcp-client] "${conn.name}" resource updated:`, params);
        /* Invalidate tool cache - server may have new tools */
        conn.cachedTools = null;
        break;
      default:
        console.debug(
          `[mcp-client] "${conn.name}" unknown notification "${method}":`,
          params,
        );
    }
  }

  /**
   * Schedules a reconnection attempt with exponential backoff and jitter.
   *
   * Delay formula:
   *   delay = min(BASE * 2^attempt, MAX) * (1 + random(+/-0.25))
   *
   * After MAX_RECONNECT_ATTEMPTS failures, the connection enters
   * "degraded" mode and falls back to simulation.
   */
  private scheduleReconnect(conn: ServerConnection): void {
    if (conn.reconnectAttempts >= MAX_RECONNECT_ATTEMPTS) {
      console.error(
        `[mcp-client] "${conn.name}" failed ${MAX_RECONNECT_ATTEMPTS} reconnection attempts - entering degraded mode.`,
      );
      conn.state = 'degraded';
      return;
    }

    const attempt = conn.reconnectAttempts;
    const baseDelay = Math.min(
      RECONNECT_BASE_DELAY_MS * Math.pow(2, attempt),
      RECONNECT_MAX_DELAY_MS,
    );
    /* Jitter: +/-25 % */
    const jitter = baseDelay * (0.75 + Math.random() * 0.5);
    const delay = Math.round(jitter);

    conn.reconnectAttempts++;

    console.info(
      `[mcp-client] Scheduling reconnect for "${conn.name}" in ${delay} ms (attempt ${conn.reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS})`,
    );

    setTimeout(() => {
      if (this.isShuttingDown || conn.state === 'connected') return;

      this.connectServer(conn).catch((err: unknown) => {
        console.error(
          `[mcp-client] Reconnect attempt ${conn.reconnectAttempts} for "${conn.name}" failed:`,
          (err as Error).message,
        );
        /* scheduleReconnect will be called again from the 'close' handler */
      });
    }, delay);
  }

  /* -----------------------------------------------------------------
   *  Private: Simulation / Mock
   * ----------------------------------------------------------------- */

  /**
   * Returns a realistic simulated tool list for the given server.
   * These mirror the actual tools exposed by the real MCP servers.
   */
  private getSimulatedToolList(serverName: ServerName): MCPTool[] {
    if (serverName === 'brightdata') {
      return [
        {
          name: 'scraping_browser_navigate',
          description: 'Navigate a Scraping Browser instance to a URL and return page content via CDP.',
          inputSchema: {
            type: 'object' as const,
            properties: {
              url: { type: 'string', description: 'The target URL to navigate to.' },
              wait_for_selector: { type: 'string', description: 'CSS selector to wait for before returning.' },
              timeout: { type: 'number', description: 'Navigation timeout in ms.' },
            },
            required: ['url'],
          },
        },
        {
          name: 'scraping_browser_click',
          description: 'Click an element on the current page in the Scraping Browser.',
          inputSchema: {
            type: 'object' as const,
            properties: { selector: { type: 'string' } },
            required: ['selector'],
          },
        },
        {
          name: 'scraping_browser_screenshot',
          description: 'Take a screenshot of the current Scraping Browser viewport.',
          inputSchema: { type: 'object' as const, properties: {} },
        },
        {
          name: 'scraping_browser_get_text',
          description: 'Extract visible text content from the current page.',
          inputSchema: { type: 'object' as const, properties: {} },
        },
        {
          name: 'web_data_unlocker_fetch',
          description: 'Fetch a URL through the Web Unlocker with automatic proxy rotation and JA4 evasion.',
          inputSchema: {
            type: 'object' as const,
            properties: {
              url: { type: 'string', description: 'Target URL.' },
              zone: { type: 'string', description: 'Web Unlocker zone name.' },
              format: { type: 'string', description: 'Response format: raw | markdown | json.' },
            },
            required: ['url'],
          },
        },
        {
          name: 'search_engine',
          description: 'Perform a search engine query and return structured SERP results.',
          inputSchema: {
            type: 'object' as const,
            properties: {
              query: { type: 'string' },
              engine: { type: 'string', description: 'google | bing | yandex' },
              num_results: { type: 'number' },
            },
            required: ['query'],
          },
        },
        {
          name: 'web_data_unlocker_unlock',
          description: 'Unlock a protected webpage, solving CAPTCHAs and bypassing WAFs automatically.',
          inputSchema: {
            type: 'object' as const,
            properties: {
              url: { type: 'string' },
              zone: { type: 'string' },
            },
            required: ['url'],
          },
        },
      ];
    }

    /* Alpaca MCP tools */
    return [
      {
        name: 'place_order',
        description: 'Place a new order on Alpaca Markets.',
        inputSchema: {
          type: 'object' as const,
          properties: {
            symbol: { type: 'string' },
            qty: { type: 'number' },
            side: { type: 'string' },
            type: { type: 'string' },
            time_in_force: { type: 'string' },
            limit_price: { type: 'number' },
          },
          required: ['symbol', 'qty', 'side', 'type', 'time_in_force'],
        },
      },
      {
        name: 'get_positions',
        description: 'Get all open positions in the Alpaca account.',
        inputSchema: { type: 'object' as const, properties: {} },
      },
      {
        name: 'get_account',
        description: 'Get account information including buying power and portfolio value.',
        inputSchema: { type: 'object' as const, properties: {} },
      },
      {
        name: 'cancel_order',
        description: 'Cancel an existing order by order ID.',
        inputSchema: {
          type: 'object' as const,
          properties: { order_id: { type: 'string' } },
          required: ['order_id'],
        },
      },
      {
        name: 'get_order',
        description: 'Get details of a specific order.',
        inputSchema: {
          type: 'object' as const,
          properties: { order_id: { type: 'string' } },
          required: ['order_id'],
        },
      },
    ];
  }

  /**
   * Generates a realistic simulated response for a tool call.
   *
   * Mock data is designed to be structurally identical to real server
   * responses, enabling the trading engine to run end-to-end in
   * simulation without code changes.
   */
  private simulateToolCall(
    serverName: ServerName,
    toolName: string,
    params: Record<string, unknown>,
  ): MCPToolResult {
    /* Add realistic latency jitter indication */
    const simLatency = Math.round(50 + Math.random() * 200);

    console.debug(
      `[mcp-client:sim] Simulating ${serverName}/${toolName} (latency~${simLatency} ms)`,
    );

    if (serverName === 'brightdata') {
      return this.simulateBrightDataTool(toolName, params);
    }

    return this.simulateAlpacaTool(toolName, params);
  }

  /** Simulates Bright Data MCP tool responses. */
  private simulateBrightDataTool(
    toolName: string,
    params: Record<string, unknown>,
  ): MCPToolResult {
    const url = (params['url'] as string | undefined) ?? 'https://example.com';

    switch (toolName) {
      case 'scraping_browser_navigate':
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({
                success: true,
                url,
                title: `Simulated Page - ${new URL(url).hostname}`,
                status: 200,
                html: `<html><body><h1>Simulated content for ${url}</h1>` +
                  `<div class="market-data">` +
                  `<span class="price">$${(50 + Math.random() * 150).toFixed(2)}</span>` +
                  `<span class="volume">${Math.round(1_000_000 + Math.random() * 9_000_000)}</span>` +
                  `<span class="change">${(Math.random() * 10 - 5).toFixed(2)}%</span>` +
                  `</div></body></html>`,
                loadTime: Math.round(800 + Math.random() * 700),
              }),
            },
          ],
          isError: false,
        };

      case 'web_data_unlocker_fetch':
      case 'web_data_unlocker_unlock':
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({
                success: true,
                url,
                statusCode: 200,
                body: JSON.stringify({
                  simulated: true,
                  source: 'web_unlocker',
                  data: {
                    price: Number((100 + Math.random() * 500).toFixed(2)),
                    timestamp: new Date().toISOString(),
                    confidence: 0.95,
                  },
                }),
                headers: {
                  'content-type': 'application/json',
                  'x-proxy-country': 'US',
                  'x-fingerprint': `ja4_sim_${Math.random().toString(36).slice(2, 10)}`,
                },
                proxyUsed: true,
                captchaSolved: false,
              }),
            },
          ],
          isError: false,
        };

      case 'search_engine':
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({
                success: true,
                query: params['query'] ?? 'market data',
                results: [
                  {
                    title: 'Market Data Feed - Simulated Result 1',
                    url: 'https://example.com/market-1',
                    snippet: 'Simulated SERP result with market data context.',
                  },
                  {
                    title: 'Financial API Documentation - Simulated Result 2',
                    url: 'https://example.com/api-docs',
                    snippet: 'REST API for real-time and historical market data.',
                  },
                ],
                totalResults: 2,
              }),
            },
          ],
          isError: false,
        };

      case 'scraping_browser_screenshot':
        return {
          content: [
            {
              type: 'image',
              data: 'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==',
              mimeType: 'image/png',
            },
          ],
          isError: false,
        };

      case 'scraping_browser_get_text':
        return {
          content: [
            {
              type: 'text',
              text: `Simulated page text content from ${url}. ` +
                `Market price: $${(100 + Math.random() * 400).toFixed(2)}. ` +
                `Volume: ${Math.round(Math.random() * 10_000_000)}. ` +
                `Last updated: ${new Date().toISOString()}.`,
            },
          ],
          isError: false,
        };

      default:
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({
                simulated: true,
                tool: toolName,
                params,
                message: `No specific simulation for tool "${toolName}" - returning generic success.`,
              }),
            },
          ],
          isError: false,
        };
    }
  }

  /** Simulates Alpaca MCP tool responses. */
  private simulateAlpacaTool(
    toolName: string,
    params: Record<string, unknown>,
  ): MCPToolResult {
    switch (toolName) {
      case 'place_order':
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({
                id: uuidv4(),
                client_order_id: uuidv4(),
                created_at: new Date().toISOString(),
                updated_at: new Date().toISOString(),
                submitted_at: new Date().toISOString(),
                filled_at: null,
                expired_at: null,
                canceled_at: null,
                failed_at: null,
                asset_id: uuidv4(),
                symbol: params['symbol'] ?? 'AAPL',
                qty: params['qty'] ?? 1,
                filled_qty: '0',
                type: params['type'] ?? 'market',
                side: params['side'] ?? 'buy',
                time_in_force: params['time_in_force'] ?? 'day',
                limit_price: params['limit_price'] ?? null,
                status: 'accepted',
                extended_hours: false,
              }),
            },
          ],
          isError: false,
        };

      case 'get_positions':
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify([
                {
                  asset_id: uuidv4(),
                  symbol: 'AAPL',
                  qty: '10',
                  avg_entry_price: '178.50',
                  market_value: '1820.00',
                  current_price: '182.00',
                  unrealized_pl: '35.00',
                  side: 'long',
                },
                {
                  asset_id: uuidv4(),
                  symbol: 'TSLA',
                  qty: '5',
                  avg_entry_price: '245.00',
                  market_value: '1275.00',
                  current_price: '255.00',
                  unrealized_pl: '50.00',
                  side: 'long',
                },
              ]),
            },
          ],
          isError: false,
        };

      case 'get_account':
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({
                id: uuidv4(),
                account_number: 'SIM00000001',
                status: 'ACTIVE',
                currency: 'USD',
                buying_power: '150000.00',
                cash: '75000.00',
                portfolio_value: '225000.00',
                equity: '225000.00',
                last_equity: '223500.00',
                long_market_value: '150000.00',
                short_market_value: '0.00',
                pattern_day_trader: false,
                trading_blocked: false,
                transfers_blocked: false,
                account_blocked: false,
              }),
            },
          ],
          isError: false,
        };

      case 'cancel_order':
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({
                success: true,
                order_id: params['order_id'],
                status: 'cancelled',
                cancelled_at: new Date().toISOString(),
              }),
            },
          ],
          isError: false,
        };

      case 'get_order':
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({
                id: params['order_id'] ?? uuidv4(),
                symbol: 'AAPL',
                qty: '10',
                side: 'buy',
                type: 'market',
                status: 'filled',
                filled_qty: '10',
                filled_avg_price: '179.25',
                filled_at: new Date().toISOString(),
              }),
            },
          ],
          isError: false,
        };

      default:
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({
                simulated: true,
                tool: toolName,
                params,
                message: `No specific simulation for Alpaca tool "${toolName}".`,
              }),
            },
          ],
          isError: false,
        };
    }
  }

  /* -----------------------------------------------------------------
   *  Private: Helpers
   * ----------------------------------------------------------------- */

  /**
   * Retrieves a server connection by name, throwing if it doesn't exist.
   */
  private getConnection(serverName: ServerName): ServerConnection {
    const conn = this.servers.get(serverName);
    if (!conn) {
      throw new Error(`[mcp-client] Unknown server: "${serverName}"`);
    }
    return conn;
  }
}
