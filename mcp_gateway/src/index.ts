export { loadConfig } from './config/brightdata.js';
export type { BrightDataConfig, AlpacaMCPConfig } from './config/brightdata.js';
export { UnifiedMCPClient } from './client.js';
export type { MCPToolResult } from './client.js';
export { MCPDynamicRouter } from './router.js';
export type { ScrapeResult, TradeResult } from './router.js';

import { loadConfig } from './config/brightdata.js';
import { UnifiedMCPClient } from './client.js';
import { MCPDynamicRouter } from './router.js';

// Demo execution block if run directly
if (process.argv[1]?.endsWith('index.js') || process.argv[1]?.endsWith('index.ts')) {
  (async () => {
    console.log('--- MCP Gateway Demo initialization ---');
    const config = loadConfig();
    const client = new UnifiedMCPClient(config);
    const router = new MCPDynamicRouter(client);

    await client.connect();

    console.log('\n--- Demo Web Scraping ---');
    const scrape = await router.scrapeData('https://polymarket.com/market/fed-june-cut');
    console.log('Scrape result:', JSON.stringify(scrape, null, 2));

    console.log('\n--- Demo Trade Execution ---');
    const trade = await router.executeTrade('SPY', 10, 'buy');
    console.log('Trade result:', JSON.stringify(trade, null, 2));

    client.disconnect();
  })();
}
