"""
TradFi Researcher Agent — MCP-gateway-powered macro & social signal ingestion.

This module implements the **Researcher** node in the agent mesh.  It is the
first stage of the Researcher → Risk Analyst → Executor pipeline and is
responsible for:

1. Fetching unstructured web content via the MCP gateway's ``web_unlocker``
   endpoint (Bright Data integration).
2. Parsing raw HTML / JSON into a strict Pydantic ``MarketSignalSchema``,
   tolerating malformed LLM-generated JSON via ``json-repair``.
3. Running SERP-based social sentiment scans across configurable source
   lists (Twitter/X, Reddit, Telegram, etc.).
4. Providing a deterministic **mock mode** that yields synthetic
   Fed / CPI / unemployment data for local development and CI.

MCP Gateway protocol
====================

The researcher expects an HTTP POST endpoint at ``{mcp_endpoint}/web_unlocker``
that accepts:

.. code-block:: json

    {
        "url": "<target_url>",
        "render_js": true,
        "format": "raw"
    }

and returns the page content in ``response.body``.

Dependencies:  ``aiohttp>=3.9``, ``pydantic>=2.0``, ``json-repair>=0.25``
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import random
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import aiohttp
import json_repair
from pydantic import BaseModel, Field, field_validator

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pydantic schemas
# ---------------------------------------------------------------------------


class SentimentDirection(str, Enum):
    """Directional bias extracted from a macro report."""

    BULLISH = "bullish"
    BEARISH = "bearish"
    NEUTRAL = "neutral"


class MarketSignalSchema(BaseModel):
    """Strict schema for parsed market intelligence signals.

    Every field must be present and pass validation before the signal is
    forwarded to the Risk Analyst.  The ``json-repair`` pre-processing
    step ensures that malformed LLM outputs are coerced into valid JSON
    before Pydantic validation.

    Attributes
    ----------
    ticker : str
        The asset ticker symbol (e.g. ``"BTC-USD"``, ``"SPY"``).
    sentiment_score : float
        Normalised sentiment in ``[-1.0, 1.0]``.  Negative = bearish,
        positive = bullish.
    macro_context : str
        Free-text summary of the macro backdrop (Fed policy, CPI print,
        labour market, geopolitics).
    source_verification : str
        SHA-256 hash of the raw source payload for auditability.
    direction : SentimentDirection
        Inferred directional bias.
    timestamp_utc : float
        Unix epoch timestamp of when the signal was generated.
    confidence : float
        Researcher's self-assessed confidence in ``[0.0, 1.0]``.
    raw_snippet : str
        Up to 500-char excerpt from the source for human review.
    """

    ticker: str = Field(..., min_length=1, max_length=20)
    sentiment_score: float = Field(..., ge=-1.0, le=1.0)
    macro_context: str = Field(..., min_length=1)
    source_verification: str = Field(..., min_length=64, max_length=64)
    direction: SentimentDirection = SentimentDirection.NEUTRAL
    timestamp_utc: float = Field(default_factory=time.time)
    confidence: float = Field(default=0.5, ge=0.0, le=1.0)
    raw_snippet: str = Field(default="", max_length=500)
    price: float | None = Field(default=None)

    @field_validator("sentiment_score")
    @classmethod
    def _clamp_sentiment(cls, v: float) -> float:  # noqa: N805
        return max(-1.0, min(1.0, v))


class SocialSentimentHit(BaseModel):
    """A single social-media sentiment hit returned by the SERP scanner."""

    source: str
    title: str = ""
    snippet: str = ""
    sentiment_score: float = Field(default=0.0, ge=-1.0, le=1.0)
    url: str = ""
    timestamp_utc: float = Field(default_factory=time.time)


# ---------------------------------------------------------------------------
# Mock data generators
# ---------------------------------------------------------------------------

_MOCK_TICKERS: list[str] = ["SPY", "BTC-USD", "ETH-USD", "GLD", "TLT"]

_MOCK_MACRO_CONTEXTS: list[str] = [
    (
        "Fed held rates steady at 5.25-5.50%.  Chair Powell signalled "
        "data-dependent cuts possible in Q3.  Core PCE came in at 2.6% "
        "YoY, slightly below consensus 2.7%."
    ),
    (
        "CPI print surprised to the upside at 3.5% YoY (consensus 3.4%). "
        "Shelter inflation remains sticky.  10Y yield jumped 8 bps to 4.62%."
    ),
    (
        "Non-farm payrolls beat expectations: +303 k vs +200 k consensus. "
        "Unemployment rate fell to 3.8%.  Average hourly earnings +0.3% MoM."
    ),
    (
        "ISM Manufacturing PMI contracted for 17th consecutive month at "
        "47.8.  New orders sub-index fell to 46.1.  Supply-chain pressures "
        "easing but demand side remains weak."
    ),
    (
        "ECB cut rates by 25 bps to 3.75%.  Lagarde guided toward gradual "
        "normalisation.  EUR/USD fell 40 pips on the announcement."
    ),
]


def _generate_mock_signal(ticker: str | None = None) -> MarketSignalSchema:
    """Generate a single synthetic macro signal for testing."""
    chosen_ticker = ticker or random.choice(_MOCK_TICKERS)
    sentiment = round(random.uniform(-0.8, 0.8), 4)
    ctx = random.choice(_MOCK_MACRO_CONTEXTS)
    raw = f"MOCK|{chosen_ticker}|{sentiment}|{ctx[:120]}"
    verification = hashlib.sha256(raw.encode()).hexdigest()

    if sentiment > 0.15:
        direction = SentimentDirection.BULLISH
    elif sentiment < -0.15:
        direction = SentimentDirection.BEARISH
    else:
        direction = SentimentDirection.NEUTRAL

    return MarketSignalSchema(
        ticker=chosen_ticker,
        sentiment_score=sentiment,
        macro_context=ctx,
        source_verification=verification,
        direction=direction,
        timestamp_utc=time.time(),
        confidence=round(random.uniform(0.4, 0.95), 3),
        raw_snippet=ctx[:500],
    )


def _generate_mock_social_hits(
    query: str, n: int = 5
) -> list[SocialSentimentHit]:
    """Generate synthetic social-sentiment hits."""
    sources = ["twitter/x", "reddit", "telegram", "stocktwits", "discord"]
    hits: list[SocialSentimentHit] = []
    for i in range(n):
        hits.append(
            SocialSentimentHit(
                source=sources[i % len(sources)],
                title=f"Mock social hit #{i + 1} for '{query}'",
                snippet=(
                    f"Synthetic sentiment snippet about {query}.  "
                    f"Market participants are discussing volatility and "
                    f"upcoming macro catalysts."
                ),
                sentiment_score=round(random.uniform(-0.6, 0.6), 4),
                url=f"https://mock.example.com/{sources[i % len(sources)]}/{i}",
                timestamp_utc=time.time() - random.uniform(0, 3600 * 24),
            )
        )
    return hits


# ---------------------------------------------------------------------------
# Researcher agent
# ---------------------------------------------------------------------------


class TradFiResearcher:
    """MCP-gateway-powered macro research and social sentiment agent.

    The researcher is the first node in the agent mesh pipeline.  It
    fetches raw intelligence from the web (via Bright Data's Web Unlocker
    exposed through the MCP gateway), parses it into structured signals,
    and forwards those signals to the Risk Analyst for Bayesian consensus
    and position sizing.

    Parameters
    ----------
    mcp_endpoint : str
        Base URL of the MCP gateway (e.g. ``http://localhost:3000``).
    mock_mode : bool
        When *True*, all external calls are replaced by deterministic
        synthetic data generators.  Suitable for CI and local dev.
    request_timeout : float
        HTTP timeout in seconds for MCP gateway calls.

    Deep Research references
    ------------------------
    *  The Bright Data MCP integration follows the *Web Data UNLOCKED*
       hackathon spec: ``POST /web_unlocker`` with ``render_js=true``.
    *  Signal parsing adopts the ``json-repair`` strategy recommended for
       LLM-generated structured outputs that may contain trailing commas,
       single quotes, or truncated arrays.
    *  The 50-persona swarm feeds into this researcher; each persona's
       prior bias modulates how the researcher weighs conflicting signals.
    """

    def __init__(
        self,
        mcp_endpoint: str = "http://localhost:3000",
        mock_mode: bool = True,
        request_timeout: float = 30.0,
        bright_data_token: str | None = None,
        bright_data_zone: str = "unlocker",
    ) -> None:
        self._endpoint = mcp_endpoint.rstrip("/")
        self._mock = mock_mode
        self._timeout = aiohttp.ClientTimeout(total=request_timeout)
        self._session: aiohttp.ClientSession | None = None
        # Bright Data Web Unlocker direct API config
        self._bd_token = bright_data_token or os.environ.get("BRIGHT_DATA_API_TOKEN", "")
        self._bd_zone = os.environ.get("BRIGHT_DATA_UNLOCKER_ZONE", bright_data_zone)
        self._bd_api_url = "https://api.brightdata.com/request"

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=self._timeout,
                max_line_size=32768,
                max_field_size=32768
            )
        return self._session

    async def close(self) -> None:
        """Close the underlying HTTP session."""
        if self._session and not self._session.closed:
            await self._session.close()

    # ------------------------------------------------------------------
    # JSON repair
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_and_repair_json(raw: str) -> dict[str, Any]:
        """Parse potentially malformed JSON using ``json-repair``.

        LLM-generated JSON frequently contains:
        *  Trailing commas in objects / arrays.
        *  Single-quoted strings instead of double-quoted.
        *  Unquoted keys.
        *  Truncated output (missing closing braces / brackets).

        ``json-repair`` handles all of these cases gracefully.  If even
        the repaired output is not a dict, a ``ValueError`` is raised.

        Parameters
        ----------
        raw : str
            The raw JSON (or almost-JSON) string.

        Returns
        -------
        dict[str, Any]
            Parsed and sanitised dictionary.

        Raises
        ------
        ValueError
            If the repaired output is not a JSON object.
        """
        repaired: Any = json_repair.loads(raw)
        if isinstance(repaired, dict):
            return repaired
        # json-repair may return a list if the top-level was an array.
        if isinstance(repaired, list) and repaired:
            first = repaired[0]
            if isinstance(first, dict):
                return first
        raise ValueError(
            f"Repaired JSON is not an object: {type(repaired).__name__}"
        )

    # ------------------------------------------------------------------
    # News Ingestion
    # ------------------------------------------------------------------

    async def fetch_news_rss(self, ticker: str) -> list[str]:
        """Fetch latest news headlines for a ticker from Google News RSS feed."""
        if self._mock:
            return [
                f"Mock News 1: {ticker} shows strong quarterly resilience.",
                f"Mock News 2: Analysts upgrade {ticker} targets ahead of earnings.",
                f"Mock News 3: Market dynamics favor {ticker} mid-term trends."
            ]
        session = await self._ensure_session()
        url = f"https://news.google.com/rss/search?q={ticker}+stock&hl=en-US&gl=US&ceid=US:en"
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            }
            async with session.get(url, headers=headers, timeout=10) as resp:
                if resp.status == 200:
                    xml_text = await resp.text()
                    import xml.etree.ElementTree as ET
                    root = ET.fromstring(xml_text)
                    headlines = []
                    for item in root.findall(".//item")[:5]:
                        title = item.find("title")
                        if title is not None and title.text:
                            # Strip source from end (e.g. "... - Reuters")
                            h = title.text
                            if " - " in h:
                                h = h.rsplit(" - ", 1)[0]
                            headlines.append(h.strip())
                    return headlines
        except Exception as e:
            logger.warning(f"Failed to fetch Google News RSS for {ticker}: {e}")
        return []

    # ------------------------------------------------------------------
    # Core ingestion
    # ------------------------------------------------------------------

    async def ingest_macro_report(
        self, target_url: str
    ) -> MarketSignalSchema:
        """Fetch a macro report from the web and parse it into a signal.

        In **live mode** the method POSTs to the MCP gateway's
        ``/web_unlocker`` endpoint, receives raw page content, and
        attempts to extract a JSON-serialised ``MarketSignalSchema``
        from within the body.

        In **mock mode** it returns a synthetic Fed / CPI / unemployment
        signal deterministically seeded by the URL hash.

        Parameters
        ----------
        target_url : str
            The URL of the macro report to ingest (e.g. a BLS release,
            FRED page, Fed minutes transcript).

        Returns
        -------
        MarketSignalSchema
            The validated, structured market intelligence signal.

        Raises
        ------
        aiohttp.ClientError
            On HTTP-level failures (only in live mode).
        ValueError
            If the response body cannot be parsed into a valid schema.
        """
        if self._mock:
            # Seed RNG from URL for reproducible mocks.
            seed = int(hashlib.md5(target_url.encode()).hexdigest()[:8], 16)
            random.seed(seed)
            signal = _generate_mock_signal()
            random.seed()  # re-seed from entropy
            logger.info(
                "Mock researcher: generated signal for %s → %s %.3f",
                target_url,
                signal.ticker,
                signal.sentiment_score,
            )
            return signal

        # --- Live mode: call Bright Data MCP free tier (SSE), with HTTP fallback ---
        session = await self._ensure_session()
        body = None
        self._used_bright_data = False
        
        if self._bd_token:
            try:
                from mcp import ClientSession as MCPClientSession
                from mcp.client.sse import sse_client

                mcp_url = f"https://mcp.brightdata.com/sse?token={self._bd_token}"
                logger.info("Attempting to ingest via Bright Data MCP free tier (SSE)...")
                
                async with sse_client(mcp_url) as (read_stream, write_stream):
                    async with MCPClientSession(read_stream, write_stream) as mcp_session:
                        await mcp_session.initialize()
                        
                        result = await mcp_session.call_tool(
                            "scrape_as_markdown",
                            arguments={"url": target_url}
                        )
                        
                        if result and result.content:
                            content_text = result.content[0].text
                            if "execution failed" in content_text.lower() or "bad gateway" in content_text.lower() or len(content_text) < 300:
                                logger.warning(f"Bright Data MCP returned error message or invalid body. Using direct HTTP fallback.")
                                body = None
                            else:
                                body = content_text
                                self._used_bright_data = True
                                logger.info(f"Bright Data MCP scrape_as_markdown returned OK for {target_url} ({len(body)} chars)")
                        else:
                            logger.warning("Bright Data MCP returned empty content. Using direct HTTP fallback.")
            except Exception as e:
                logger.warning(f"Bright Data MCP free tier failed: {e}. Using direct HTTP fallback.")

        # Fallback to direct HTTP GET request if gateway fails or returns non-200
        if body is None:
            try:
                logger.info(f"Direct HTTP fallback: fetching {target_url} directly...")
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.5",
                }
                async with session.get(target_url, headers=headers) as resp:
                    resp.raise_for_status()
                    body = await resp.text()
                    logger.info(f"Successfully fetched target URL directly: {target_url}")
            except Exception as e:
                logger.error(f"Direct HTTP fallback failed: {e}")
                raise ValueError(f"Unable to retrieve market data from {target_url} via gateway or direct fallback.")

        # Parse the returned payload
        try:
            # Try to parse as JSON first (useful if the gateway or target URL outputs structured JSON)
            data = self._parse_and_repair_json(body)
            # Ensure it is not an arbitrary JSON object (e.g. from an HTML page containing scripts)
            if not isinstance(data, dict) or not all(k in data for k in ["ticker", "sentiment_score", "macro_context"]):
                raise ValueError("JSON is missing required market signal fields.")
            ticker = data.get("ticker", "SPY")
            news = await self.fetch_news_rss(ticker)
            if news:
                data["macro_context"] = f"Recent News: {' | '.join(news)}. " + data.get("macro_context", "")
            # Ensure dynamic confidence score is always calculated
            data["confidence"] = round(0.5 + 0.5 * abs(data["sentiment_score"]), 3)
            # Clean exposed code from snippet and macro context if any
            if "macro_context" in data:
                data["macro_context"] = _clean_exposed_code(data["macro_context"])
            if "raw_snippet" in data:
                data["raw_snippet"] = _clean_exposed_code(data["raw_snippet"])
        except Exception:
            # Fallback for raw HTML (e.g. direct fetch of a BLS/FRED page): strip tags and parse sentiment naively
            logger.info("Raw HTML/text content detected. Running naive text sentiment parser.")
            
            import re
            # Remove scripts and styles along with their contents to avoid exposing JS/CSS code in raw_snippet
            clean_body = re.sub(r"<script.*?>.*?</script>", " ", body, flags=re.DOTALL | re.IGNORECASE)
            clean_body = re.sub(r"<style.*?>.*?</style>", " ", clean_body, flags=re.DOTALL | re.IGNORECASE)
            # Strip remaining HTML tags and clean code
            clean_text = _clean_exposed_code(clean_body)
            
            # Infer the asset ticker first based on URL or keyword occurrence
            import urllib.parse
            path_parts = urllib.parse.urlparse(target_url).path.strip("/").split("/")
            ticker = "SPY"
            if "quote" in path_parts and len(path_parts) > path_parts.index("quote") + 1:
                ticker = path_parts[path_parts.index("quote") + 1].upper()
            else:
                for t in ["GLD", "QQQ", "TLT", "BTC", "ETH"]:
                    if t.lower() in clean_text.lower():
                        ticker = t
                        break

            # Compute sentiment score. If it's a Yahoo Finance quote page, let's extract the market change percent and price
            sentiment = 0.0
            price = None
            parsed_yfi = False
            if "finance.yahoo.com" in target_url:
                # Try SvelteKit fetched JSON data blocks first (incredibly robust)
                import html
                pattern = rf'<script\s+type="application/json"\s+data-sveltekit-fetched\s+data-url="([^"]*finance/quote[^"]*symbols={ticker}[^"]*)"[^>]*>(.*?)</script>'
                match_svelte = re.search(pattern, body, re.DOTALL | re.IGNORECASE)
                if match_svelte:
                    try:
                        script_content = html.unescape(match_svelte.group(2).strip())
                        data = json.loads(script_content)
                        body_str = data.get("body", "")
                        body_json = json.loads(body_str)
                        quote_result = body_json.get("quoteResponse", {}).get("result", [])
                        if not quote_result:
                            quote_result = body_json.get("result", [])
                        if quote_result:
                            quote = quote_result[0]
                            price_raw = quote.get("regularMarketPrice", {}).get("raw") or quote.get("regularMarketPrice", 0)
                            price = float(price_raw) if price_raw else None
                            change_pct_raw = quote.get("regularMarketChangePercent", {}).get("raw") or quote.get("regularMarketChangePercent", 0)
                            change_pct = float(change_pct_raw) if change_pct_raw else 0.0
                            # SvelteKit ChangePercent is percentage direct value, e.g. 0.54% = 0.54. Map to sentiment (capping at [-1.0, 1.0])
                            # If change_pct raw is a fraction (e.g. 0.0054), multiply by 50.0, else if it is percent (e.g. 0.54), multiply by 0.5
                            multiplier = 50.0 if abs(change_pct) < 0.05 else 0.5
                            sentiment = max(-1.0, min(1.0, change_pct * multiplier))
                            logger.info(f"Yahoo Finance SvelteKit Parser: Detected {ticker} price={price}, change={change_pct:.4f}%. Assigned sentiment={sentiment:.3f}")
                            parsed_yfi = True
                    except Exception as e:
                        logger.warning(f"Failed to parse SvelteKit quote data for {ticker}: {e}")

                if not parsed_yfi:
                    # Fallback to general regex searches (might match first ticker in trending list)
                    match = re.search(r'"regularMarketChangePercent"\s*:\s*\{\s*"raw"\s*:\s*(-?\d+\.?\d*)', body)
                    if match:
                        change_pct = float(match.group(1))
                        multiplier = 50.0 if abs(change_pct) < 0.05 else 0.5
                        sentiment = max(-1.0, min(1.0, change_pct * multiplier))
                        logger.info(f"Yahoo Finance Regex Parser: Detected regularMarketChangePercent={change_pct}%. Assigned sentiment={sentiment:.3f}")
                        parsed_yfi = True
                    else:
                        match_alt = re.search(r'"regularMarketChange"\s*:\s*\{\s*"raw"\s*:\s*(-?\d+\.?\d*)', body)
                        if match_alt:
                            change_val = float(match_alt.group(1))
                            sentiment = max(-1.0, min(1.0, change_val * 2.0))
                            logger.info(f"Yahoo Finance Regex Parser: Detected regularMarketChange={change_val}. Assigned sentiment={sentiment:.3f}")
                            parsed_yfi = True

                    match_price = re.search(r'"regularMarketPrice"\s*:\s*\{\s*"raw"\s*:\s*(-?\d+\.?\d*)', body)
                    if match_price:
                        price = float(match_price.group(1))
                        logger.info(f"Yahoo Finance Regex Parser: Detected regularMarketPrice={price}")
            
            # Ingest news headlines and combine into macro_context
            news = await self.fetch_news_rss(ticker)
            if news:
                news_context = " | ".join(news)
                macro_context = f"Recent News: {news_context}. Market Sentiment: {clean_text[:150]}..."
            else:
                macro_context = clean_text[:300] + "..."
            
            if sentiment > 0.15:
                direction = SentimentDirection.BULLISH
            elif sentiment < -0.15:
                direction = SentimentDirection.BEARISH
            else:
                direction = SentimentDirection.NEUTRAL
                
            data = {
                "ticker": ticker,
                "sentiment_score": sentiment,
                "macro_context": macro_context,
                "direction": direction,
                "confidence": round(0.5 + 0.5 * abs(sentiment), 3),
                "raw_snippet": clean_text[:450],
                "price": price
            }

        # Ensure source verification hash.
        data.setdefault(
            "source_verification",
            hashlib.sha256(body.encode()).hexdigest(),
        )
        data.setdefault("timestamp_utc", time.time())

        return MarketSignalSchema.model_validate(data)

def _clean_exposed_code(text: str) -> str:
    import re
    # 1. Remove markdown code blocks
    text = re.sub(r"```.*?```", " ", text, flags=re.DOTALL)
    
    # 2. Split text into segments by lines or sentence boundaries
    segments = re.split(r"(\n|\r|\. )", text)
    cleaned_segments = []
    
    code_keywords = [
        "window.", "function(", "Date.now", "allowContent", "YAHOO", "allowAds", 
        "content-type", "display:none", "margin-top", "padding-", "behavior:", 
        "flex-direction", "box-sizing", "align-items", "justify-content", "background-color",
        "var ", "const ", "let ", "typeof ", "undefined"
    ]
    
    for seg in segments:
        letters = sum(1 for c in seg if c.isalpha())
        if not letters:
            cleaned_segments.append(seg)
            continue
            
        # Count only JS/CSS specific symbols: { } ; =
        special_chars = sum(1 for c in seg if c in "{};=")
        
        is_code = False
        ratio = special_chars / letters
        if ratio > 0.05:
            is_code = True
        else:
            for kw in code_keywords:
                if kw in seg:
                    is_code = True
                    break
                    
        if not is_code:
            cleaned_segments.append(seg)
        else:
            cleaned_segments.append(" ")
            
    # Remove HTML tags
    cleaned_text = "".join(cleaned_segments)
    cleaned_text = re.sub(r"<[^>]+?>", " ", cleaned_text)
    return " ".join(cleaned_text.split())

    # ------------------------------------------------------------------
    # Social sentiment
    # ------------------------------------------------------------------

    async def scan_social_sentiment(
        self,
        query: str,
        sources: list[str] | None = None,
    ) -> list[SocialSentimentHit]:
        """Run a SERP-based social sentiment scan.

        Uses the MCP gateway's ``/serp`` endpoint to search across
        social-media sources and extract sentiment signals.

        Parameters
        ----------
        query : str
            Search query (e.g. ``"BTC price prediction"``).
        sources : list[str], optional
            Restrict results to these source platforms.  Defaults to
            ``["twitter", "reddit", "telegram"]``.

        Returns
        -------
        list[SocialSentimentHit]
            Parsed social-sentiment hits.
        """
        sources = sources or ["twitter", "reddit", "telegram"]

        if self._mock:
            hits = _generate_mock_social_hits(query, n=len(sources) * 2)
            logger.info(
                "Mock researcher: generated %d social hits for '%s'.",
                len(hits),
                query,
            )
            return hits

        # --- Live mode ---
        session = await self._ensure_session()
        all_hits: list[SocialSentimentHit] = []

        for source in sources:
            payload = {
                "query": f"site:{source}.com {query}",
                "num_results": 10,
                "format": "json",
            }
            url = f"{self._endpoint}/serp"
            try:
                async with session.post(url, json=payload) as resp:
                    resp.raise_for_status()
                    body = await resp.text()
                results = self._parse_and_repair_json(body)
                organic = results.get("organic_results", results.get("results", []))
                if not isinstance(organic, list):
                    organic = []
                for item in organic:
                    snippet = item.get("snippet", item.get("description", ""))
                    # Naïve keyword sentiment: count bullish vs bearish terms.
                    score = self._naive_keyword_sentiment(snippet)
                    all_hits.append(
                        SocialSentimentHit(
                            source=source,
                            title=item.get("title", ""),
                            snippet=snippet[:500],
                            sentiment_score=score,
                            url=item.get("link", item.get("url", "")),
                            timestamp_utc=time.time(),
                        )
                    )
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Social scan for source '%s' failed: %s", source, exc
                )

        return all_hits

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _naive_keyword_sentiment(text: str) -> float:
        """Ultra-simple keyword-based sentiment scorer.

        Counts occurrences of bullish and bearish lexicons and returns a
        normalised score in ``[-1, 1]``.  This is intentionally simplistic
        — production systems should use the LLM personas for nuanced
        sentiment analysis.

        Parameters
        ----------
        text : str
            The snippet to score.

        Returns
        -------
        float
            Normalised sentiment score.
        """
        text_lower = text.lower()
        bullish_words = {
            "bullish", "rally", "surge", "moon", "pump", "breakout",
            "upgrade", "beat", "exceeded", "strong", "growth", "gain",
            "buy", "accumulate", "outperform", "recovery", "soar",
        }
        bearish_words = {
            "bearish", "crash", "dump", "plunge", "selloff", "downgrade",
            "miss", "weak", "decline", "loss", "sell", "underperform",
            "recession", "contraction", "default", "collapse", "drop",
        }
        bull_count = sum(1 for w in bullish_words if w in text_lower)
        bear_count = sum(1 for w in bearish_words if w in text_lower)
        total = bull_count + bear_count
        if total == 0:
            return 0.0
        return round((bull_count - bear_count) / total, 4)

    def __repr__(self) -> str:
        mode = "mock" if self._mock else "live"
        return (
            f"<TradFiResearcher endpoint={self._endpoint!r} mode={mode}>"
        )
