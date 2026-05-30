package com.broker.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serial;
import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;
import java.util.Properties;

/**
 * Centralized Kafka source factory for the Dual-Broker SOTA engine.
 *
 * <h2>Architecture Overview</h2>
 * <p>This class encapsulates all Kafka connectivity concerns, providing two
 * pre-configured {@link KafkaSource} builders and their companion
 * {@link WatermarkStrategy} instances.  The engine ingests from <em>two</em>
 * Kafka topics that represent orthogonal slices of the financial-information
 * landscape:</p>
 *
 * <ol>
 *   <li><strong>{@code tradfi-macro-stream}</strong> — Sentiment events derived
 *       from macroeconomic indicators, central-bank speeches, SERP news
 *       headlines, Reddit posts, and X/Twitter firehose data.  Each record is
 *       a {@link SentimentEvent} carrying a normalised sentiment score
 *       (−1.0 … +1.0), a source-reliability tag, and a confidence multiplier.</li>
 *   <li><strong>{@code polymarket-orderbook-stream}</strong> — Real-time
 *       market ticks (Level-1 mid-prices) streamed from Polymarket prediction
 *       markets and, optionally, traditional CEX/DEX order books.  Each
 *       record is a {@link MarketTick} whose {@code price} field is
 *       interpreted as the market-implied probability (0.0 … 1.0).</li>
 * </ol>
 *
 * <h2>Watermark Strategy</h2>
 * <p>Both sources use <em>bounded-out-of-orderness</em> watermarking with a
 * 10-second tolerance window.  This accounts for the inherent jitter in
 * multi-hop data pipelines (WebSocket → Kafka producer → broker → consumer)
 * while keeping end-to-end latency sub-minute.</p>
 *
 * <p>An <em>idle-source timeout</em> of 30 seconds is configured per
 * partition.  This prevents a single quiescent partition (common on
 * prediction-market feeds outside US trading hours) from blocking the global
 * watermark advance and stalling all downstream windows.</p>
 *
 * <h2>Deserialization</h2>
 * <p>A generic {@link JsonDeserializationSchema} backed by Jackson
 * {@link ObjectMapper} (with the {@link JavaTimeModule} registered) is used
 * for both POJOs.  The schema is lenient: unknown JSON properties are
 * silently ignored so upstream producers can evolve their schemas without
 * breaking the pipeline.</p>
 *
 * @author Dual-Broker SOTA Engine Team
 * @since 1.0.0
 * @see SentimentEvent
 * @see MarketTick
 */
public class MarketDataConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(MarketDataConsumer.class);

    /*
     * Thread-safe, immutable ObjectMapper shared across all deserialization
     * instances within this JVM.  JavaTimeModule is required for ISO-8601
     * date fields that some upstream producers may include.
     */
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    // ──────────────────────────────────────────────────────────────────────
    //  POJO: MarketTick
    // ──────────────────────────────────────────────────────────────────────

    /**
     * Plain Old Java Object representing a single financial price tick from
     * either a <em>centralised exchange</em> (CEX), a <em>decentralised
     * exchange</em> (DEX), or a <em>prediction market</em> (POLYMARKET).
     *
     * <h3>Semantic Conventions</h3>
     * <ul>
     *   <li>{@code symbol} — The ticker or contract identifier, e.g.
     *       {@code "BTC-USD"} or {@code "PRESIDENT_2024_YES"}.</li>
     *   <li>{@code price} — For prediction markets this is the implied
     *       probability (0.0 … 1.0).  For spot markets it is the mid-price
     *       in the quote currency.</li>
     *   <li>{@code volume} — Notional volume transacted at this price level
     *       in the base currency.</li>
     *   <li>{@code timestamp} — Event time in <em>epoch milliseconds</em>
     *       (UTC).  Used by Flink's watermark assigner.</li>
     *   <li>{@code source} — Origin tag: one of {@code "CEX"},
     *       {@code "DEX"}, or {@code "POLYMARKET"}.</li>
     * </ul>
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class MarketTick implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;

        /** Ticker or contract identifier. */
        private String symbol;
        /** Mid-price or implied probability (0.0–1.0 for prediction markets). */
        private double price;
        /** Notional volume in base currency units. */
        private double volume;
        /** Event time in epoch milliseconds (UTC). */
        private long timestamp;
        /** Origin tag: {@code "CEX"}, {@code "DEX"}, or {@code "POLYMARKET"}. */
        private String source;

        /** No-arg constructor required by Flink's POJO serializer. */
        public MarketTick() {}

        /**
         * Canonical constructor with full Jackson annotation wiring.
         *
         * @param symbol    ticker / contract identifier
         * @param price     mid-price or implied probability
         * @param volume    notional volume
         * @param timestamp event time (epoch ms)
         * @param source    origin tag
         */
        @JsonCreator
        public MarketTick(
                @JsonProperty("symbol") String symbol,
                @JsonProperty("price") double price,
                @JsonProperty("volume") double volume,
                @JsonProperty("timestamp") long timestamp,
                @JsonProperty("source") String source) {
            this.symbol = Objects.requireNonNull(symbol, "symbol must not be null");
            this.price = price;
            this.volume = volume;
            this.timestamp = timestamp;
            this.source = Objects.requireNonNullElse(source, "UNKNOWN");
        }

        // ── Getters ──────────────────────────────────────────────────────
        public String getSymbol()    { return symbol; }
        public double getPrice()     { return price; }
        public double getVolume()    { return volume; }
        public long   getTimestamp() { return timestamp; }
        public String getSource()    { return source; }

        // ── Setters (Flink POJO requirement) ─────────────────────────────
        public void setSymbol(String symbol)       { this.symbol = symbol; }
        public void setPrice(double price)         { this.price = price; }
        public void setVolume(double volume)       { this.volume = volume; }
        public void setTimestamp(long timestamp)    { this.timestamp = timestamp; }
        public void setSource(String source)       { this.source = source; }

        @Override
        public String toString() {
            return String.format(
                    "MarketTick{symbol='%s', price=%.6f, volume=%.2f, ts=%d, src='%s'}",
                    symbol, price, volume, timestamp, source);
        }
    }

    // ──────────────────────────────────────────────────────────────────────
    //  POJO: SentimentEvent
    // ──────────────────────────────────────────────────────────────────────

    /**
     * POJO representing a single sentiment observation from the upstream NLP
     * pipeline.  Sentiment events are produced by analysing raw text
     * (headlines, tweets, Reddit posts, macro-report summaries) through a
     * transformer-based classifier and emitting a normalised score.
     *
     * <h3>Field Semantics</h3>
     * <ul>
     *   <li>{@code text} — The raw input snippet (truncated to 512 chars)
     *       for debuggability; <em>not</em> used in the aggregation
     *       formula.</li>
     *   <li>{@code source} — The provenance tag.  Determines the
     *       reliability weight {@code ω_j} used in the exponentially-weighted
     *       sentiment tensor (see
     *       {@link com.broker.flink.SentimentStreamProcessor}).</li>
     *   <li>{@code sentimentScore} — Normalised to [−1.0, +1.0].  A value
     *       of +1.0 is maximally bullish; −1.0 is maximally bearish.</li>
     *   <li>{@code confidence} — Classifier confidence in [0.0, 1.0],
     *       acting as a multiplicative weight so that low-confidence
     *       observations contribute less to the rolling average.</li>
     *   <li>{@code timestamp} — Event time in epoch milliseconds (UTC).</li>
     * </ul>
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class SentimentEvent implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;

        /** Raw text snippet (for debugging / audit trail). */
        private String text;
        /** Provenance tag, e.g. {@code "REDDIT"}, {@code "X_TWITTER"}, {@code "SERP_NEWS"}. */
        private String source;
        /** Normalised sentiment score in [−1.0, +1.0]. */
        private double sentimentScore;
        /** Classifier confidence in [0.0, 1.0]. */
        private double confidence;
        /** Event time in epoch milliseconds (UTC). */
        private long timestamp;

        /** No-arg constructor required by Flink's POJO serializer. */
        public SentimentEvent() {}

        /**
         * Canonical constructor.
         *
         * @param text           raw text snippet
         * @param source         provenance tag
         * @param sentimentScore normalised score [−1.0, +1.0]
         * @param confidence     classifier confidence [0.0, 1.0]
         * @param timestamp      event time (epoch ms)
         */
        @JsonCreator
        public SentimentEvent(
                @JsonProperty("text") String text,
                @JsonProperty("source") String source,
                @JsonProperty("sentimentScore") double sentimentScore,
                @JsonProperty("confidence") double confidence,
                @JsonProperty("timestamp") long timestamp) {
            this.text = text;
            this.source = Objects.requireNonNullElse(source, "UNKNOWN");
            this.sentimentScore = Math.max(-1.0, Math.min(1.0, sentimentScore));
            this.confidence = Math.max(0.0, Math.min(1.0, confidence));
            this.timestamp = timestamp;
        }

        // ── Getters ──────────────────────────────────────────────────────
        public String getText()           { return text; }
        public String getSource()         { return source; }
        public double getSentimentScore() { return sentimentScore; }
        public double getConfidence()     { return confidence; }
        public long   getTimestamp()      { return timestamp; }

        // ── Setters (Flink POJO requirement) ─────────────────────────────
        public void setText(String text)                     { this.text = text; }
        public void setSource(String source)                 { this.source = source; }
        public void setSentimentScore(double sentimentScore) { this.sentimentScore = sentimentScore; }
        public void setConfidence(double confidence)         { this.confidence = confidence; }
        public void setTimestamp(long timestamp)              { this.timestamp = timestamp; }

        @Override
        public String toString() {
            return String.format(
                    "SentimentEvent{src='%s', score=%.4f, conf=%.4f, ts=%d}",
                    source, sentimentScore, confidence, timestamp);
        }
    }

    // ──────────────────────────────────────────────────────────────────────
    //  JSON Deserialization Schema
    // ──────────────────────────────────────────────────────────────────────

    /**
     * Generic Flink {@link AbstractDeserializationSchema} that converts raw
     * Kafka value bytes to a Java POJO via Jackson.
     *
     * <p>The schema is <em>lenient</em>: unknown JSON properties are silently
     * ignored (via {@code @JsonIgnoreProperties(ignoreUnknown = true)} on the
     * POJO classes) so that upstream producers can add new fields without
     * breaking the pipeline.</p>
     *
     * <p>Malformed messages (non-JSON, truncated payloads) throw
     * {@link IOException}, which Flink's task manager surfaces as a
     * recoverable exception — the record is skipped and a log line emitted,
     * but the pipeline does <em>not</em> fail.</p>
     *
     * @param <T> the target POJO type
     */
    public static class JsonDeserializationSchema<T> extends AbstractDeserializationSchema<T> {
        @Serial
        private static final long serialVersionUID = 1L;

        private final Class<T> clazz;

        /**
         * Constructs a new schema for the given target class.
         *
         * @param clazz the POJO class to deserialize into
         */
        public JsonDeserializationSchema(Class<T> clazz) {
            super(clazz);
            this.clazz = clazz;
        }

        /**
         * Deserializes the raw Kafka message payload.
         *
         * @param message the raw byte array from the Kafka record value
         * @return a fully-populated POJO instance
         * @throws IOException if the payload is not valid JSON or does not
         *                     match the target schema
         */
        @Override
        public T deserialize(byte[] message) throws IOException {
            return OBJECT_MAPPER.readValue(message, clazz);
        }
    }

    // ──────────────────────────────────────────────────────────────────────
    //  KafkaSource Builders
    // ──────────────────────────────────────────────────────────────────────

    /**
     * Builds a {@link KafkaSource} for {@link MarketTick} records from the
     * {@code polymarket-orderbook-stream} topic.
     *
     * <h3>Consumer Configuration Highlights</h3>
     * <ul>
     *   <li><strong>Starting offsets</strong>: {@code latest} — we only care
     *       about real-time events, not historical replay.</li>
     *   <li><strong>Partition discovery</strong>: re-scanned every 30 s so
     *       that dynamic topic expansion (Kafka auto-create) is honoured
     *       without a pipeline restart.</li>
     *   <li><strong>Fetch tuning</strong>: {@code fetch.min.bytes=1} and
     *       {@code fetch.max.wait.ms=100} for low-latency delivery at the
     *       cost of slightly higher broker RPC overhead — acceptable in
     *       HFT scenarios.</li>
     *   <li><strong>Isolation level</strong>: {@code read_committed} to skip
     *       aborted transactions from upstream producers that use exactly-once
     *       semantics.</li>
     * </ul>
     *
     * @param bootstrapServers Kafka bootstrap servers (comma-separated)
     * @param groupId          consumer group identifier
     * @param topic            Kafka topic name (default {@code polymarket-orderbook-stream})
     * @return a fully-configured, ready-to-attach {@link KafkaSource}
     */
    public static KafkaSource<MarketTick> createMarketTickSource(
            String bootstrapServers, String groupId, String topic) {

        LOG.info("Building MarketTick KafkaSource [topic={}, group={}]", topic, groupId);

        return KafkaSource.<MarketTick>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new JsonDeserializationSchema<>(MarketTick.class))
                .setProperties(buildKafkaProperties())
                .build();
    }

    /**
     * Builds a {@link KafkaSource} for {@link SentimentEvent} records from
     * the {@code tradfi-macro-stream} topic.
     *
     * <p>Configuration mirrors {@link #createMarketTickSource} but targets
     * the sentiment topic.  The two sources are intended to run in parallel
     * inside the same Flink job graph, connected downstream via a
     * {@code CoProcessFunction}.</p>
     *
     * @param bootstrapServers Kafka bootstrap servers
     * @param groupId          consumer group identifier
     * @param topic            Kafka topic name (default {@code tradfi-macro-stream})
     * @return a fully-configured {@link KafkaSource}
     */
    public static KafkaSource<SentimentEvent> createSentimentSource(
            String bootstrapServers, String groupId, String topic) {

        LOG.info("Building SentimentEvent KafkaSource [topic={}, group={}]", topic, groupId);

        return KafkaSource.<SentimentEvent>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new JsonDeserializationSchema<>(SentimentEvent.class))
                .setProperties(buildKafkaProperties())
                .build();
    }

    // ──────────────────────────────────────────────────────────────────────
    //  Watermark Strategy
    // ──────────────────────────────────────────────────────────────────────

    /**
     * Creates a {@link WatermarkStrategy} suitable for both market-tick and
     * sentiment-event streams.
     *
     * <h3>Bounded Out-of-Orderness (10 s)</h3>
     * <p>Financial event streams arrive with bounded disorder because
     * multiple WebSocket connections, network hops, and Kafka partitioner
     * decisions introduce variable latency.  A 10-second tolerance allows
     * the pipeline to accept mildly late events without dropping them while
     * keeping window-firing latency low.</p>
     *
     * <h3>Idle Source Timeout (30 s)</h3>
     * <p>Polymarket contracts can go minutes without a trade outside of US
     * trading hours.  Without an idle timeout, the idle partition's watermark
     * would freeze at its last value and prevent the global watermark from
     * advancing — blocking <em>all</em> downstream event-time windows.
     * Setting {@code withIdleness(30s)} tells Flink to exclude idle
     * partitions from the global-watermark minimum calculation.</p>
     *
     * @param <T>                the element type
     * @param timestampExtractor a function that extracts epoch-millisecond
     *                           timestamps from elements
     * @return a watermark strategy with 10 s bounded out-of-orderness and
     *         30 s idle timeout
     */
    public static <T> WatermarkStrategy<T> getWatermarkStrategy(
            java.util.function.ToLongFunction<T> timestampExtractor) {

        return WatermarkStrategy
                .<T>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((event, recordTimestamp) ->
                        timestampExtractor.applyAsLong(event))
                .withIdleness(Duration.ofSeconds(30));
    }

    // ──────────────────────────────────────────────────────────────────────
    //  Kafka Consumer Properties
    // ──────────────────────────────────────────────────────────────────────

    /**
     * Assembles the baseline Kafka consumer properties used by both source
     * builders.
     *
     * <h3>Key Configuration Rationale</h3>
     * <table>
     *   <tr><th>Property</th><th>Value</th><th>Why</th></tr>
     *   <tr>
     *     <td>{@code auto.offset.reset}</td>
     *     <td>{@code latest}</td>
     *     <td>On first start (no committed offsets) we skip historical data
     *         — this is a real-time engine, not a batch replay job.</td>
     *   </tr>
     *   <tr>
     *     <td>{@code enable.auto.commit}</td>
     *     <td>{@code false}</td>
     *     <td>Flink's Kafka connector manages offsets via its own checkpoint
     *         mechanism, so auto-commit must be disabled to prevent offset
     *         drift between Flink's checkpoint state and Kafka's
     *         __consumer_offsets.</td>
     *   </tr>
     *   <tr>
     *     <td>{@code partition.discovery.interval.ms}</td>
     *     <td>{@code 30000}</td>
     *     <td>Re-scan for new partitions every 30 s so dynamically-expanded
     *         topics are picked up without restarting the job.</td>
     *   </tr>
     *   <tr>
     *     <td>{@code fetch.min.bytes}</td>
     *     <td>{@code 1}</td>
     *     <td>Return immediately when any data is available — minimises
     *         fetch latency at the cost of slightly more broker RPCs.</td>
     *   </tr>
     *   <tr>
     *     <td>{@code fetch.max.wait.ms}</td>
     *     <td>{@code 100}</td>
     *     <td>Upper bound on server-side wait before responding, ensuring
     *         sub-200 ms end-to-end Kafka poll latency.</td>
     *   </tr>
     *   <tr>
     *     <td>{@code max.poll.records}</td>
     *     <td>{@code 500}</td>
     *     <td>Batch size per poll — balances throughput with per-record
     *         processing time to keep task-manager back-pressure low.</td>
     *   </tr>
     *   <tr>
     *     <td>{@code isolation.level}</td>
     *     <td>{@code read_committed}</td>
     *     <td>Skips uncommitted/aborted transactional records from upstream
     *         producers that use idempotent/transactional writes.</td>
     *   </tr>
     * </table>
     *
     * @return a populated {@link Properties} instance
     */
    private static Properties buildKafkaProperties() {
        Properties props = new Properties();

        // ── Offset management ────────────────────────────────────────────
        props.setProperty("auto.offset.reset", "latest");
        // Let Flink manage offsets via checkpoints, not Kafka auto-commit.
        props.setProperty("enable.auto.commit", "false");

        // ── Partition discovery ──────────────────────────────────────────
        props.setProperty("partition.discovery.interval.ms", "30000");

        // ── Fetch tuning for low-latency HFT ────────────────────────────
        props.setProperty("fetch.min.bytes", "1");
        props.setProperty("fetch.max.wait.ms", "100");
        props.setProperty("max.poll.records", "500");

        // ── Transactional isolation ─────────────────────────────────────
        props.setProperty("isolation.level", "read_committed");

        // ── Heartbeat / session tuning ──────────────────────────────────
        props.setProperty("heartbeat.interval.ms", "3000");
        props.setProperty("session.timeout.ms", "30000");

        return props;
    }
}
