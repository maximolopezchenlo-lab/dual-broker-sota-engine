package com.broker.flink;

import com.broker.kafka.MarketDataConsumer;
import com.broker.kafka.MarketDataConsumer.MarketTick;
import com.broker.kafka.MarketDataConsumer.SentimentEvent;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serial;
import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

/**
 * Main Apache Flink streaming job for the Dual-Broker SOTA engine.
 *
 * <h2>Pipeline Overview</h2>
 * <p>This class wires together the complete real-time data-fusion and
 * arbitrage-detection pipeline:</p>
 * <pre>
 *   Kafka(tradfi-macro-stream)                  Kafka(polymarket-orderbook-stream)
 *         │                                              │
 *         ▼                                              ▼
 *   SentimentEvent source                         MarketTick source
 *         │                                              │
 *         ▼                                              │
 *   keyBy("GLOBAL_SENTIMENT")                            │
 *         │                                              │
 *         ▼                                              │
 *   SlidingWindow(1h / 1min)                             │
 *   + SentimentDecayAggregator                           │
 *   + allowedLateness(1min)                              │
 *   + sideOutput(late events)                            │
 *         │                                              │
 *         ▼                                              │
 *   Aggregated sentiment (Double)                        │
 *         │                                              │
 *         └──────────────┐                               │
 *                        ▼                               ▼
 *                   connect() ◄──────────────────────────┘
 *                        │
 *                        ▼
 *              DivergenceEngine
 *              (KeyedCoProcessFunction)
 *                        │
 *                        ▼
 *              ArbitrageAlert (if D_JS > θ)
 *                        │
 *                        ▼
 *                   print() / Kafka sink
 * </pre>
 *
 * <h2>Sentiment Tensor — Exponentially-Weighted Moving Average</h2>
 * <p>The sliding window aggregator computes an <em>exponentially-weighted
 * sentiment tensor</em> using the recurrence:</p>
 * <blockquote>
 *   <strong>S<sub>t</sub> = e<sup>−λΔt</sup> · S<sub>t−1</sub>
 *   + (1 − e<sup>−λΔt</sup>) · Σ(ω<sub>j</sub> · E(d<sub>j</sub>))</strong>
 * </blockquote>
 * <p>where:</p>
 * <ul>
 *   <li><strong>λ</strong> (lambda) — the temporal decay constant.  Default
 *       0.01 per second; controls how quickly stale sentiment observations
 *       lose influence.</li>
 *   <li><strong>Δt</strong> — elapsed seconds since the previous observation.</li>
 *   <li><strong>ω<sub>j</sub></strong> — source-reliability weight for
 *       provenance tag <em>j</em> (e.g. SERP_NEWS = 1.0, X_TWITTER = 0.8,
 *       REDDIT = 0.6).</li>
 *   <li><strong>E(d<sub>j</sub>)</strong> — the raw sentiment score of
 *       document <em>d<sub>j</sub></em>, normalised to [−1, +1].</li>
 * </ul>
 * <p>The accumulator also tracks the cumulative weight sum so the final
 * result is the <em>weighted average</em> of all observations within the
 * window, discounted by recency.</p>
 *
 * <h2>Jensen-Shannon Divergence</h2>
 * <p>The aggregated sentiment is converted to a "swarm probability" <em>P</em>
 * via a scaled sigmoid.  The market-implied probability <em>Q</em> is
 * extracted directly from the Polymarket tick price.  The
 * <em>Jensen-Shannon Divergence</em> quantifies the information-theoretic
 * "distance" between these two distributions:</p>
 * <blockquote>
 *   <strong>D<sub>JS</sub>(P ‖ Q) = ½ D<sub>KL</sub>(P ‖ M)
 *   + ½ D<sub>KL</sub>(Q ‖ M)</strong>,
 *   where <strong>M = ½(P + Q)</strong>
 * </blockquote>
 * <p>When D<sub>JS</sub> exceeds a configurable threshold θ (default 0.05),
 * the pipeline emits an {@link ArbitrageAlert} signalling a statistically
 * significant disagreement between the crowd-sentiment and market-price
 * probability estimates — a potential trading opportunity.</p>
 *
 * <h2>Late-Event Handling</h2>
 * <ul>
 *   <li><strong>Allowed lateness (1 min)</strong>: Events that arrive up to
 *       1 minute after the window's watermark has passed are still included
 *       in a re-fired window computation.</li>
 *   <li><strong>Side output</strong>: Events arriving beyond the 1-minute
 *       lateness boundary are routed to a side output (tagged
 *       {@link #LATE_SENTIMENT_TAG}) for audit / dead-letter analysis.</li>
 * </ul>
 *
 * @author Dual-Broker SOTA Engine Team
 * @since 1.0.0
 * @see StateBackendConfig
 * @see MarketDataConsumer
 */
public class SentimentStreamProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(SentimentStreamProcessor.class);

    // ──────────────────────────────────────────────────────────────────────
    //  Configuration Defaults (overridable via CLI --key value)
    // ──────────────────────────────────────────────────────────────────────

    /** Default Kafka bootstrap servers. */
    private static final String DEFAULT_BOOTSTRAP = "localhost:9092";

    /** Default consumer group. */
    private static final String DEFAULT_GROUP_ID = "flink-sentiment-processor";

    /** Default topic for Polymarket order-book ticks. */
    private static final String DEFAULT_TICK_TOPIC = "polymarket-orderbook-stream";

    /** Default topic for sentiment / macro events. */
    private static final String DEFAULT_SENTIMENT_TOPIC = "tradfi-macro-stream";

    /** Default parallelism. */
    private static final int DEFAULT_PARALLELISM = 4;

    /** Default Jensen-Shannon divergence threshold for alerts. */
    private static final double DEFAULT_JS_THRESHOLD = 0.05;

    /** Default temporal decay constant λ (per second). */
    private static final double DEFAULT_LAMBDA = 0.01;

    // ──────────────────────────────────────────────────────────────────────
    //  Side-Output Tags
    // ──────────────────────────────────────────────────────────────────────

    /**
     * Side-output tag for sentiment events that arrive <em>after</em> the
     * allowed-lateness window (1 minute past the watermark).  These events
     * are too stale to influence the sentiment tensor but are captured for
     * observability and dead-letter analysis.
     */
    public static final OutputTag<SentimentEvent> LATE_SENTIMENT_TAG =
            new OutputTag<>("late-sentiment-events",
                    TypeInformation.of(SentimentEvent.class));

    /**
     * Side-output tag for market ticks that arrive extremely late.
     * In practice this is rare (order-book feeds are near-real-time) but
     * is included for completeness.
     */
    public static final OutputTag<MarketTick> LATE_TICK_TAG =
            new OutputTag<>("late-tick-events",
                    TypeInformation.of(MarketTick.class));

    // ──────────────────────────────────────────────────────────────────────
    //  Data Types
    // ──────────────────────────────────────────────────────────────────────

    /**
     * Arbitrage alert payload emitted when the Jensen-Shannon Divergence
     * between the swarm sentiment probability and the market-implied
     * probability exceeds the configured threshold.
     *
     * <p>Downstream consumers (trading bots, risk dashboards, notification
     * services) subscribe to a Kafka topic containing serialized instances
     * of this class.</p>
     */
    public static class ArbitrageAlert implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;

        /** The ticker or contract identifier. */
        private String symbol;

        /** Swarm-sentiment implied probability (P). */
        private double sentimentProbability;

        /** Market-implied probability from the tick price (Q). */
        private double marketProbability;

        /** Jensen-Shannon Divergence D_JS(P ‖ Q). */
        private double jensenShannonDivergence;

        /**
         * Signed edge: {@code sentimentProbability − marketProbability}.
         * Positive → sentiment is more bullish than the market;
         * negative → sentiment is more bearish.
         */
        private double edge;

        /** Event time of the triggering market tick (epoch ms). */
        private long timestamp;

        /** No-arg constructor for serialization frameworks. */
        public ArbitrageAlert() {}

        /**
         * Full constructor.
         *
         * @param symbol                   contract / ticker id
         * @param sentimentProbability     P (from sigmoid of sentiment score)
         * @param marketProbability        Q (from tick price)
         * @param jensenShannonDivergence  D_JS(P ‖ Q)
         * @param edge                     P − Q
         * @param timestamp                epoch milliseconds
         */
        public ArbitrageAlert(String symbol,
                              double sentimentProbability,
                              double marketProbability,
                              double jensenShannonDivergence,
                              double edge,
                              long timestamp) {
            this.symbol = symbol;
            this.sentimentProbability = sentimentProbability;
            this.marketProbability = marketProbability;
            this.jensenShannonDivergence = jensenShannonDivergence;
            this.edge = edge;
            this.timestamp = timestamp;
        }

        // ── Getters & Setters ────────────────────────────────────────────
        public String getSymbol()                    { return symbol; }
        public void   setSymbol(String symbol)       { this.symbol = symbol; }
        public double getSentimentProbability()       { return sentimentProbability; }
        public void   setSentimentProbability(double v) { this.sentimentProbability = v; }
        public double getMarketProbability()          { return marketProbability; }
        public void   setMarketProbability(double v)  { this.marketProbability = v; }
        public double getJensenShannonDivergence()    { return jensenShannonDivergence; }
        public void   setJensenShannonDivergence(double v) { this.jensenShannonDivergence = v; }
        public double getEdge()                       { return edge; }
        public void   setEdge(double edge)            { this.edge = edge; }
        public long   getTimestamp()                   { return timestamp; }
        public void   setTimestamp(long timestamp)     { this.timestamp = timestamp; }

        @Override
        public String toString() {
            return String.format(
                    "⚡ ArbitrageAlert{symbol='%s', P_swarm=%.4f, Q_market=%.4f, "
                  + "D_JS=%.6f, edge=%+.4f, ts=%d}",
                    symbol, sentimentProbability, marketProbability,
                    jensenShannonDivergence, edge, timestamp);
        }
    }

    // ──────────────────────────────────────────────────────────────────────
    //  Sentiment Accumulator
    // ──────────────────────────────────────────────────────────────────────

    /**
     * Mutable accumulator for the exponentially-weighted sentiment tensor.
     *
     * <p>Tracks the running weighted sum, cumulative weight, event count
     * (for metrics), and the timestamp of the last incorporated
     * observation.  The accumulator is designed to be <em>mergeable</em>
     * across session-window panes (required by Flink when windows are
     * merged).</p>
     */
    public static class SentimentAccumulator implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;

        /** Running sum of decayed, reliability-weighted sentiment scores. */
        public double weightedSum = 0.0;

        /** Running sum of decayed reliability weights. */
        public double weightCount = 0.0;

        /** Epoch-millisecond timestamp of the last incorporated event. */
        public long lastTimestamp = 0L;

        /** Total number of events incorporated (for monitoring). */
        public long eventCount = 0L;
    }

    // ──────────────────────────────────────────────────────────────────────
    //  AggregateFunction: Exponential-Decay Sentiment Aggregator
    // ──────────────────────────────────────────────────────────────────────

    /**
     * Flink {@link AggregateFunction} that computes an <em>incremental</em>
     * exponentially-weighted moving average of sentiment scores over a
     * sliding window.
     *
     * <h3>Mathematical Formulation</h3>
     * <p>For each incoming {@link SentimentEvent} with score
     * {@code E(d_j)}, source weight {@code ω_j}, and confidence
     * {@code c_j}, the accumulator is updated as:</p>
     * <pre>
     *   α      = e^(−λ · Δt)                          // temporal decay
     *   w_eff  = ω_j · c_j                             // effective weight
     *   S_sum  = α · S_sum  + (1 − α) · E(d_j) · w_eff
     *   W_sum  = α · W_sum  + (1 − α) · w_eff
     *   result = S_sum / W_sum                          // weighted average
     * </pre>
     *
     * <h3>Why Exponential Decay?</h3>
     * <p>Standard arithmetic averages assign equal weight to a 59-minute-old
     * headline and a 1-second-old tweet.  In fast-moving markets, recent
     * signals are far more informative.  The decay constant {@code λ}
     * (default 0.01 /s) gives a 1-minute-old event only
     * {@code e^(−0.01 × 60) ≈ 0.55} of its original weight — a smooth
     * discount that avoids the "cliff" artefact of tumbling windows.</p>
     *
     * <h3>Source Reliability Weights</h3>
     * <table>
     *   <tr><th>Source</th><th>ω</th><th>Rationale</th></tr>
     *   <tr><td>SERP_NEWS</td><td>1.0</td><td>Editorially curated;
     *       lowest noise</td></tr>
     *   <tr><td>BLOOMBERG</td><td>1.0</td><td>Institutional-grade terminal
     *       data</td></tr>
     *   <tr><td>X_TWITTER</td><td>0.8</td><td>High velocity but noisy;
     *       partially offset by large sample size</td></tr>
     *   <tr><td>REDDIT</td><td>0.6</td><td>Community-driven, high latency,
     *       susceptible to brigading</td></tr>
     *   <tr><td>TELEGRAM</td><td>0.5</td><td>Unverified channels;
     *       highest noise floor</td></tr>
     *   <tr><td>(default)</td><td>0.5</td><td>Unknown sources penalised
     *       by default</td></tr>
     * </table>
     *
     * <h3>Merge Semantics</h3>
     * <p>When Flink merges two window panes (e.g. during session-window
     * compaction or rebalance), both accumulators are first <em>decayed</em>
     * to the later timestamp before being summed.  This preserves the
     * temporal-decay invariant across merges.</p>
     */
    public static class SentimentDecayAggregator
            implements AggregateFunction<SentimentEvent, SentimentAccumulator, Double> {

        @Serial
        private static final long serialVersionUID = 1L;

        /** Temporal decay constant λ (per second). */
        private final double lambda;

        /** Source-reliability weight table (immutable after construction). */
        private static final Map<String, Double> SOURCE_WEIGHTS = Map.of(
                "SERP_NEWS",  1.0,
                "BLOOMBERG",  1.0,
                "X_TWITTER",  0.8,
                "REDDIT",     0.6,
                "TELEGRAM",   0.5
        );

        /** Default weight for unknown sources. */
        private static final double DEFAULT_SOURCE_WEIGHT = 0.5;

        /**
         * Creates an aggregator with the given decay constant.
         *
         * @param lambda temporal decay rate (per second); must be &gt; 0
         * @throws IllegalArgumentException if lambda ≤ 0
         */
        public SentimentDecayAggregator(double lambda) {
            if (lambda <= 0) {
                throw new IllegalArgumentException("Lambda must be positive, got " + lambda);
            }
            this.lambda = lambda;
        }

        /**
         * Creates a fresh, empty accumulator for a new window pane.
         *
         * @return a zeroed {@link SentimentAccumulator}
         */
        @Override
        public SentimentAccumulator createAccumulator() {
            return new SentimentAccumulator();
        }

        /**
         * Incorporates a single {@link SentimentEvent} into the
         * running accumulator using the exponential-decay formula.
         *
         * <p>First observation initialises the accumulator directly;
         * subsequent observations apply the decay factor based on the
         * elapsed time since the previous event.</p>
         *
         * @param event       the incoming sentiment event
         * @param accumulator the current accumulator state
         * @return the updated accumulator (same reference, mutated in place)
         */
        @Override
        public SentimentAccumulator add(SentimentEvent event,
                                        SentimentAccumulator accumulator) {

            double score       = event.getSentimentScore();
            double confidence  = event.getConfidence();
            double sourceOmega = resolveSourceWeight(event.getSource());
            long   eventTime   = event.getTimestamp();

            // Effective weight: source reliability × classifier confidence
            double wEff = sourceOmega * confidence;

            if (accumulator.lastTimestamp == 0L) {
                // First event in this pane — initialise without decay
                accumulator.weightedSum = score * wEff;
                accumulator.weightCount = wEff;
            } else {
                // Compute elapsed seconds since last event
                long deltaMs = Math.max(0L, eventTime - accumulator.lastTimestamp);
                double deltaSec = deltaMs / 1000.0;

                // Temporal decay factor: α = e^(−λ · Δt)
                double alpha = Math.exp(-lambda * deltaSec);

                // Exponentially-weighted update
                accumulator.weightedSum =
                        alpha * accumulator.weightedSum
                      + (1.0 - alpha) * score * wEff;

                accumulator.weightCount =
                        alpha * accumulator.weightCount
                      + (1.0 - alpha) * wEff;
            }

            accumulator.lastTimestamp = eventTime;
            accumulator.eventCount++;
            return accumulator;
        }

        /**
         * Extracts the final aggregated sentiment score from the
         * accumulator.  This is the ratio of the decayed weighted sum to
         * the decayed weight count, i.e. the weighted average.
         *
         * @param accumulator the window accumulator
         * @return the aggregated sentiment score in [−1.0, +1.0],
         *         or 0.0 if no events were observed
         */
        @Override
        public Double getResult(SentimentAccumulator accumulator) {
            if (accumulator.weightCount < 1e-12) {
                return 0.0;
            }
            return accumulator.weightedSum / accumulator.weightCount;
        }

        /**
         * Merges two accumulators by decaying both to the <em>later</em>
         * timestamp and summing.
         *
         * <p>This preserves the temporal-decay invariant: if accumulator A
         * last saw an event at {@code t_A} and B at {@code t_B > t_A},
         * then A's contributions are further decayed by
         * {@code e^(−λ · (t_B − t_A))} before addition.</p>
         *
         * @param a first accumulator
         * @param b second accumulator
         * @return a new merged accumulator
         */
        @Override
        public SentimentAccumulator merge(SentimentAccumulator a,
                                          SentimentAccumulator b) {
            SentimentAccumulator merged = new SentimentAccumulator();
            long maxTime = Math.max(a.lastTimestamp, b.lastTimestamp);

            // Decay each accumulator to the common reference time
            double deltaA = Math.max(0, (maxTime - a.lastTimestamp) / 1000.0);
            double deltaB = Math.max(0, (maxTime - b.lastTimestamp) / 1000.0);
            double alphaA = Math.exp(-lambda * deltaA);
            double alphaB = Math.exp(-lambda * deltaB);

            merged.weightedSum =
                    alphaA * a.weightedSum + alphaB * b.weightedSum;
            merged.weightCount =
                    alphaA * a.weightCount + alphaB * b.weightCount;
            merged.lastTimestamp = maxTime;
            merged.eventCount = a.eventCount + b.eventCount;

            return merged;
        }

        /**
         * Resolves the source-reliability weight for the given provenance
         * tag.  Unknown sources receive the default penalty weight.
         *
         * @param source the provenance tag (case-insensitive)
         * @return the reliability weight ω ∈ (0, 1]
         */
        private static double resolveSourceWeight(String source) {
            if (source == null) {
                return DEFAULT_SOURCE_WEIGHT;
            }
            return SOURCE_WEIGHTS.getOrDefault(
                    source.toUpperCase(), DEFAULT_SOURCE_WEIGHT);
        }
    }

    // ──────────────────────────────────────────────────────────────────────
    //  CoProcessFunction: Jensen-Shannon Divergence Engine
    // ──────────────────────────────────────────────────────────────────────

    /**
     * Stateful co-processing function that joins live {@link MarketTick}
     * events with the latest aggregated sentiment score and computes the
     * <em>Jensen-Shannon Divergence</em> between the swarm probability
     * and the market-implied probability.
     *
     * <h3>State Management</h3>
     * <p>A single {@link ValueState} holds the most-recently-emitted
     * sentiment average from the upstream sliding window.  Each incoming
     * market tick reads this state, computes D<sub>JS</sub>, and — if the
     * divergence exceeds the threshold — emits an {@link ArbitrageAlert}.
     * </p>
     *
     * <h3>Probability Mapping</h3>
     * <ul>
     *   <li><strong>Swarm probability P</strong>: The aggregated sentiment
     *       score (∈ [−1, +1]) is mapped to a probability via a scaled
     *       sigmoid: {@code P = 1 / (1 + e^(−κ·S))}, where κ = 3.0 controls
     *       the steepness.  This maps neutral sentiment (0.0) to P = 0.5
     *       and saturates near 0.0 / 1.0 for extreme scores.</li>
     *   <li><strong>Market probability Q</strong>: For prediction markets
     *       the tick price <em>is</em> the probability (0.0 … 1.0).
     *       Clamped to [0.001, 0.999] to avoid log(0) in the KL
     *       computation.</li>
     * </ul>
     *
     * <h3>Jensen-Shannon Divergence (Binary Case)</h3>
     * <p>For two Bernoulli distributions P = (p, 1−p) and Q = (q, 1−q):</p>
     * <pre>
     *   M_yes = 0.5·(p + q)
     *   M_no  = 0.5·((1−p) + (1−q))
     *
     *   D_KL(P ‖ M) = p·ln(p / M_yes) + (1−p)·ln((1−p) / M_no)
     *   D_KL(Q ‖ M) = q·ln(q / M_yes) + (1−q)·ln((1−q) / M_no)
     *
     *   D_JS(P ‖ Q) = 0.5·D_KL(P ‖ M) + 0.5·D_KL(Q ‖ M)
     * </pre>
     * <p>D<sub>JS</sub> ∈ [0, ln 2].  Values above the threshold θ
     * (default 0.05) indicate a statistically meaningful disagreement.</p>
     */
    public static class DivergenceEngine
            extends KeyedCoProcessFunction<String, MarketTick, Double, ArbitrageAlert> {

        @Serial
        private static final long serialVersionUID = 1L;

        /**
         * Small additive constant to prevent {@code log(0)} in the
         * KL-divergence computation.  Chosen to be orders of magnitude
         * below the minimum clamped probability (0.001) so it does not
         * materially affect the result.
         */
        private static final double EPSILON = 1e-10;

        /**
         * Sigmoid steepness parameter κ.  Controls how aggressively the
         * sentiment score (−1 … +1) is mapped to a probability (0 … 1).
         * A value of 3.0 maps ±0.5 → ~0.82 / 0.18, providing reasonable
         * dynamic range without excessive saturation.
         */
        private static final double SIGMOID_KAPPA = 3.0;

        /** Jensen-Shannon divergence threshold for alert emission. */
        private final double jsThreshold;

        /** Keyed state: latest sentiment aggregation result. */
        private transient ValueState<Double> latestSentimentScore;

        /** Keyed state: count of alerts emitted (for monitoring). */
        private transient ValueState<Long> alertCount;

        /**
         * Constructs the engine with the given divergence threshold.
         *
         * @param jsThreshold the D_JS threshold; alerts fire when
         *                    D_JS &gt; this value
         */
        public DivergenceEngine(double jsThreshold) {
            this.jsThreshold = jsThreshold;
        }

        /**
         * Initialises keyed state descriptors.  Called once per key
         * when the operator is first invoked for that key.
         *
         * @param parameters runtime configuration (unused)
         */
        @Override
        public void open(Configuration parameters) {
            latestSentimentScore = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("latest-sentiment", Double.class, 0.0));
            alertCount = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("alert-count", Long.class, 0L));
        }

        /**
         * Processes a {@link MarketTick} from the order-book stream.
         *
         * <ol>
         *   <li>Reads the latest sentiment score from keyed state.</li>
         *   <li>Converts sentiment → swarm probability via sigmoid.</li>
         *   <li>Extracts market probability from the tick price.</li>
         *   <li>Computes D<sub>JS</sub>(P ‖ Q).</li>
         *   <li>If D<sub>JS</sub> &gt; θ, emits an {@link ArbitrageAlert}.</li>
         * </ol>
         *
         * @param tick the incoming market tick
         * @param ctx  the process function context
         * @param out  the output collector
         * @throws Exception on state access failure
         */
        @Override
        public void processElement1(MarketTick tick,
                                    Context ctx,
                                    Collector<ArbitrageAlert> out)
                throws Exception {

            // ── 1. Read latest sentiment ────────────────────────────────
            Double sentimentVal = latestSentimentScore.value();
            double rawSentiment = (sentimentVal != null) ? sentimentVal : 0.0;

            // ── 2. Sentiment → swarm probability via scaled sigmoid ─────
            double swarmProb = sigmoid(rawSentiment, SIGMOID_KAPPA);

            // ── 3. Market probability from tick price ───────────────────
            // Prediction-market prices are probabilities in [0, 1].
            // Clamp to [0.001, 0.999] to keep KL-divergence finite.
            double marketProb = clamp(tick.getPrice(), 0.001, 0.999);

            // ── 4. Compute Jensen-Shannon Divergence ────────────────────
            double dJS = jensenShannonDivergence(swarmProb, marketProb);

            // ── 5. Emit alert if divergence exceeds threshold ───────────
            if (dJS > jsThreshold) {
                double edge = swarmProb - marketProb;

                ArbitrageAlert alert = new ArbitrageAlert(
                        tick.getSymbol(),
                        swarmProb,
                        marketProb,
                        dJS,
                        edge,
                        tick.getTimestamp());

                out.collect(alert);

                // Update alert counter for monitoring
                Long count = alertCount.value();
                alertCount.update((count != null ? count : 0L) + 1L);

                if (LOG.isInfoEnabled()) {
                    LOG.info("Alert #{}: {}", alertCount.value(), alert);
                }
            }
        }

        /**
         * Processes an aggregated sentiment score from the sliding-window
         * output.  Simply updates the keyed state so the next market tick
         * will use the freshest sentiment estimate.
         *
         * @param sentimentAverage the latest window-aggregated sentiment
         * @param ctx              the process function context
         * @param out              the output collector (not used here)
         * @throws Exception on state access failure
         */
        @Override
        public void processElement2(Double sentimentAverage,
                                    Context ctx,
                                    Collector<ArbitrageAlert> out)
                throws Exception {

            latestSentimentScore.update(sentimentAverage);

            if (LOG.isDebugEnabled()) {
                LOG.debug("Sentiment state updated: {}", sentimentAverage);
            }
        }

        // ── Private helpers ──────────────────────────────────────────────

        /**
         * Computes the Jensen-Shannon Divergence for two Bernoulli
         * distributions P = (pYes, 1−pYes) and Q = (qYes, 1−qYes).
         *
         * <pre>
         *   M     = 0.5·(P + Q)
         *   D_JS  = 0.5·D_KL(P ‖ M) + 0.5·D_KL(Q ‖ M)
         * </pre>
         *
         * @param pYes the "yes" probability of distribution P
         * @param qYes the "yes" probability of distribution Q
         * @return D_JS ∈ [0, ln 2]
         */
        private static double jensenShannonDivergence(double pYes, double qYes) {
            double pNo = 1.0 - pYes;
            double qNo = 1.0 - qYes;

            // Mixture distribution M = 0.5·(P + Q)
            double mYes = 0.5 * (pYes + qYes);
            double mNo  = 0.5 * (pNo  + qNo);

            // KL(P ‖ M) = Σ p_i · ln(p_i / m_i)
            double klPM = klTerm(pYes, mYes) + klTerm(pNo, mNo);

            // KL(Q ‖ M) = Σ q_i · ln(q_i / m_i)
            double klQM = klTerm(qYes, mYes) + klTerm(qNo, mNo);

            return 0.5 * klPM + 0.5 * klQM;
        }

        /**
         * Computes a single term of the KL-divergence sum:
         * {@code p · ln(p / q)}, with epsilon smoothing to handle p ≈ 0.
         *
         * @param p the distribution probability
         * @param q the reference probability
         * @return p · ln(p / q), or 0 if p ≈ 0
         */
        private static double klTerm(double p, double q) {
            if (p < EPSILON) {
                return 0.0;  // 0 · ln(0/q) = 0 by convention
            }
            return p * Math.log((p + EPSILON) / (q + EPSILON));
        }

        /**
         * Scaled sigmoid: {@code 1 / (1 + e^(−κ·x))}.
         *
         * @param x     input value (sentiment score)
         * @param kappa steepness parameter
         * @return probability in (0, 1)
         */
        private static double sigmoid(double x, double kappa) {
            return 1.0 / (1.0 + Math.exp(-kappa * x));
        }

        /**
         * Clamps a value to the range [min, max].
         *
         * @param value the input value
         * @param min   lower bound
         * @param max   upper bound
         * @return the clamped value
         */
        private static double clamp(double value, double min, double max) {
            return Math.max(min, Math.min(max, value));
        }
    }

    // ──────────────────────────────────────────────────────────────────────
    //  Main Entry Point
    // ──────────────────────────────────────────────────────────────────────

    /**
     * Assembles and launches the Flink streaming job.
     *
     * <h3>Supported CLI Parameters</h3>
     * <ul>
     *   <li>{@code --bootstrap} — Kafka bootstrap servers
     *       (default {@code localhost:9092})</li>
     *   <li>{@code --group} — Kafka consumer group
     *       (default {@code flink-sentiment-processor})</li>
     *   <li>{@code --tick-topic} — Market-tick topic
     *       (default {@code polymarket-orderbook-stream})</li>
     *   <li>{@code --sentiment-topic} — Sentiment topic
     *       (default {@code tradfi-macro-stream})</li>
     *   <li>{@code --parallelism} — Job parallelism (default 4)</li>
     *   <li>{@code --js-threshold} — D_JS alert threshold
     *       (default 0.05)</li>
     *   <li>{@code --lambda} — Sentiment decay constant
     *       (default 0.01)</li>
     * </ul>
     *
     * @param args command-line arguments (parsed by {@link ParameterTool})
     * @throws Exception if the Flink job fails to start or execute
     */
    public static void main(String[] args) throws Exception {

        // ── Parse CLI parameters ─────────────────────────────────────────
        final ParameterTool params = ParameterTool.fromArgs(args);
        final String bootstrap      = params.get("bootstrap",       DEFAULT_BOOTSTRAP);
        final String groupId        = params.get("group",           DEFAULT_GROUP_ID);
        final String tickTopic      = params.get("tick-topic",      DEFAULT_TICK_TOPIC);
        final String sentimentTopic = params.get("sentiment-topic", DEFAULT_SENTIMENT_TOPIC);
        final int    parallelism    = params.getInt("parallelism",   DEFAULT_PARALLELISM);
        final double jsThreshold    = params.getDouble("js-threshold", DEFAULT_JS_THRESHOLD);
        final double lambda         = params.getDouble("lambda",     DEFAULT_LAMBDA);

        LOG.info("╔══════════════════════════════════════════════════════════╗");
        LOG.info("║   Dual-Broker SOTA Engine — Streaming Pipeline v1.0.0  ║");
        LOG.info("╠══════════════════════════════════════════════════════════╣");
        LOG.info("║  Bootstrap:       {}", bootstrap);
        LOG.info("║  Group ID:        {}", groupId);
        LOG.info("║  Tick topic:      {}", tickTopic);
        LOG.info("║  Sentiment topic: {}", sentimentTopic);
        LOG.info("║  Parallelism:     {}", parallelism);
        LOG.info("║  JS threshold:    {}", jsThreshold);
        LOG.info("║  Decay λ:         {}", lambda);
        LOG.info("╚══════════════════════════════════════════════════════════╝");

        // ── 1. Configure Flink execution environment ─────────────────────
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);

        // Make CLI parameters available to all operators via global config
        env.getConfig().setGlobalJobParameters(params);

        // ── 2. Apply RocksDB state-backend configuration ────────────────
        StateBackendConfig.configure(env);

        // ── 3. Build Kafka sources ──────────────────────────────────────
        KafkaSource<MarketTick> tickSource =
                MarketDataConsumer.createMarketTickSource(
                        bootstrap, groupId, tickTopic);

        KafkaSource<SentimentEvent> sentimentSource =
                MarketDataConsumer.createSentimentSource(
                        bootstrap, groupId, sentimentTopic);

        // ── 4. Attach sources with watermark strategies ─────────────────
        DataStream<MarketTick> tickStream = env.fromSource(
                tickSource,
                MarketDataConsumer.getWatermarkStrategy(MarketTick::getTimestamp),
                "Kafka-Source-MarketTicks")
                .uid("source-market-ticks");

        DataStream<SentimentEvent> sentimentStream = env.fromSource(
                sentimentSource,
                MarketDataConsumer.getWatermarkStrategy(SentimentEvent::getTimestamp),
                "Kafka-Source-SentimentEvents")
                .uid("source-sentiment-events");

        // ── 5. Sliding-window sentiment aggregation ─────────────────────
        //
        //  Window:   1 hour (3600 s)
        //  Slide:    1 minute (60 s)  → window fires every minute
        //  Lateness: 1 minute        → late events re-trigger the window
        //  Side out: beyond-lateness events captured for audit
        //
        //  The key "GLOBAL_SENTIMENT" means ALL sentiment events are
        //  routed to the same operator instance.  This is intentional:
        //  we want a single, unified "swarm probability" that fuses
        //  information from all sources.  With parallelism = 4 and a
        //  single key, only one slot performs the aggregation; the others
        //  remain idle.  This is acceptable because the sentiment stream
        //  is low-volume (hundreds of events/min) compared to market ticks.
        //
        SingleOutputStreamOperator<Double> sentimentAverages = sentimentStream
                .keyBy(ev -> "GLOBAL_SENTIMENT")
                .window(SlidingEventTimeWindows.of(
                        Time.hours(1),    // window size
                        Time.minutes(1))) // slide interval
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(LATE_SENTIMENT_TAG)
                .aggregate(new SentimentDecayAggregator(lambda))
                .name("Sentiment-EMA-SlidingWindow")
                .uid("sentiment-ema-window");

        // ── 6. Connect tick stream with sentiment averages ──────────────
        //
        //  Both streams are keyed by a constant ("GLOBAL") so they are
        //  routed to the same operator instance where the
        //  DivergenceEngine can join the latest sentiment state with
        //  each incoming tick.
        //
        //  Note: for multi-symbol support, key the tick stream by symbol
        //  and broadcast the sentiment average.  The current design uses
        //  a constant key because the engine targets a single prediction-
        //  market contract at a time.
        //
        SingleOutputStreamOperator<ArbitrageAlert> alerts = tickStream
                .keyBy(tick -> "GLOBAL")
                .connect(sentimentAverages.keyBy(avg -> "GLOBAL"))
                .process(new DivergenceEngine(jsThreshold))
                .name("JS-Divergence-Engine")
                .uid("divergence-engine");

        // ── 7. Late-event side outputs ──────────────────────────────────
        DataStream<SentimentEvent> lateSentiment =
                sentimentAverages.getSideOutput(LATE_SENTIMENT_TAG);
        lateSentiment
                .map(ev -> {
                    LOG.warn("Late sentiment event dropped from window: {}", ev);
                    return ev.toString();
                })
                .returns(Types.STRING)
                .name("Late-Sentiment-Logger")
                .uid("late-sentiment-logger");

        // ── 8. Sinks ────────────────────────────────────────────────────
        //  In production, replace print() with a KafkaSink writing to an
        //  "arbitrage-alerts" topic consumed by the order-execution layer.
        alerts.print().name("Console-Alert-Sink").uid("alert-sink-console");

        // ── 9. Launch ───────────────────────────────────────────────────
        LOG.info("Submitting Flink job graph...");
        env.execute("Dual-Broker-SOTA-Streaming-Pipeline");
    }
}
