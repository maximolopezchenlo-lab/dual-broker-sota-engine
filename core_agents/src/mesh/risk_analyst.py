"""
Risk Analyst Agent — Bayesian consensus, information-theoretic divergence,
position sizing, and CEX-implied probability computations.

This is the **quantitative core** of the dual-broker SOTA engine.  Every
public method carries a complete mathematical derivation in its docstring
and a fully working NumPy implementation — no stubs.

Key algorithms
==============

1. **Bayesian Swarm Consensus** (``compute_bayesian_consensus``)
   Weighted average across 50 LLM personas where weights are a function
   of cumulative Brier score accuracy and confidence gamma.

2. **Log-Odds Posterior Fusion** (``compute_posterior``)
   Blends the swarm consensus probability with the on-chain market price
   in log-odds space using a mixing coefficient alpha.

3. **KL & Jensen-Shannon Divergence** (``kl_divergence``,
   ``jensen_shannon_divergence``)
   Measures information-theoretic distance between the model's posterior
   and the market's implied distribution.

4. **Quarter-Kelly Position Sizing** (``quarter_kelly_sizing``)
   Conservative fractional-Kelly bet sizing for prediction-market
   positions.

5. **CEX Implied Probability** (``cex_implied_probability``)
   Extracts the risk-neutral probability from a log-normal (Black-Scholes-
   Merton) asset price model via Phi(d2).

6. **Arbitrage Detection** (``detect_arbitrage_opportunity``)
   Compares CEX- and DEX-derived probabilities, computes edge and Kelly
   size, and returns a structured arbitrage signal.

Dependencies:  ``numpy>=1.26``, ``scipy>=1.12``
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from typing import Any

import numpy as np
from numpy.typing import NDArray
from scipy import stats as sp_stats

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Shared dataclass used by the Swarm Orchestrator
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class SwarmPersona:
    """Configuration for a single LLM persona within the 50-agent swarm.

    Attributes
    ----------
    id : str
        Unique identifier (UUID7 string).
    temperature : float
        Sampling temperature used by this persona.  Ranges from 0.1
        (deterministic/conservative) to 1.5 (creative/contrarian).
    prior_bias : float
        Pre-set directional bias in [-1, 1].  Negative = bearish
        prior, positive = bullish prior.
    brier_score_cumulative : float
        Running Brier score (lower is better).  Updated after every
        resolved market.  Initialised at 0.25 (the expected Brier score
        of a perfectly-calibrated 50/50 estimator).
    confidence_gamma : float
        Self-reported confidence exponent gamma >= 1.  Higher gamma
        penalises low-confidence estimates more aggressively in the
        consensus weighting.
    """

    id: str
    temperature: float = 0.7
    prior_bias: float = 0.0
    brier_score_cumulative: float = 0.25
    confidence_gamma: float = 1.0


# ---------------------------------------------------------------------------
# Risk Analyst
# ---------------------------------------------------------------------------


class RiskAnalyst:
    """Quantitative risk engine for the dual-broker SOTA pipeline.

    All methods are stateless pure functions (no side-effects beyond
    logging).  They operate on NumPy arrays for vectorised performance
    and use SciPy for the normal CDF in the CEX pricing model.

    Usage
    -----
    >>> analyst = RiskAnalyst()
    >>> p_swarm = analyst.compute_bayesian_consensus(personas, estimates)
    >>> p_post  = analyst.compute_posterior(p_swarm, p_market=0.55, alpha=0.7)
    >>> size    = analyst.quarter_kelly_sizing(p_post, c_yes=0.52, bankroll=10_000)
    """

    # Numerical epsilon to prevent log(0) and division-by-zero.
    _EPS: float = 1e-12

    # ------------------------------------------------------------------
    # 1.  Bayesian Swarm Consensus
    # ------------------------------------------------------------------

    def compute_bayesian_consensus(
        self,
        personas: list[SwarmPersona],
        estimates: list[tuple[float, float]],
    ) -> float:
        """Compute the Bayesian swarm consensus probability.

        Mathematical formulation
        ------------------------

        Each persona *i* supplies:
        *  p_i in (0, 1) — probability estimate for the target outcome.
        *  c_i in (0, 1] — self-reported confidence.

        Its track-record is summarised by the cumulative Brier score B_i
        (lower = better).  The consensus weight for persona *i* is:

            w_i  =  c_i^gamma  *  (1 - B_i)^2

        where gamma = ``persona.confidence_gamma`` >= 1.

        *  The (1 - B_i)^2 term rewards historically accurate personas
           quadratically.
        *  The c_i^gamma term penalises low confidence exponentially;
           when gamma = 1 it is linear, gamma = 2 is quadratic, etc.

        The swarm consensus is the weighted mean:

            P_swarm  =  sum(w_i * p_i) / sum(w_i)

        Parameters
        ----------
        personas : list[SwarmPersona]
            The 50 (or N) LLM personas.
        estimates : list[tuple[float, float]]
            Parallel list of ``(probability, confidence)`` tuples, one per
            persona.  Must have the same length as *personas*.

        Returns
        -------
        float
            P_swarm in (0, 1).

        Raises
        ------
        ValueError
            If *personas* and *estimates* have different lengths or are
            empty.
        """
        if len(personas) != len(estimates):
            raise ValueError(
                f"Length mismatch: {len(personas)} personas vs "
                f"{len(estimates)} estimates."
            )
        if not personas:
            raise ValueError("At least one persona is required.")

        n = len(personas)
        probs = np.empty(n, dtype=np.float64)
        weights = np.empty(n, dtype=np.float64)

        for i, (persona, (p_i, c_i)) in enumerate(
            zip(personas, estimates, strict=True)
        ):
            # Clamp inputs to valid ranges.
            p_i = float(np.clip(p_i, self._EPS, 1.0 - self._EPS))
            c_i = float(np.clip(c_i, self._EPS, 1.0))
            gamma = max(persona.confidence_gamma, 1.0)
            b_i = float(np.clip(persona.brier_score_cumulative, 0.0, 1.0))

            probs[i] = p_i
            # w_i = c_i^gamma * (1 - B_i)^2
            weights[i] = (c_i ** gamma) * ((1.0 - b_i) ** 2)

        weight_sum = weights.sum()
        if weight_sum < self._EPS:
            logger.warning(
                "All consensus weights are ~0; returning uniform 0.5."
            )
            return 0.5

        p_swarm = float((weights * probs).sum() / weight_sum)
        # Clamp to (0, 1) open interval for downstream log-odds.
        return float(np.clip(p_swarm, self._EPS, 1.0 - self._EPS))

    # ------------------------------------------------------------------
    # 2.  Log-Odds Posterior Fusion
    # ------------------------------------------------------------------

    def compute_posterior(
        self,
        p_swarm: float,
        p_market: float,
        alpha: float = 0.7,
    ) -> float:
        """Fuse swarm consensus with market-implied probability in log-odds.

        Mathematical formulation
        ------------------------

        Denoting the *logit* (log-odds) function as:

            logit(p) = ln(p / (1 - p))

        the fused log-odds are:

            logit(P_posterior) = alpha * logit(P_swarm)
                               + (1 - alpha) * logit(P_market)

        Inverting back to probability:

            P_posterior = sigmoid(alpha * logit(P_swarm)
                                + (1 - alpha) * logit(P_market))

        where sigmoid(x) = 1 / (1 + exp(-x)).

        The mixing coefficient alpha controls how much to trust the swarm
        model vs the market.  alpha = 1 -> pure model, alpha = 0 -> pure
        market.

        Parameters
        ----------
        p_swarm : float
            Swarm consensus probability in (0, 1).
        p_market : float
            Market-implied probability in (0, 1).
        alpha : float
            Mixing coefficient in [0, 1].  Default 0.7 (70% model).

        Returns
        -------
        float
            P_posterior in (0, 1).
        """
        # Clamp to avoid log(0).
        p_s = float(np.clip(p_swarm, self._EPS, 1.0 - self._EPS))
        p_m = float(np.clip(p_market, self._EPS, 1.0 - self._EPS))
        alpha = float(np.clip(alpha, 0.0, 1.0))

        logit_s = math.log(p_s / (1.0 - p_s))
        logit_m = math.log(p_m / (1.0 - p_m))

        logit_fused = alpha * logit_s + (1.0 - alpha) * logit_m
        p_post = 1.0 / (1.0 + math.exp(-logit_fused))

        return float(np.clip(p_post, self._EPS, 1.0 - self._EPS))

    # ------------------------------------------------------------------
    # 3.  KL Divergence
    # ------------------------------------------------------------------

    def kl_divergence(
        self,
        p: NDArray[np.float64] | list[float],
        q: NDArray[np.float64] | list[float],
    ) -> float:
        """Compute the Kullback-Leibler divergence D_KL(P || Q).

        Mathematical formulation
        ------------------------

        For discrete distributions P and Q over the same support:

            D_KL(P || Q)  =  sum_x  P(x) * ln( P(x) / Q(x) )

        Properties:
        *  D_KL >= 0  (Gibbs' inequality).
        *  D_KL = 0  iff  P = Q  almost everywhere.
        *  **Asymmetric**: D_KL(P||Q) != D_KL(Q||P) in general.

        Epsilon smoothing is applied to avoid log(0): both P and Q are
        clipped to [eps, 1] and re-normalised.

        Parameters
        ----------
        p, q : array-like of float, shape (K,)
            Discrete probability distributions.  Must have the same
            length and sum to approx 1.

        Returns
        -------
        float
            D_KL(P || Q) in nats (natural log).

        Raises
        ------
        ValueError
            If *p* and *q* have different lengths.
        """
        p_arr = np.asarray(p, dtype=np.float64)
        q_arr = np.asarray(q, dtype=np.float64)

        if p_arr.shape != q_arr.shape:
            raise ValueError(
                f"Shape mismatch: P{p_arr.shape} vs Q{q_arr.shape}."
            )

        # Epsilon-smooth and renormalise.
        p_arr = np.clip(p_arr, self._EPS, None)
        q_arr = np.clip(q_arr, self._EPS, None)
        p_arr = p_arr / p_arr.sum()
        q_arr = q_arr / q_arr.sum()

        # D_KL = sum P(x) ln(P(x)/Q(x))
        kl: float = float(np.sum(p_arr * np.log(p_arr / q_arr)))
        return max(kl, 0.0)  # enforce non-negativity for numerical safety

    # ------------------------------------------------------------------
    # 4.  Jensen-Shannon Divergence
    # ------------------------------------------------------------------

    def jensen_shannon_divergence(
        self,
        p: NDArray[np.float64] | list[float],
        q: NDArray[np.float64] | list[float],
    ) -> float:
        """Compute the Jensen-Shannon divergence D_JS(P || Q).

        Mathematical formulation
        ------------------------

        The JSD is a symmetrised, bounded variant of KL divergence:

            M       =  0.5 * (P + Q)
            D_JS    =  0.5 * D_KL(P || M)  +  0.5 * D_KL(Q || M)

        Properties:
        *  D_JS in [0, ln 2] when using natural logs.
        *  **Symmetric**: D_JS(P||Q) = D_JS(Q||P).
        *  sqrt(D_JS) is a proper metric (Jensen-Shannon distance).

        In the dual-broker engine, D_JS measures how far the model's
        posterior distribution is from the market-implied distribution.
        A large D_JS flags a potential mispricing / arbitrage window.

        Parameters
        ----------
        p, q : array-like of float, shape (K,)
            Discrete probability distributions.

        Returns
        -------
        float
            D_JS(P || Q) in nats.
        """
        p_arr = np.asarray(p, dtype=np.float64)
        q_arr = np.asarray(q, dtype=np.float64)

        # Epsilon-smooth and renormalise before computing M.
        p_arr = np.clip(p_arr, self._EPS, None)
        q_arr = np.clip(q_arr, self._EPS, None)
        p_arr = p_arr / p_arr.sum()
        q_arr = q_arr / q_arr.sum()

        m = 0.5 * (p_arr + q_arr)

        jsd = 0.5 * self.kl_divergence(p_arr, m) + 0.5 * self.kl_divergence(
            q_arr, m
        )
        return max(jsd, 0.0)

    # ------------------------------------------------------------------
    # 5.  Quarter-Kelly Position Sizing
    # ------------------------------------------------------------------

    def quarter_kelly_sizing(
        self,
        p_posterior: float,
        c_yes: float,
        bankroll: float,
    ) -> float:
        """Compute the Quarter-Kelly position size in units of currency.

        Mathematical formulation
        ------------------------

        Full Kelly criterion for a binary bet at cost *c* with model
        probability *p*:

            f*_full  =  (p - c) / (1 - c)

        This is the fraction of bankroll that maximises the expected
        log-growth rate.  However, full Kelly is aggressive and assumes
        perfect probability estimates.  The **quarter-Kelly** variant
        trades growth rate for lower variance:

            f*  =  0.25 * f*_full  =  0.25 * (p - c) / (1 - c)

        The dollar-denominated position size is:

            size  =  f* * bankroll

        Edge guard: if the perceived edge (p - c) is non-positive the
        method returns **0** — no bet should be placed.

        Parameters
        ----------
        p_posterior : float
            Model's posterior probability of the outcome in (0, 1).
        c_yes : float
            Current market cost of a YES share in (0, 1).  On Polymarket
            this equals the market price normalised to [0, 1].
        bankroll : float
            Total available capital (e.g. USDC balance).

        Returns
        -------
        float
            Recommended position size in currency units.
            Returns 0.0 if the perceived edge is <= 0.

        Examples
        --------
        >>> analyst = RiskAnalyst()
        >>> analyst.quarter_kelly_sizing(
        ...     p_posterior=0.65, c_yes=0.52, bankroll=10_000
        ... )
        677.083333
        """
        p = float(np.clip(p_posterior, self._EPS, 1.0 - self._EPS))
        c = float(np.clip(c_yes, self._EPS, 1.0 - self._EPS))

        edge = p - c
        if edge <= 0.0:
            logger.debug(
                "No positive edge (p=%.4f, c=%.4f). Returning 0.", p, c
            )
            return 0.0

        f_star = 0.25 * edge / (1.0 - c)
        size = f_star * bankroll
        logger.debug(
            "Quarter-Kelly: edge=%.4f, f*=%.4f, size=%.2f",
            edge,
            f_star,
            size,
        )
        return round(size, 6)

    # ------------------------------------------------------------------
    # 6.  CEX Implied Probability (Log-Normal / BSM)
    # ------------------------------------------------------------------

    def cex_implied_probability(
        self,
        spot_price: float,
        strike: float,
        vol: float,
        time_to_expiry: float,
        risk_free_rate: float = 0.05,
    ) -> float:
        """Extract risk-neutral probability from a log-normal asset model.

        Mathematical formulation
        ------------------------

        Under the Black-Scholes-Merton assumptions the asset price at
        expiry follows:

            ln(S_T) ~ N( ln(S) + (r - 0.5*sigma^2)*(T-t),  sigma^2*(T-t) )

        The risk-neutral probability that the asset finishes **above**
        the strike K is:

            P(S_T > K) = Phi(d2)

        where Phi is the standard-normal CDF and:

            d2  =  [ ln(S/K) + (r - 0.5*sigma^2)*(T-t) ]  /  [ sigma * sqrt(T-t) ]

        For the dual-broker engine this maps a CEX spot + options-implied
        vol to a probability comparable to a prediction-market YES price.

        Parameters
        ----------
        spot_price : float
            Current spot price S > 0.
        strike : float
            Strike / threshold price K > 0.
        vol : float
            Annualised volatility sigma > 0 (e.g. 0.80 for 80%).
        time_to_expiry : float
            Time to expiry in **years** (T - t > 0).
        risk_free_rate : float
            Annualised risk-free rate r.  Default 5%.

        Returns
        -------
        float
            Phi(d2) in (0, 1) — the risk-neutral probability of finishing
            in-the-money.

        Raises
        ------
        ValueError
            If any input is non-positive where positivity is required.
        """
        if spot_price <= 0:
            raise ValueError(f"spot_price must be > 0, got {spot_price}")
        if strike <= 0:
            raise ValueError(f"strike must be > 0, got {strike}")
        if vol <= 0:
            raise ValueError(f"vol must be > 0, got {vol}")
        if time_to_expiry <= 0:
            raise ValueError(
                f"time_to_expiry must be > 0, got {time_to_expiry}"
            )

        sqrt_t = math.sqrt(time_to_expiry)
        d2 = (
            math.log(spot_price / strike)
            + (risk_free_rate - 0.5 * vol**2) * time_to_expiry
        ) / (vol * sqrt_t)

        # Phi(d2) via scipy.stats.norm.cdf
        prob: float = float(sp_stats.norm.cdf(d2))
        return prob

    # ------------------------------------------------------------------
    # 7.  Arbitrage Detector
    # ------------------------------------------------------------------

    def detect_arbitrage_opportunity(
        self,
        p_cex: float,
        p_dex: float,
        epsilon: float = 0.02,
        bankroll: float = 10_000.0,
    ) -> dict[str, Any]:
        """Detect cross-venue arbitrage between CEX and DEX probabilities.

        Compares the CEX-implied probability (from the log-normal model)
        with the DEX (prediction-market) price.  If the absolute edge
        exceeds *epsilon* (transaction-cost buffer), an arbitrage signal
        is generated.

        Parameters
        ----------
        p_cex : float
            CEX-implied probability in (0, 1).
        p_dex : float
            DEX (prediction-market) YES price in (0, 1).
        epsilon : float
            Minimum edge required to signal an arbitrage (accounts for
            gas, slippage, and fees).  Default 2%.
        bankroll : float
            Capital available for sizing the arbitrage leg.

        Returns
        -------
        dict
            ``{is_arb, edge, direction, kelly_size, jsd}``

            *  ``is_arb`` (bool) — whether the edge exceeds epsilon.
            *  ``edge`` (float) — signed edge (positive = CEX > DEX).
            *  ``direction`` (str) — ``"BUY_DEX"`` or ``"SELL_DEX"``.
            *  ``kelly_size`` (float) — quarter-Kelly position size.
            *  ``jsd`` (float) — Jensen-Shannon divergence between the
               two probability distributions ``[p, 1-p]``.
        """
        p_cex = float(np.clip(p_cex, self._EPS, 1.0 - self._EPS))
        p_dex = float(np.clip(p_dex, self._EPS, 1.0 - self._EPS))

        edge = p_cex - p_dex

        # JSD between the two Bernoulli distributions.
        dist_cex = np.array([p_cex, 1.0 - p_cex])
        dist_dex = np.array([p_dex, 1.0 - p_dex])
        jsd = self.jensen_shannon_divergence(dist_cex, dist_dex)

        is_arb = abs(edge) > epsilon

        if edge > 0:
            # CEX says higher probability than DEX -> buy on DEX.
            direction = "BUY_DEX"
            kelly = self.quarter_kelly_sizing(p_cex, p_dex, bankroll)
        elif edge < 0:
            # DEX says higher probability than CEX -> sell on DEX
            # (buy NO shares, effectively).
            direction = "SELL_DEX"
            kelly = self.quarter_kelly_sizing(
                1.0 - p_cex, 1.0 - p_dex, bankroll
            )
        else:
            direction = "NONE"
            kelly = 0.0

        result = {
            "is_arb": is_arb,
            "edge": round(edge, 6),
            "direction": direction,
            "kelly_size": round(kelly, 6),
            "jsd": round(jsd, 8),
        }
        logger.info("Arbitrage detection: %s", result)
        return result

    # ------------------------------------------------------------------
    # Brier Score Utility
    # ------------------------------------------------------------------

    @staticmethod
    def brier_score(predicted: float, actual: float) -> float:
        """Compute the Brier score for a single binary prediction.

        BS = (predicted - actual)^2

        where *actual* in {0, 1} and *predicted* in [0, 1].

        A perfectly-calibrated 50/50 estimator has E[BS] = 0.25.
        A perfect oracle has BS = 0.  Worst case BS = 1.

        Parameters
        ----------
        predicted : float
            Predicted probability in [0, 1].
        actual : float
            Outcome label, 0 or 1.

        Returns
        -------
        float
            Brier score in [0, 1].
        """
        return (predicted - actual) ** 2

    def __repr__(self) -> str:
        return "<RiskAnalyst>"
