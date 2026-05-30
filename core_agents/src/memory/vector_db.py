"""
Sentiment Vector Database — Qdrant-backed embedding store with temporal decay.

This module provides the ``SentimentVectorDB`` class, the long-term memory
layer for the dual-broker SOTA engine.  It persists MiniLM-L6-v2 embeddings
(384-d) alongside rich metadata payloads and exposes a temporal sentiment
tensor that implements an exponentially-weighted moving average (EWMA) over
historical sentiment vectors.

Mathematical foundation — Temporal Sentiment Tensor
====================================================

Given a time-ordered sequence of sentiment embeddings
{s₁, s₂, …, sₙ} with corresponding timestamps {t₁, t₂, …, tₙ}, the
temporal sentiment tensor at evaluation time *t* is defined as:

    S_t  =  Σᵢ  wᵢ · sᵢ  /  Σᵢ wᵢ

where the exponential-decay weight for each observation is:

    wᵢ  =  exp(−λ · Δtᵢ)          Δtᵢ = t − tᵢ   (seconds)

*  λ > 0 is the decay constant (``decay_lambda``).  Larger λ → more
   aggressive forgetting of stale signals.
*  Δtᵢ is the age of observation *i* relative to the evaluation
   timestamp *t*.

The resulting S_t is a 384-dimensional vector in the same embedding space
as the individual observations, representing the *recency-weighted centroid*
of all stored sentiment signals.

Qdrant connectivity
===================

The class tries ``localhost:6333`` (gRPC) first.  If the server is
unreachable it falls back transparently to Qdrant's in-memory mode so that
unit tests and CI pipelines can run without a running Qdrant instance.

Dependencies:  ``qdrant-client>=1.9``, ``numpy>=1.26``
"""

from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

import numpy as np
from numpy.typing import NDArray

try:
    from qdrant_client import QdrantClient
    from qdrant_client.http.models import (
        Distance,
        PointStruct,
        VectorParams,
        Filter,
        ScrollRequest,
    )

    _QDRANT_AVAILABLE = True
except ImportError:  # pragma: no cover — optional dependency
    _QDRANT_AVAILABLE = False

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DEFAULT_HOST: str = "localhost"
_DEFAULT_PORT: int = 6333
_COLLECTION_NAME: str = "sentiment_embeddings"
_VECTOR_DIM: int = 384  # MiniLM-L6-v2 compatible


# ---------------------------------------------------------------------------
# Data transfer objects
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SimilarityResult:
    """Single result row from a cosine-similarity search."""

    doc_id: str
    score: float
    payload: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Main class
# ---------------------------------------------------------------------------


class SentimentVectorDB:
    """Qdrant-backed vector store for sentiment embeddings.

    Parameters
    ----------
    host : str
        Qdrant gRPC host.  Defaults to ``localhost``.
    port : int
        Qdrant gRPC port.  Defaults to ``6333``.
    collection_name : str
        Name of the Qdrant collection.  Defaults to ``sentiment_embeddings``.
    vector_dim : int
        Dimensionality of stored vectors.  Defaults to ``384``.
    force_in_memory : bool
        If *True*, skip the remote connection attempt and use in-memory mode
        unconditionally.  Useful for tests.
    """

    def __init__(
        self,
        host: str = _DEFAULT_HOST,
        port: int = _DEFAULT_PORT,
        collection_name: str = _COLLECTION_NAME,
        vector_dim: int = _VECTOR_DIM,
        force_in_memory: bool = False,
    ) -> None:
        self._collection = collection_name
        self._dim = vector_dim
        self._in_memory = force_in_memory

        if not _QDRANT_AVAILABLE:
            raise ImportError(
                "qdrant-client is required.  Install with: "
                "pip install 'qdrant-client>=1.9'"
            )

        self._client = self._connect(host, port, force_in_memory)
        self._ensure_collection()

    # ------------------------------------------------------------------
    # Connection helpers
    # ------------------------------------------------------------------

    def _connect(
        self, host: str, port: int, force_in_memory: bool
    ) -> "QdrantClient":
        """Attempt a remote connection; fall back to in-memory mode."""
        if force_in_memory:
            logger.info("Qdrant: using in-memory mode (forced).")
            return QdrantClient(location=":memory:")

        try:
            client = QdrantClient(host=host, port=port, timeout=5)
            # Lightweight health check — list existing collections.
            client.get_collections()
            logger.info("Qdrant: connected to %s:%d.", host, port)
            return client
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Qdrant: cannot reach %s:%d (%s). Falling back to in-memory.",
                host,
                port,
                exc,
            )
            self._in_memory = True
            return QdrantClient(location=":memory:")

    def _ensure_collection(self) -> None:
        """Create the collection if it does not already exist."""
        existing = {
            c.name for c in self._client.get_collections().collections
        }
        if self._collection not in existing:
            self._client.create_collection(
                collection_name=self._collection,
                vectors_config=VectorParams(
                    size=self._dim, distance=Distance.COSINE
                ),
            )
            logger.info(
                "Qdrant: created collection '%s' (dim=%d, cosine).",
                self._collection,
                self._dim,
            )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def store_embedding(
        self,
        doc_id: str,
        embedding: NDArray[np.float32] | list[float],
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Upsert a single embedding with its metadata payload.

        Parameters
        ----------
        doc_id : str
            Unique document / signal identifier.  Must be a valid UUID string
            or any string convertible to a deterministic point id.
        embedding : array-like of float32, shape ``(384,)``
            The dense vector to store.
        metadata : dict, optional
            Arbitrary JSON-serialisable payload attached to the point.
            Typical keys: ``source``, ``timestamp_utc``, ``sentiment_score``,
            ``ticker``, ``market_id``.
        """
        vec = (
            embedding.tolist()
            if isinstance(embedding, np.ndarray)
            else list(embedding)
        )
        if len(vec) != self._dim:
            raise ValueError(
                f"Embedding dimension mismatch: expected {self._dim}, "
                f"got {len(vec)}"
            )

        payload = dict(metadata) if metadata else {}
        # Ensure a storage timestamp exists for temporal queries.
        payload.setdefault("timestamp_utc", time.time())

        # Qdrant requires either int or UUID point ids.
        try:
            point_id = str(uuid.UUID(doc_id))
        except ValueError:
            # Deterministic UUID-5 from the doc_id string.
            point_id = str(
                uuid.uuid5(uuid.NAMESPACE_URL, doc_id)
            )

        self._client.upsert(
            collection_name=self._collection,
            points=[
                PointStruct(
                    id=point_id,
                    vector=vec,
                    payload=payload,
                )
            ],
        )
        logger.debug("Stored embedding %s (%d-d).", point_id, self._dim)

    def query_similar(
        self,
        embedding: NDArray[np.float32] | list[float],
        top_k: int = 5,
    ) -> list[SimilarityResult]:
        """Return the *top_k* most cosine-similar points.

        Parameters
        ----------
        embedding : array-like of float32, shape ``(384,)``
            Query vector.
        top_k : int
            Maximum number of neighbours to return.

        Returns
        -------
        list[SimilarityResult]
            Nearest neighbours sorted by descending cosine similarity.
        """
        vec = (
            embedding.tolist()
            if isinstance(embedding, np.ndarray)
            else list(embedding)
        )
        hits = self._client.search(
            collection_name=self._collection,
            query_vector=vec,
            limit=top_k,
        )
        return [
            SimilarityResult(
                doc_id=str(hit.id),
                score=hit.score,
                payload=hit.payload or {},
            )
            for hit in hits
        ]

    def compute_temporal_sentiment_tensor(
        self,
        decay_lambda: float = 1e-4,
        delta_t: float | None = None,
    ) -> NDArray[np.float64]:
        """Compute the exponentially-weighted sentiment centroid **S_t**.

        Mathematical formulation
        ------------------------

        ::

            S_t  =  Σᵢ  wᵢ · sᵢ  /  Σᵢ wᵢ

            wᵢ   =  exp(−λ · (t − tᵢ))

        where *t* is the evaluation timestamp (defaults to ``time.time()``),
        *tᵢ* is the timestamp stored in each point's ``timestamp_utc`` payload
        field, and *λ* (``decay_lambda``) controls how aggressively older
        observations are down-weighted.

        Parameters
        ----------
        decay_lambda : float
            Decay constant λ.  Larger values → faster forgetting.
            A value of 1e-4 gives a half-life of ≈1.93 hours.
        delta_t : float, optional
            If provided, used as the reference "now" timestamp instead of
            ``time.time()``.  Useful for reproducible back-tests.

        Returns
        -------
        NDArray[np.float64]
            The 384-dimensional recency-weighted centroid vector.
            Returns the zero vector if the collection is empty.

        Notes
        -----
        Half-life relationship:  t½ = ln(2) / λ ≈ 0.6931 / λ.
        """
        now = delta_t if delta_t is not None else time.time()

        # Scroll through ALL points in the collection.  For production
        # scale (>100 k points) this should be replaced with a streaming
        # scroll + on-the-fly accumulation to avoid OOM.
        all_points: list[Any] = []
        offset = None
        while True:
            result = self._client.scroll(
                collection_name=self._collection,
                limit=256,
                offset=offset,
                with_vectors=True,
                with_payload=True,
            )
            points, next_offset = result
            all_points.extend(points)
            if next_offset is None:
                break
            offset = next_offset

        if not all_points:
            logger.warning(
                "Temporal sentiment tensor: collection is empty — "
                "returning zero vector."
            )
            return np.zeros(self._dim, dtype=np.float64)

        # Build matrices — N × D embeddings and N × 1 weights.
        n = len(all_points)
        embeddings = np.empty((n, self._dim), dtype=np.float64)
        weights = np.empty(n, dtype=np.float64)

        for idx, pt in enumerate(all_points):
            vec = pt.vector
            if isinstance(vec, dict):
                # Named vectors: take the first (and only) one.
                vec = next(iter(vec.values()))
            embeddings[idx] = vec
            ts = (pt.payload or {}).get("timestamp_utc", now)
            age = max(now - float(ts), 0.0)
            weights[idx] = np.exp(-decay_lambda * age)

        # Weighted centroid: S_t = Σ wᵢ sᵢ / Σ wᵢ
        weight_sum = weights.sum()
        if weight_sum < 1e-30:
            logger.warning(
                "Temporal sentiment tensor: all weights decayed to zero."
            )
            return np.zeros(self._dim, dtype=np.float64)

        # (N,) @ (N, D) → (D,)  via broadcasting: (N,1) * (N,D) then sum
        s_t: NDArray[np.float64] = (
            (weights[:, np.newaxis] * embeddings).sum(axis=0) / weight_sum
        )
        return s_t

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    def count(self) -> int:
        """Return the number of points in the collection."""
        info = self._client.get_collection(self._collection)
        return info.points_count or 0

    @property
    def is_in_memory(self) -> bool:
        """Whether the client is using in-memory mode."""
        return self._in_memory

    def __repr__(self) -> str:
        mode = "in-memory" if self._in_memory else "remote"
        return (
            f"<SentimentVectorDB collection={self._collection!r} "
            f"dim={self._dim} mode={mode}>"
        )
