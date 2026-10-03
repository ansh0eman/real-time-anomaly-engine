"""Pure anomaly-detection helpers shared by the worker and unit tests."""

from dataclasses import dataclass

import numpy as np
from sklearn.ensemble import IsolationForest


FEATURE_NAMES = ("request_volume", "error_rate", "latency_ms")


@dataclass(frozen=True)
class Detection:
    is_anomaly: bool
    score: float
    flagged_features: list[str]


def fit_detector(rows: list[list[float]]):
    """Return an Isolation Forest for mature histories or z-score stats otherwise."""
    data = np.asarray(rows, dtype=np.float64)

    if len(data) > 1_000:
        model = IsolationForest(
            contamination=0.01,
            n_estimators=100,
            random_state=42,
        )
        model.fit(data)
        return model, None

    if len(data) == 0:
        return None, (np.zeros(3), np.ones(3))

    mean = np.mean(data, axis=0)
    std = np.std(data, axis=0)
    std[std == 0] = 1.0
    return None, (mean, std)


def detect(features: list[float], model=None, stats=None) -> Detection:
    """Evaluate one metric vector without any database or network side effects."""
    vector = np.asarray(features, dtype=np.float64)
    if vector.shape != (3,):
        raise ValueError("expected request volume, error rate, and latency")

    if model is not None:
        prediction = model.predict([vector])[0]
        score = float(model.score_samples([vector])[0])
        is_anomaly = prediction == -1
        return Detection(
            is_anomaly=is_anomaly,
            score=score,
            flagged_features=list(FEATURE_NAMES) if is_anomaly else [],
        )

    if stats is None:
        raise ValueError("either model or stats must be provided")

    mean, std = stats
    z_scores = (vector - mean) / std
    flagged = [
        FEATURE_NAMES[index]
        for index, z_score in enumerate(z_scores)
        if abs(z_score) > 3
    ]
    score = min(float(np.max(np.abs(z_scores))), 9.9999)
    return Detection(bool(flagged), score, flagged)
