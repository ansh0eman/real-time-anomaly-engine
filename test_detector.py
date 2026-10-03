import numpy as np
import pytest

from detector import detect, fit_detector


def test_empty_history_uses_neutral_fallback():
    model, stats = fit_detector([])

    result = detect([0, 0, 0], model=model, stats=stats)

    assert model is None
    assert result.is_anomaly is False
    assert result.flagged_features == []


def test_z_score_fallback_identifies_only_outlying_features():
    history = [[100, 0.02, 40], [110, 0.03, 45], [90, 0.01, 35]]
    model, stats = fit_detector(history)

    result = detect([5_000, 0.02, 40], model=model, stats=stats)

    assert result.is_anomaly is True
    assert result.flagged_features == ["request_volume"]
    assert result.score > 3


def test_constant_features_do_not_divide_by_zero():
    model, stats = fit_detector([[100, 0.01, 20]] * 5)

    result = detect([104, 0.01, 20], model=model, stats=stats)

    assert np.isfinite(result.score)
    assert result.flagged_features == ["request_volume"]


def test_invalid_feature_vector_is_rejected():
    with pytest.raises(ValueError, match="expected request volume"):
        detect([1, 2], stats=(np.zeros(3), np.ones(3)))
