"""The promotion gate.

This is the whole safety argument for letting the system retrain itself
unattended, so the tests that matter are the ones asserting it REFUSES. A gate
that only ever says yes is not a gate.
"""
import pytest

from model_loop import (judge, MIN_IMPROVEMENT, TIGHT_MIN_GAIN,
                        TIGHT_TOLERANCE, OVERALL_TOLERANCE, CAL_TOLERANCE)


def res(inc=0.0300, chal=0.0290, inc_t=0.1300, chal_t=0.1250,
        inc_c=0.0100, chal_c=0.0100, base=0.0650):
    return {"inc_brier": inc, "chal_brier": chal, "inc_tight": inc_t,
            "chal_tight": chal_t, "inc_cal": inc_c, "chal_cal": chal_c,
            "base_brier": base, "tight_frac": 0.12, "tight_base_rate": 0.57}


def test_clear_overall_win_is_promoted():
    v = judge(res(inc=0.0330, chal=0.0290, inc_t=0.1500, chal_t=0.1400))
    assert v["promoted"] and v["promotion_path"] == "overall"


def test_marginal_overall_gain_alone_is_rejected():
    """The original behaviour, deliberately preserved: a rounding-error gain
    with no tight improvement earns nothing."""
    v = judge(res(inc=0.0300, chal=0.0300 - MIN_IMPROVEMENT / 2,
                  inc_t=0.1300, chal_t=0.1299))
    assert not v["promoted"]
    assert "below" in v["decision"]


def test_decisive_tight_win_is_promoted_despite_flat_overall():
    """Poland's case. Tight changes are ~12% of rows, so a large gain there is
    diluted ~8x in the overall figure and the overall gate could never see it."""
    v = judge(res(inc=0.0276, chal=0.0265, inc_t=0.1234, chal_t=0.1106,
                  inc_c=0.0097, chal_c=0.0103))
    assert v["promoted"] and v["promotion_path"] == "tight"
    assert "tight" in v["decision"]


def test_tight_win_rejected_if_overall_meaningfully_worse():
    """Winning on tight must not be a licence to degrade everywhere else."""
    v = judge(res(inc=0.0276, chal=0.0276 + OVERALL_TOLERANCE * 3,
                  inc_t=0.1234, chal_t=0.1234 - TIGHT_MIN_GAIN * 2))
    assert not v["promoted"]


def test_tight_win_rejected_if_badly_miscalibrated():
    v = judge(res(inc=0.0276, chal=0.0272, inc_t=0.1234, chal_t=0.1100,
                  inc_c=0.0100, chal_c=0.0100 + CAL_TOLERANCE * 2))
    assert not v["promoted"]
    assert "calibration" in v["decision"]


def test_overall_win_rejected_if_tight_regresses():
    """Rule 2: a model may not improve on average by getting worse at the only
    case anyone asks about."""
    v = judge(res(inc=0.0330, chal=0.0280,
                  inc_t=0.1300, chal_t=0.1300 + TIGHT_TOLERANCE * 3))
    assert not v["promoted"]
    assert "tight" in v["decision"]


def test_losing_to_the_base_rate_is_always_rejected():
    """Even with a huge tight win. A model that cannot beat 'always guess the
    average' is not a model."""
    v = judge(res(inc=0.0300, chal=0.0700, inc_t=0.1300, chal_t=0.0500, base=0.0650))
    assert not v["promoted"]
    assert "base rate" in v["decision"]


def test_missing_tight_scores_fall_back_to_the_overall_path():
    """A test set with no tight rows at all must not crash, and must not be
    promotable through the tight path."""
    v = judge(res(inc=0.0330, chal=0.0290, inc_t=None, chal_t=None))
    assert v["promoted"] and v["promotion_path"] == "overall"
    v2 = judge(res(inc=0.0300, chal=0.0299, inc_t=None, chal_t=None))
    assert not v2["promoted"]


def test_a_tie_is_not_a_promotion():
    v = judge(res(inc=0.0300, chal=0.0300, inc_t=0.1300, chal_t=0.1300))
    assert not v["promoted"]
