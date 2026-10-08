from __future__ import annotations

import unittest

from india_trader.dashboard import participation_summary


class ParticipationSummaryTests(unittest.TestCase):
    def test_summary_uses_each_candle_not_deduplicated_rejection_events(self):
        signal = {
            "kind": "SIGNAL_EVALUATED", "symbol": "DEMO", "bar_end": "2026-10-08T11:00:00+05:30",
            "checks": {"entry_window": True, "market_alignment": True}, "setup": "momentum_breakout",
            "reason": "Risk gate: expected target does not clear net reward/cost hurdle",
            "risk_details": {"modeled_target_net_paise": 100, "modeled_risk_paise": 200},
        }
        events = [signal, {**signal, "bar_end": "2026-10-08T11:05:00+05:30"},
                  {"kind": "CANDIDATE_REJECTED", "reason": signal["reason"]}]
        result = participation_summary(events)
        self.assertEqual(result["evaluated_bars"], 2)
        self.assertEqual(result["qualified_signals"], 2)
        self.assertEqual(result["entry_plans"], 0)
        self.assertEqual(result["risk_rejections"][0]["count"], 2)
        self.assertEqual(result["latest_risk_details"]["bar_end"], events[1]["bar_end"])

    def test_after_hours_and_non_signal_audits_do_not_distort_entry_window_counts(self):
        result = participation_summary([
            {"kind": "SIGNAL_EVALUATED", "checks": {"entry_window": False, "market_alignment": False}},
            {"kind": "SIGNAL_EVALUATED", "checks": {"entry_window": True,
                                                   "market_alignment": False, "stock_above_vwap": False}},
            {"kind": "EQUITY"}, {"kind": "ENTRY_PLAN"},
        ])
        self.assertEqual(result["evaluated_bars"], 2)
        self.assertEqual(result["entry_window_bars"], 1)
        self.assertEqual(result["qualified_signals"], 0)
        self.assertEqual(result["entry_plans"], 1)
        self.assertEqual(result["top_signal_blocks"],
                         [{"reason": "market alignment", "count": 1},
                          {"reason": "stock above vwap", "count": 1}])

    def test_empty_history_never_claims_opportunities_or_simulated_trades(self):
        result = participation_summary([])
        self.assertEqual(result["qualified_signals"], 0)
        self.assertEqual(result["entry_plans"], 0)
        self.assertEqual(result["top_signal_blocks"], [])
        self.assertIsNone(result["latest_risk_details"])


if __name__ == "__main__":
    unittest.main()
