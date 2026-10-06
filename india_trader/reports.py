from __future__ import annotations

import hashlib
import json
import random
from pathlib import Path
from statistics import mean

from .core import Config, SafetyError, bps, now_ist, paise
from .runtime import code_hash, research_hash
from .storage import Store


def build_report(store: Store, config: Config, day: str, operating_cost_rupees: float | None) -> dict:
    rows = [x for x in store.events() if x["at"][:10] == day]
    starts = [x for x in rows if x["kind"] == "RUN_START"]
    ends = [x for x in rows if x["kind"] == "RUN_END"]
    if not starts or not ends:
        raise SafetyError("A completed run record is required; an interrupted run is not a qualification sample.")
    start, end = starts[0], ends[-1]
    trades = [x for x in rows if x["kind"] == "TRADE_CLOSED"]
    operating = paise(operating_cost_rupees) if operating_cost_rupees is not None else 0
    if operating < 0:
        raise ValueError("Operating costs cannot be negative.")
    net = sum(x["net_paise"] for x in trades) - operating
    extra_slippage = sum(bps(x["buy_value_paise"] + x["sell_value_paise"], 2) for x in trades)
    fees = sum(x["fees_paise"] for x in trades)
    stress = net - fees - extra_slippage
    winners = [x["net_paise"] for x in trades if x["net_paise"] > 0]
    losers = [x["net_paise"] for x in trades if x["net_paise"] <= 0]
    starting = start.get("starting_equity", config.risk.capital_rupees * 100)
    peak, drawdown = starting, 0
    for row in rows:
        if row["kind"] == "EQUITY":
            peak = max(peak, row["equity"])
            drawdown = max(drawdown, peak - row["equity"])
    return {
        "schema": 1, "day": day, "mode": start["mode"],
        "dataset_kind": start["dataset_kind"],
        "out_of_sample": start.get("out_of_sample", False),
        "code_hash": start["code_hash"], "research_hash": start["research_hash"],
        "provenance": start.get("provenance", []),
        "flat": end["flat"], "halt": end["halt"], "quarantine": end.get("quarantine", False),
        "trade_count": len(trades), "winning_trades": len(winners),
        "losing_trades": len(losers), "net_paise": net, "modeled_fees_paise": fees,
        "operating_costs_declared": operating_cost_rupees is not None,
        "operating_cost_paise": operating, "cost_stress_net_paise": stress,
        "starting_equity_paise": starting, "sampled_drawdown_paise": drawdown,
        "win_rate": len(winners) / len(trades) if trades else None,
        "profit_factor": sum(winners) / abs(sum(losers)) if sum(losers) < 0 else None,
        "open_exposure": end["position"],
        "trades": trades,
        "limitations": [
            "Modeled fills, fees and displayed liquidity; not audited live profitability.",
            "Sampled equity can understate intrabar drawdown and real tail loss.",
            "Data licensing and out-of-sample designation are operator attestations.",
            "Synthetic results cannot authorize live trading.",
        ],
    }


def evidence_gate(reports: list[dict], config: Config, root: Path) -> dict:
    problems = []
    expected_code, expected_research = code_hash(root), research_hash(config)
    if len({(x["mode"], x["day"]) for x in reports}) != len(reports):
        problems.append("Duplicate mode/session reports.")
    if any(x.get("dataset_kind") != "licensed" for x in reports):
        problems.append("All data must be operator-declared licensed real data, never synthetic.")
    if any(not (x["mode"] == "shadow" or (x["mode"] == "paper" and x.get("out_of_sample")))
           for x in reports):
        problems.append("Training/in-sample runs cannot be included as evaluation evidence.")
    if any(x.get("code_hash") != expected_code or x.get("research_hash") != expected_research for x in reports):
        problems.append("All reports must match current code and research configuration.")
    if any(not x.get("flat") or x.get("quarantine") for x in reports):
        problems.append("Every evaluation session must finish flat and without reconciliation quarantine.")
    allowed_halts = {
        "", "Daily loss/profit-giveback threshold reached.", "Consecutive losing trades limit reached.",
    }
    if any(x.get("halt") not in allowed_halts for x in reports):
        problems.append("Resolve operational/data halts before live qualification.")
    if any(x.get("operating_costs_declared") is not True for x in reports):
        problems.append("Declare per-session data, hosting, AI and other operating costs.")
    if any(x.get("sampled_drawdown_paise", 0) > bps(
        x.get("starting_equity_paise", config.risk.capital_rupees * 100), 200
    ) for x in reports):
        problems.append("An evaluation session exceeded a 2% sampled-equity drawdown.")
    holdout = [x for x in reports if x["mode"] == "paper" and x.get("out_of_sample")]
    shadow = [x for x in reports if x["mode"] == "shadow"]
    if len(holdout) < 30 or len(shadow) < 20:
        problems.append("Need at least 30 held-out replay sessions and 20 forward-shadow sessions.")
    if any(x["day"] >= min((y["day"] for y in shadow), default="9999") for x in holdout):
        problems.append("Held-out replay period must precede forward-shadow evaluation.")
    if any(x["day"] > now_ist().date().isoformat() for x in reports):
        problems.append("Future-dated results are not evaluation evidence.")
    if sum(x["trade_count"] for x in reports) < 100:
        problems.append("Need at least 100 completed trades across evaluation sessions.")
    if sum(x["losing_trades"] for x in reports) < 5:
        problems.append("Insufficient observed loss cases; exercise adverse sessions.")
    for name, group in (("holdout", holdout), ("shadow", shadow)):
        if not group or sum(x["cost_stress_net_paise"] for x in group) <= 0:
            problems.append(f"{name} must be positive after doubled modeled fees, extra slippage and operating costs.")
    # Moving-block resampling preserves some local dependence, not all regime dependence.
    ordered = sorted(shadow, key=lambda x: x["day"])
    daily = [x["cost_stress_net_paise"] for x in ordered]
    lower = None
    if len(daily) >= 20:
        rng, samples = random.Random(741), []
        for _ in range(1000):
            sample = []
            while len(sample) < len(daily):
                start = rng.randrange(0, len(daily) - 4)
                sample.extend(daily[start:start + 5])
            samples.append(mean(sample[:len(daily)]))
        lower = sorted(samples)[49]
        if lower <= 0:
            problems.append("Forward-shadow 5th-percentile block-bootstrap daily net is not positive.")
    return {"passed": not problems, "problems": problems,
            "shadow_stress_mean_lower_95_paise": lower,
            "note": "A conservative research gate, not proof of a profitable strategy or regulatory approval."}


def qualification(
    root: Path, config: Config, report_paths: list[Path], software_path: Path, out: Path,
) -> dict:
    reports = [json.loads(path.read_text(encoding="utf-8")) for path in report_paths]
    assessment = evidence_gate(reports, config, root)
    software = json.loads(software_path.read_text(encoding="utf-8"))
    passed = (software.get("passed") is True and software.get("code_hash") == code_hash(root)
              and software.get("tests_run", 0) >= 20)
    result = {
        "approved_on": now_ist().isoformat(), "code_hash": code_hash(root),
        "research_hash": research_hash(config), "assessment": assessment,
        "evidence_gate_passed": assessment["passed"], "software_tests_passed": passed,
        "software_check_path": str(software_path.resolve()),
        "reports": [{"path": str(path.resolve()),
                     "sha256": hashlib.sha256(path.read_bytes()).hexdigest()} for path in report_paths],
        "static_ip_confirmed": False, "broker_approved_self_coded_algo": False,
        "current_order_rules_confirmed": False, "current_fees_confirmed": False,
        "licensed_realtime_data": False, "news_coverage_reviewed": False,
        "dedicated_cash_account": False, "supervised_incident_drill_passed": False,
        "broker_approval_reference": "",
    }
    if out.exists():
        raise SafetyError("Qualification file exists; archive it deliberately before replacing it.")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=2, allow_nan=False), encoding="utf-8")
    return result
