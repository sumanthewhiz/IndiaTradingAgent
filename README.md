# India Trading Agent

**New: credentials-only local dashboard.** Launch it, enter your Gemini and
Zerodha API credentials, and use **Save & authorize live startup**. Credentials
are encrypted for your Windows user; the controller handles stock screening,
official feeds, history warm-up and guarded startup without editing TOML.

```powershell
Set-Location 'C:\Projects\IndiaTradingAgent' # Use your checkout directory.
.\.venv\Scripts\python.exe -m india_trader dashboard
```

The launcher opens the local HTML interface at `http://127.0.0.1:8787`.
Read **[DASHBOARD_GUIDE.md](DASHBOARD_GUIDE.md)** before authorizing real orders.
Saved credentials are reused on restart, but expired broker sessions still
require official sign-in/2FA. An explicit Stop is not overridden on restart.
The LIVE marker requires fresh worker/broker/data evidence, not just saved keys.
Transient broker-stream interruptions now use bounded flat/reconciled recovery
with rebuilt history and verified fresh quotes; the dashboard reports the
sanitized underlying error instead of permanently latching a generic message.
Owned-position quote recovery also separates latest received prices from
indicator processing, retains native protection during brief gaps and can use
a fresh read-only broker quote for risk/exits without generating entry signals.
Missing historical candle tails now produce a bounded startup wait rather than
a generic startup failure. Flat late-session restarts respect the entry
cutoff without requesting entry-history warm-up.

Daily screening now uses the official **NIFTY 200** universe with recent
benchmark-relative history, current liquidity and issuer-event context.
Initial picks are re-ranked each trading date. **Intraday discovery also scans
the broader universe every 60 seconds**, with news-triggered scans no closer
than 30 seconds. Qualified additional stocks join a bounded 15-stock streaming
pool without restarting or resetting account limits. The dashboard shows the
latest scan and newly admitted names.
News recovery uses bounded retries, conditional requests and per-source health.
A transient unfinished publication may use only the last complete verified
snapshot within the unchanged freshness deadline; expired/invalid coverage
still blocks entries.

The automatic profile now includes stock-relative-strength alignment and a
volume-confirmed continuation setup, rather than vetoing every stock when
NIFTY is marginally below its open. Each completed candle produces an explicit
signal/no-trade diagnostic. Capital, loss, ownership and execution limits remain.

Pre-market research begins from **08:30 IST**, with continuous-market monitoring
from **09:15** and earliest confirmed five-minute opening entry **09:25** for
new session plans. The pre-open auction is not traded. Official Fed/ECB policy
context and explicit global-data gaps are shown in the dashboard.

The dashboard's automatic-live workflow is explicit opt-in and does not claim
validated profitability. The original paper/replay/manual CLI remains available
below; its manual live-qualification gates are separate and unchanged.

**Verification:** generate source-bound evidence locally with
`python -m india_trader self-test --out data\software-check.json`.
Tests and synthetic results do not establish live reliability or profitability.
Private operator incident notes and runtime evidence are not included in Git.

A local, event-driven **research and execution system**, not an expert trader or
a promise of profits. The offline demo/replay are paper-only; saving and
authorizing dashboard credentials requests real-money operation. The scope is fully
funded, long-only NSE cash equities; no derivatives, short selling, leverage,
bank transfers, or autonomous increases in capital.

Price streams and timers run ordinary Python, not repeated AI conversations.
Deterministic specialists handle bars, signals, risk, execution, reconciliation,
and reporting. Optional AI receives selected public news only, has persistent
daily budgets, and can only add a trading pause.

**Start with [GUIDEBOOK.md](GUIDEBOOK.md).** It covers installation, an offline
demo, licensed data, Zerodha configuration, paper/shadow operation, daily
authorization, live prerequisites, emergency procedures, and limitations.
See the guide's installation section for the validation commands.

**Original offline baseline (20 September 2026):** compilation, all 50 tests, configuration
checks and the synthetic demo passed on Python 3.12.10. This is not a live
qualification; the example configuration disables live trading.

```powershell
Set-Location 'C:\Projects\IndiaTradingAgent' # Use your checkout directory.
python --version                    # Python 3.11 or newer
python -m unittest discover -s tests -v
python -m india_trader demo --out data\demo
```

The offline demo and test suite need only the Python standard library. Broker
streaming additionally needs `python -m pip install -e ".[kite]"`. Development
verification has included read-only broker profile/funds checks; no real order
was placed by those checks.

Read the guide before creating a local configuration. There is deliberately no
"guaranteed daily profit" target, no self-modifying strategy, and no automatic
live promotion from a profitable synthetic demonstration.

## Repository setup and private files

Clone `https://github.com/sumanthewhiz/IndiaTradingAgent.git`, then follow
[GUIDEBOOK.md](GUIDEBOOK.md) to create the local environment. A clone contains
source, tests, guides and disabled example configuration, not credentials,
account authorization or trading history. Copy the example files to
`config.local.toml` and `session.local.json` only for the manual CLI workflow.

`.gitignore` excludes local configuration, `.env` files, credential stores,
private validation notes, databases, market data, logs and generated reports.
The dashboard's Windows-DPAPI state lives outside the checkout at
`%LOCALAPPDATA%\IndiaTradingAgent`; never copy it into Git, even encrypted.
Keep real keys out of examples and tests, and do not use `git add -f` to bypass
these exclusions. Review staged changes and run a local secret scan before
publishing future commits; ignore rules alone cannot detect inline secrets.
