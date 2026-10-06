# India Trading Agent: configuration and operating guide

Version 0.1 | Prepared 20 September 2026 | Normal NSE cash sessions only

> **Version 0.2 dashboard:** For the new credentials-only UI and automatic
> preparation/startup, use [DASHBOARD_GUIDE.md](DASHBOARD_GUIDE.md). The commands
> and manually configured research/live gates below describe the retained
> advanced CLI workflow. Your existing local TOML/session files were not replaced.
> The dashboard now prepares from 08:30, starts normal-market monitoring at
> 09:15 and uses a five-minute opening range for new-day plans (first possible
> confirmation 09:25). The older manual defaults described below remain intact.

## 1. What you have, and what you do not

This is a local Python implementation of a **paper-first, event-driven trading
research system with a gated Zerodha execution adapter**. It is not a seasoned
human trader, a trained proprietary trading model, a regulated advisory service,
or a proven profitable strategy.

Shell execution was subsequently enabled. On 20 September 2026, **compilation,
all 50 offline tests, the configuration check and a standalone synthetic demo
passed on Python 3.12.10**. The saved demo ledger reconciled, finished with no
open position or active order, and recorded no AI usage. Source-bound test
results and demo artifacts are generated locally using the commands in section 3.
Private operator incident notes and account-specific evidence are not published.

This remains a prototype, not a live-certified trading system. Broker streaming,
optional broker dependencies, AI-provider integration, real-data strategy
performance and live execution have not been validated. Complete the remaining
research and operational gates before considering live trading.

A fresh clone contains no credentials or account state. The example
configuration disables live operation; the dashboard has a separate explicit
authorization workflow described in its guide.

There is no honest way to bundle "all previous data" or create a foolproof profit
machine. Historical depth/tick data and real-time news are licensed products;
new events and structural changes will always create uncertainty. This system
provides the acquisition interfaces, constrained decision logic, local knowledge,
research workflow, and controls. **You still need suitable data subscriptions,
broker authorization, independent review, and evidence of an actual net edge.**

The previous `Intraday_Trading_Plan.md` is not imported as trusted knowledge.
Its claims about routine monthly returns and professional success were not
established by backtests or live evidence. Do not use those claims as a forecast.

Financial suitability is personal. Consult a SEBI-registered adviser where
appropriate and a Chartered Accountant about taxation. Do not use essential
savings, borrowed funds, or money needed for living expenses.

### Implemented scope

| Component | Actual behavior |
|---|---|
| Market agent | Broker full-mode WebSocket, selected cash tickers and one reference index |
| Signal agent | Completed five-minute bars; opening-range breakout; optional VWAP pullback hypothesis |
| Event agent | Required-source heartbeats, append-only JSON news/events, optional approved RSS/Atom feeds |
| Risk/execution agent | Integer-paise accounting, sizing, costs, ownership, order intents, reconciliation, exits |
| AI context agent | Optional asynchronous public-news classifier; it can only add a pause |
| Research | Offline executable-quote replay, deterministic synthetic demo, forward shadow, reports and gates |
| Knowledge | Original local playbooks, operating rules, data contracts and primary-source references |
| Persistence | SQLite ledger/audit/AI budgets plus single-process and local-account locks |

There are **not** separate paid LLM calls for every agent. Most "agents" here are
bounded, deterministic components. There is no chat agent with a shell, account
passwords, or permission to rewrite its trading logic.

### Deliberate exclusions

No futures/options, expiry scalping, short selling, leverage, margin funding,
stock lending, commodity/currency trading, investment recommendations, smart-order
routing across brokers, automatic tax filing, or fund transfers. No universal
stock screener, automatic symbol-universe changes, autonomous strategy tuning,
or automatic corporate-calendar discovery is claimed.

Only NSE cash equities approved in your allowlist are eligible; the index is
reference-only. The initial supported broker is **Zerodha Kite Connect**. A
different broker needs a separately reviewed adapter and its own integration
tests; changing a hostname is not sufficient.

## 2. Architecture and token economics

```text
Broker WebSocket ---> bounded tick queue ---> bars/benchmark ---> candidate
                             |                                    |
Licensed event inbox ------> event pause --------------------------|
Approved RSS/Atom ----------> heartbeat/news                       v
Daily reviewed calendar ----> blackout gate                  deterministic risk
                                                                  |
Selected PUBLIC news ---> cache/budget ---> optional AI             |
                                        |                         v
                                        +--> PAUSE ONLY      durable order intent
                                                                  |
                                             fixed broker allowlist ---> exchange
                                                                  |
                                      order update / timed reconciliation
                                                                  |
                                              ledger, stops, exits, audit
```

The market is monitored continuously by ordinary networking/code during the
session. A human-readable market event triggers work; there is no LLM polling
loop searching the web every few seconds.

| Trigger | Work | LLM tokens |
|---|---|---|
| Price/depth tick | Validate, update bars, check existing-position risk | 0 |
| Closed five-minute bar | Evaluate enabled setups and risk eligibility | 0 |
| Broker order update | Request reconciliation, apply only new cumulative fills | 0 |
| One-second timer | Kill switch, time limits, stale-feed checks | 0 |
| Reconciliation interval | Broker orders/positions/cash; ownership checks | 0 |
| RSS poll / source heartbeat | Feed health and event de-duplication | 0 |
| Material approved public headline | Local pause, optional bounded AI classification | 0 by default |
| End of session | Local ledger/report calculations | 0 |

AI is disabled by default. When enabled, the example caps are:

- At most 2 requests per IST calendar day, with a 30-minute global cooldown.
- At most 4,000 **reserved** tokens per day across restarts of the same ledger.
- At most USD 0.10/day in estimated reserved spend, at your configured tariff.
- At most 1,400 serialized input bytes plus a 512-token framing reserve per call.
- At most 128 completion tokens, one outstanding AI request, and an 8-second timeout.

Every AI request reserves its worst-case budget **before** sending. Failure,
timeout, process restart, or malformed output does not refund the reservation.
There are no automatic AI retries. A full budget skips AI, not stops/exits or
deterministic market monitoring.

The dollar cap is an estimate, not a provider-enforced billing cap. Model pricing,
tokenization/framing, currency conversion and provider billing can change. Set
an independent provider spending limit and verify its behavior. Data feeds,
broker subscriptions, hosting, electricity, brokerage, taxes and slippage often
matter more than tokens.

AI receives only an explicitly public headline, publisher, timestamp and short
local knowledge excerpts. It does not receive your cash, positions, trades,
broker keys or access token. Its accepted output is exactly:

```json
{"pause_minutes": 15}
```

Valid integers are 0-60. Zero means no **additional** pause; it cannot remove an
existing pause or override a risk gate. A malformed/failed response adds a
no-verdict failure record in the current adapter rather than an invented
additional pause. Existing deterministic news pauses are never cleared.
News is untrusted data, not an instruction source.

## 3. Install and exercise the offline version first

Use Python 3.11 or newer. The demo, replay and tests use the standard library.
The commands below assume Windows PowerShell and the project directory.

```powershell
Set-Location 'C:\Projects\IndiaTradingAgent' # Use your checkout directory.
python --version
python -m unittest discover -s tests -v
python -m india_trader self-test --out data\software-check.json
python -m india_trader demo --out data\demo-001
```

The demo creates synthetic ticks, synthetic news heartbeats, an instrument
fixture, `paper.db`, and `report.json` in the selected new directory. The synthetic
path intentionally creates a trade/exit opportunity to exercise the machinery.
**A positive synthetic result is not evidence of trading skill.**

The demo refuses to overwrite existing fixtures. Choose a new output directory
for a new run. Do not delete live state to reuse a demo path.

The self-test command writes a result tied to the code/tests hash. Failed tests
must be fixed before continuing. The command does not mark missing tests or a
failed run as successful.

For broker streaming, use a project-local environment:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[kite]"
.\.venv\Scripts\python.exe -m india_trader self-test --out data\software-check.json
```

Use that same interpreter for subsequent commands. Activation is optional; do
not weaken machine execution policy just to activate an environment. Once a
tested environment is working, retain its exact dependency versions and re-test
upgrades. Do not silently upgrade dependencies on a trading morning.

## 4. Configure cash, symbols, risk and operating hours

```powershell
Copy-Item config.example.toml config.local.toml
Copy-Item session.example.json session.local.json
python -m india_trader doctor --config config.local.toml
```

Edit the copies, not the examples. `doctor` validates configuration and reports
whether expected environment variables exist; it does not authenticate, place
orders, validate market-data licensing or establish a profitable edge.

### Risk settings

| Setting | Example | Meaning |
|---|---:|---|
| `capital_rupees` | 25000 | Upper bound on the allocated trading capital |
| `cash_buffer_bps` | 1000 | Leave 10% of the frozen allocation uncommitted |
| `max_position_bps` | 2500 | Maximum 25% of initial allocation in the one position |
| `risk_per_trade_bps` | 25 | Modeled entry-to-stop-limit risk plus modeled fees <=0.25% |
| `daily_loss_bps` | 75 | Stop at 0.75% loss or giveback from the day's equity peak |
| `max_trades` | 3 | Maximum entry attempts, not a required trade quota |
| `max_consecutive_losses` | 2 | Stop after two nonpositive completed trades |
| `max_daily_buy_turnover_multiple` | 2 | Limit cumulative buy turnover and churn |

One basis point is 0.01%. These are cautious research defaults, not optimal
parameters or recommendations for your financial circumstances.

At a clean start, the allocation is the minimum of configured capital, daily
authorized capital where supplied, and conservatively reported cash. The engine
maintains a separate allocated cash ledger. Additional deposits and profits
do not automatically raise the original allocation limit. Losses reduce usable
allocated cash. The allocation is not reset by restarting the same ledger.

The adapter uses `CNC`, not `MIS`, and does not use collateral or derivative
margin. It rejects detected collateral/derivative-margin usage. It sizes with
both its own ledger and broker cash; broker available margin is not treated as
permission to borrow.

**No part of the code can top up this account through a funding endpoint.** The
HTTP wrapper permits only explicitly listed read endpoints and regular
cash-equity order placement/cancellation. It has no bank integration, transfer,
withdrawal, payment, mandate or loan endpoint.

That does not guarantee that losses stop exactly at a software limit. Gaps,
circuits, partial executions, slippage, rejected stops, fees, software faults and
outages can exceed modeled loss thresholds. Fully funded long positions limit
ordinary position-price loss to the capital invested, but fees, settlement
issues and broker liabilities can still create obligations. The software cannot
promise that a broker will never require additional funds. It will **not fetch
such funds from your bank**.

Use a dedicated trading account without unrelated holdings, pledges, other bots,
or concurrent manual trades. Configure independent broker-side limits where
available. Account API credentials may have broader inherent permissions than
this program's interface; protect the host and credentials independently.

### Universe and daily eligibility

Start with 2-5 independently researched, highly liquid cash stocks rather than
30. The example names are configuration examples, not buy recommendations.

The broker's current instrument master supplies instrument tokens, tick sizes,
exchange/segment and lot size; quote data supplies circuit bands. Do not hardcode
index-option lot sizes or old corporate-action-adjusted identifiers.

Before approving symbols, review exchange/broker restrictions, trade-to-trade
status, ASM/GSM or other surveillance, special corporate actions, results,
suspension risk, abnormal liquidity and price-band behavior. These issuer checks
are **not fully automated by this version**. Removing a symbol after it is held
is not a safe way to exit it; manage the position first.

The benchmark must be a reference index present in the broker's instrument
master. It is never sent to the order endpoint.

### Session manifest and calendar

`session.local.json` must contain the actual session date and explicit human
review. The example is intentionally disabled and is **not a holiday calendar**.
Verify exchange holidays and special sessions from the exchange. Muhurat,
special Saturday, auction-only, disaster-recovery and other nonstandard sessions
are outside this version's normal-session assumptions.

For a paper/shadow session, update `day`, `symbols`, `capital_rupees`, and set
`trading_day` / `reviewed` to true only after checking. Keep `live_approved=false`.

For scheduled risks, add expanded no-entry windows, for example:

```json
{
  "start": "2026-09-21T09:45:00+05:30",
  "end": "2026-09-21T10:30:00+05:30",
  "symbols": ["*"]
}
```

This is a **format example**, not an announcement of an event on that date.
Use `["*"]` for market-wide uncertainty and explicit symbols for issuer events.
Blackouts suppress new entries; they do not independently flatten positions.
If an existing position must be closed before an event, send a high-severity
event/pause in advance or use the kill switch early enough to confirm an exit.

## 5. Supply market data and event coverage

### Real-time quotes

Kite Connect supplies full-mode WebSocket ticks. You must have the relevant
market-data entitlement/subscription. The adapter subscribes only to the
configured universe plus the benchmark, not the entire exchange.

Bid/ask prices and quantities are mandatory for cash instruments. Last-traded
price alone is not an executable quote. The engine checks staleness, spread,
price bands, per-symbol chronology, cumulative volume and bar continuity.

Start before 09:15 IST, normally around 09:05-09:10 after your checks. A feed
first seen later than 09:15:03 does not establish a complete opening sequence;
**the manual CLI without managed warm-up does not fabricate missing history**.
The dashboard retrieves actual completed broker candles for managed warm-up,
including the first completed bar after a partial opening startup; it does not
replay historical breakouts as live trades. See `DASHBOARD_GUIDE.md` for that flow.

Full-mode timestamps from the Python broker client are converted from its host
local timestamp representation to IST. Maintain correct host time, timezone and
NTP synchronization. Do not "fix" a clock error by increasing allowed quote age.

### Events through a licensed collector

Append one complete UTF-8 JSON object and a newline to `data\events.jsonl`.
See [DATA_CONTRACTS.md](DATA_CONTRACTS.md). The source name must be approved in
`news.allowed_sources`; every `news.required_sources` entry needs a fresh,
independently healthy heartbeat.

```json
{"type":"heartbeat","source":"licensed-wire","at":"2026-09-21T09:30:00+05:30"}
{"type":"news","source":"licensed-wire","at":"2026-09-21T09:31:00+05:30","symbols":["RELIANCE"],"severity":"high","public":true,"headline":"Example only: issuer announces a material event"}
```

Do not use the example timestamps for a live session. Do not emit heartbeats
merely because a timer is alive: the upstream data connection and delivery must
actually be healthy. A fake heartbeat defeats the protection.

High/critical events and selected high-impact keywords cause an immediate local
pause. If an affected position is owned, that pause requests a managed exit.
The classifier is deliberately conservative; it does not understand every
issuer alias, language, contradiction, correction or macro connection.

Use an authorized wire/vendor collector for essential events. This project
does not include credentials or a universal connector for every commercial news
API. Transform its licensed payload into the documented local contract and
test timestamps, symbol mapping, corrections, duplicates, missing messages and
the collector's health behavior before relying on it.

### Optional RSS/Atom

You can configure approved HTTPS public publisher URLs in `news.rss_urls`. Add
each publisher hostname to `allowed_sources` and, if essential, `required_sources`.
Requests use conditional headers, a minimum polling interval, size limits and
bounded timeouts. Failed essential coverage blocks entries; it is not replaced
by made-up data or an unbounded web-search loop.

RSS is **not** a low-latency market-news service. Many feeds are delayed,
incomplete, require entitlements, change structure or prohibit redistribution.
Use actual publisher-provided URLs and terms, not guessed endpoints. No RSS
endpoint is preconfigured because it would falsely imply complete coverage.

## 6. How a trade is actually selected and managed

### Opening-range breakout, enabled by default

1. Observe the full 09:15-09:30 opening sequence.
2. Use a completed five-minute bar crossing above the opening-range high.
3. Require breakout-bar volume at least 1.5 times average opening-bar volume.
4. Require the cash stock above its session VWAP and a fresh benchmark above
   its first observed session price.
5. Restrict this setup to 09:35-10:30.
6. Set a structural stop one tick below the lower of breakout-bar low and
   opening-range high.
7. Pass every global entry, data, calendar, cost, risk and ownership check.

This is an explicit hypothesis, not a claimed historically profitable rule.
Volume ratios, opening ranges and benchmark alignment must be evaluated using
your exact instruments, latency, data and costs.

### Optional VWAP pullback

`enabled=["orb","vwap_pullback"]` additionally enables a restrictive long-only
continuation rule after at least 20 complete bars. It requires prior closes
above current session VWAP, rising recent closes, a bullish rejection of VWAP,
a close above EMA20, and increased bar volume. Its stop is below the rejection
bar. It shares every risk/execution gate.

Changing the enabled setup changes the research hash and invalidates prior
qualification. Do not add it simply because the opening strategy is losing.

### Sizing and net-profit hurdle

The engine starts with an executable ask plus a small bounded allowance, then
rounds to the daily tick. It derives a stop-limit price below the stop trigger
and a target from structural stop distance. Integer search finds the largest
whole-share quantity satisfying:

```text
buy notional + modeled buy fees <= remaining funded allocation and broker cash
buy notional <= position cap and remaining daily turnover cap
quantity <= allowed fraction of BOTH displayed bid and ask size
quantity * (entry limit - stop limit) + modeled round-trip stop fees <= risk budget
modeled target net >= configured minimum net reward/risk
modeled target net >= configured multiple of modeled round-trip trading costs
```

The defaults target 3 times structural stop distance because costs and a
stop-limit gap consume a meaningful fraction of small moves. The modeled net
reward/risk minimum is 1.5 and profit must clear at least 3 times costs.
None of these ratios establish a positive win probability or expectancy.

If no single share clears all limits, the engine takes **no trade**. It does
not shrink a stop arbitrarily, borrow money, move to cheap options, or lower the
cost hurdle to make a trade fit.

### Execution sequence

Only limit `DAY` entries/exits and protective stop-limit `DAY` orders are sent.
No market, stop-market, IOC, AMO, iceberg, autoslice, bracket or unsupported
broker order type is silently substituted.

An order intent and its unique local reference are committed before submission.
Broker acknowledgment is not a fill. Cumulative fills are reconciled against
broker orders and positions; duplicates do not increment shares or cash twice.
A timeout is an **unknown submission**, not a reason to resend.

Once a fill is observed, the engine requests broker-held stop-limit protection
for uncovered owned shares. Partial entries have their unfilled remainder
cancelled; observed fills are handled rather than discarded. There is an
unavoidable latency window between fill, observation and accepted protection.
This is not an atomic exchange bracket and must not be advertised as one.

A target, stop, maximum hold, high-impact event, kill request or scheduled
flatten can request an exit. Before sending an exit sell, the engine cancels
protective sells and waits for **confirmed terminal state and reconciliation**.
If the stop fills during cancellation, only remaining owned shares can be sold.
The cancellation-to-exit interval is another protection gap.

Entry orders expire locally after 8 seconds; exits are bounded-repriced with
limits on retries. The final working limit is not represented as a successful
exit. Exhausted rejected/cancelled exits quarantine the account for intervention.

Stop-limit orders can remain unfilled after a gap or circuit. A software stop,
daily loss threshold, kill file, or 15:10 flatten target is a request/constraint,
**not a guarantee of liquidity or a guaranteed maximum loss**.

## 7. Historical replay and disciplined research

Licensed executable bid/ask tick history is required for serious replay. This
version does not infer trades from OHLC candle highs/lows or pretend historical
last prices supply queue positions and bid/ask depth.

Prepare one session of normalized ticks, matching instrument metadata, a reviewed
session file and timestamped event/health data:

```powershell
python -m india_trader replay `
  --config config.local.toml `
  --session research\2026-08-03-session.json `
  --ticks research\2026-08-03-ticks.csv `
  --instruments research\2026-08-03-instruments.json `
  --events research\2026-08-03-events.jsonl `
  --db data\replay-2026-08-03.db `
  --report data\reports\replay-2026-08-03.json `
  --out-of-sample `
  --operating-cost-rupees 75
```

Dates and INR 75 are examples, not current data or an estimate of your costs.
Supply actual licensed inputs and your conservative allocated daily overhead.
Create a new replay database each run. Never replay into a shadow/live database.

The simulator uses a later executable quote, finite displayed-size participation,
partial fills, stop-limit semantics and modeled costs. It still cannot recreate
the actual exchange queue, hidden liquidity, broker latency, rejected orders,
market impact or a real crash perfectly. Live performance can be worse.

Use chronological training, validation, untouched holdout, then forward shadow.
Account for survivorship, delistings, corporate actions, adjusted versus raw
prices, point-in-time news arrival, event revisions, stale quotes and holidays.
Never optimize on holdout or repeatedly call the same periods "out of sample".

Do not replay splits/bonuses with mismatched unadjusted execution prices and
adjusted signal prices. Do not assume every historic security was in today's
allowlist. Keep vendor/license, collection time, source hashes and transformation
version with the dataset. See the research knowledge note.

The built-in research gate requires at least:

- 30 held-out real-data replay sessions preceding the shadow period.
- 20 real-time forward-shadow sessions using the same research configuration.
- 100 completed trades across evaluation, including at least five losses.
- Finished-flat sessions, no ownership quarantine and no unexplained operational halts.
- Explicit daily operating-cost declarations.
- Positive replay and shadow net under doubled modeled trading fees plus an
  extra 2 bps of round-trip traded notional for adverse execution.
- A positive 5th-percentile daily-net estimate from 1,000 five-session
  moving-block bootstrap resamples of shadow results.
- No evaluation session with a recorded sampled-equity drawdown above 2%.

These thresholds are conservative design choices, not scientific proof of a
durable edge. Samples can still be too small, regimes can change, intraday
drawdowns can be missed by sampling, and choosing only favorable sessions
invalidates the exercise. Require broader independent review for meaningful
capital. Do not tune until these particular thresholds happen to pass.

## 8. Broker access and forward-shadow operation

Create/configure your own Kite Connect application and obtain the appropriate
data access. Complete the broker's documented interactive login/token flow and
required two-factor authentication. Do not automate password/TOTP entry,
circumvent login controls or assume tokens remain valid indefinitely.

Use process environment variables, populated from a trusted local secret store
or an interactive prompt. For example:

```powershell
$env:KITE_API_KEY = Read-Host 'Kite API key'
$token = Read-Host 'Kite access token' -AsSecureString
$env:KITE_ACCESS_TOKEN = [System.Net.NetworkCredential]::new('', $token).Password
```

Do not place credentials in TOML, the session JSON, scripts, Git, chat or screenshots.
This guide does not request bank usernames, bank passwords, UPI PINs, API secrets
for payments, or withdrawal authorization.

Run shadow with live quotes but simulated trades:

```powershell
python -m india_trader run `
  --config config.local.toml `
  --session session.local.json `
  --mode shadow `
  --db data\shadow.db
```

Shadow uses a read-only broker route allowlist. It cannot send an order even if
a strategy emits a candidate. Its P&L still uses simulated fills/costs, not your
broker account balance. Use a consistent ledger for forward-shadow continuity,
not a new bankroll after each losing day.

The system runs locally. Keep the process, machine, network and power available.
It is not a hosted service or a tested high-availability system. Use the broker
terminal as an independent view and keep access to broker support.

After a normally ended session:

```powershell
python -m india_trader report `
  --config config.local.toml --db data\shadow.db `
  --day 2026-09-21 --out data\reports\shadow-2026-09-21.json `
  --operating-cost-rupees 75
```

A force-killed process is not a clean completed sample. Reconcile it rather than
manufacturing a successful report.

## 9. Optional AI configuration

Begin without AI. Measure whether occasional classification adds useful pauses
or merely removes good trades. It is not needed for monitoring or execution.

To experiment, explicitly set both `ai.enabled=true` and
`ai.share_public_news=true`, choose a model supporting the documented chat
completion request/JSON response shape, update its actual pricing, and load
`TRADER_AI_API_KEY` into the process environment. The adapter now permits the documented OpenAI endpoint and restricted Google
Gemini native/OpenAI-compatible endpoints. The dashboard automatically chooses
the Gemini native profile; manual CLI settings remain explicit.

Do not assume every model supports the same output-token parameter. Test the
selected provider/model with non-sensitive public headlines in shadow first.
No provider integration has been exercised in the authoring environment.

Your provider may charge or retain data according to its terms. Only mark content
public when redistribution/processing is permitted. No arbitrary browsing,
remote tools, Python execution, broker tools or retrieval outside the configured
local knowledge folder are available to the classifier.

Changing the AI/news configuration changes the research fingerprint. Requalify
instead of assuming an extra model improves a validated strategy.

## 10. Live activation is a separate, deliberate decision

Do not use this section as a shortcut past validation. The implementation has
not been live-tested. Have an independent engineer and knowledgeable operator
review it, including execution races and your broker's actual response shapes.

### Broker/regulatory checks

Obtain current confirmation from your broker for personal self-coded retail
algorithms, API permissions, whitelisted static IP requirements, applicable
registration/algo identifiers, order types/validity, rate limits, auditing and
exchange restrictions. Do not equate a client `tag` with a regulatory algo ID.

The supplied code uses a fixed API origin, a conservative approximately one
request-per-second spacing, and limit-family DAY orders. **That is not, by
itself, regulatory approval or a compliance certificate.** Primary-source
references and the limits of this research are in [SOURCES.md](SOURCES.md).

### Produce qualification artifacts

```powershell
python -m india_trader self-test --out data\software-check.json
python -m india_trader qualify `
  --config config.local.toml `
  --reports 'data\reports\*.json' `
  --software-check data\software-check.json `
  --out data\qualification.json
```

The output explains failed research gates and initially leaves manual
attestations **false**. Do not edit a false test/evidence result into true.
Live startup recomputes the evidence gate, verifies report hashes and requires
an actual source-matching software test result.

Only after genuinely completing them, record the manual confirmations:
static IP; broker approval/reference; current order rules; current fees; licensed
data; news coverage; dedicated cash account; and a supervised incident drill.
The operator's review must be no more than seven days old. Renewing a date is
not a substitute for reviewing changed conditions.

Retain/archive an existing qualification file before deliberately replacing it.
Do not change code or strategy to bypass the gate. Code, tests, local knowledge and dependency-manifest changes invalidate
the code hash; strategy/risk/fee/news/AI changes invalidate the research hash.

### Final configuration

In `config.local.toml`, set the actual `live.broker_user_id`, applicable broker
`algo_id` if instructed, and `live.enabled=true`. Other account IDs are rejected.
Run `doctor` and copy the resulting `config_hash` into today's session manifest.
The session must also contain:

- Today's reviewed normal trading date and exact approved symbol set.
- `live_approved=true`.
- The same account ID and an authorized capital amount no greater than configuration.
- The relevant scheduled blackouts.

The live-section change alone does not change the research hash, but does change
the full daily configuration hash. Keep paper, shadow and live databases separate.

Live orders require the explicit command flag as well:

```powershell
python -m india_trader run `
  --config config.local.toml --session session.local.json `
  --mode live --db data\live.db --accept-live-risk
```

This command can place real orders. Do not execute it until all prerequisites
are met. No such command was executed during creation of this project.

Begin any eventual live pilot with a separately evaluated small allocation.
Reducing/changing allocation changes the research configuration; check minimum
shares and transaction-cost economics rather than assuming the same result.
Monitor the entire pilot. Do not leave an unproven bot unattended.

## 11. Daily operating procedure

| IST window | Operator responsibility | Engine behavior |
|---|---|---|
| Before 09:05 | Review broker/exchange notices, calendar, issuer events, universe, costs, limits and account | No automatic research assumption |
| 09:05-09:14 | Authenticate, confirm data health, clock/power/network, start process, inspect broker state | Connect and prepare |
| 09:15-09:30 | Confirm complete feeds and no fault/kill state | Build opening range; no new entries |
| 09:35-10:30 | Supervise authorized strategy | ORB may produce eligible candidates |
| Later, before 14:30 | Maintain event coverage and watch health | Optional qualified VWAP strategy; manage positions |
| 14:30 onward | Do not force a final profit | No new entries; cancel aged/unwanted entries |
| 15:10 onward | Confirm actual remaining shares and orders | Request scheduled flatten using bounded limits |
| Before 15:30 | Resolve any residual exposure with broker | No promise of a fill or forced settlement |
| After session | Reconcile contract notes/cash, journal incidents and export report | Exit normally only when flat/no active intents |

No opportunity is an acceptable outcome. There is no daily income target and
no strategy switch to compensate for a losing morning.

Keep the same live ledger when restarting. Do not run the same account on
another computer or start a second process with a different database. The
local-account lock covers processes on this host, not distributed deployments.

## 12. Kill switch and incident runbook

From another terminal in the project:

```powershell
python -m india_trader halt --kill-file data\HALT
python -m india_trader status --db data\live.db
```

The running engine observes that file, cancels pending entries and requests a
managed exit where fresh quotes and confirmed ownership make that possible.
The halt is latched in the ledger. Removing the file does not erase the halt.

`status` reads local saved state. It is not an independent live broker query.
Confirm quantities, fills and open orders in the broker terminal.

First Ctrl+C requests shutdown with managed flattening. A second Ctrl+C forces
process shutdown; it is **not** an exit fill. A dead process cannot observe a kill
file. Native accepted stops may remain at the broker, but are not guaranteed to
fill and DAY orders may expire at the session boundary.

| Condition | Automated behavior | Required operator action |
|---|---|---|
| Missing required news heartbeat | Refuse new entries | Restore genuine coverage; do not synthesize a heartbeat |
| High-impact event | Pause affected entries and request managed exit | Verify source, exposure and actual fills |
| Stale position quotes / WebSocket fault | Latch halt; retain protection until fresh exit handling | Restore connectivity and inspect native stop |
| Queue overflow/missing bars | Latch halt; do not invent market history | Investigate load/feed gaps; no blind restart-to-trade |
| Order submission timeout | Persist UNKNOWN; reconcile by known reference; never blindly resend | Check broker order book/trades; resolve ambiguity |
| Native stop unfilled after gap/circuit | No claim of protection/exit success | Broker terminal/support; liquidity may not exist |
| Stop fills during cancellation | Reconcile remaining shares before any new sell | Confirm no overlapping sells |
| Repeated position mismatch/unowned activity | Quarantine; no new execution decisions | Establish ownership with broker before acting |
| Rejected/exhausted exits | Bounded attempts, then critical halt/quarantine | Manual incident handling; exposure may be unprotected |
| Funds decline / collateral detected | Block unsafe new activity; no top-up route | Reconcile account and broker obligations |
| Expired API token | Halt on broker errors; no automated login bypass | Reauthenticate through broker; verify open protection |
| Daily loss/giveback / consecutive losses | Stop new entries, request managed closure | Stop for the day, review; do not reset the ledger |
| Disk/persistence/process failure | Fail rather than pretend an order succeeded | Inspect broker immediately; preserve files and logs |
| Session ends with shares/orders | Remain unresolved, not falsely "flat" | Handle delivery/settlement and next-day risk with broker |

When handling an exit manually, first check whether a stop or exit already
filled. Cancel conflicting outstanding owned sells and confirm terminal states
before selling only the remaining owned shares. Do not blindly send another
sell just because the local console looks stale.

Quarantine has no automatic "ignore and resume" command. Preserve its ledger,
audit the incident and independently verify a flat broker. Only after a
reviewed reconciliation/requalification should you create a new ledger if needed;
do not reset allocated capital upward to hide losses.

## 13. Evaluating usefulness and controlling total costs

Judge the system on net expectancy and failure containment, not on how often it
trades or how convincing its explanations sound. Track completed trade count,
after-cost P&L, loss distribution, slippage, missed/partial fills, downtime,
unexpected rejections, drawdown, parameter stability and decision reasons.

Compare with doing nothing, a passive investment appropriate to your situation,
and the value of the time you spend supervising. A profitable gross chart can
still be an uneconomic service after subscriptions, taxes, electricity and labor.

The report's `net_paise` subtracts modeled trading costs and your declared daily
operating overhead. It does not automatically calculate personal income tax or
certify contract-note reconciliation. The stressed metric adds a second copy
of trading fees and an additional adverse-execution allowance.

Record actual brokerage/statutory charges, split-fill effects, API/market-data
subscriptions, AI bills at actual exchange rates, hosting and incident expenses.
Never assume a static fee table remains current. Income-tax classification,
turnover, loss offsets, audit obligations and filing depend on current law and
your circumstances; use a qualified CA.

Freeze strategies during evaluation. Research changes offline, compare against
a fixed baseline, retain failed experiments and revalidate on new untouched
periods. Do not let an LLM edit strategy parameters, increase risk, or reinterpret
losing trades as long-term investments during a session.

## 14. Files, privacy and maintenance

| Location | Purpose |
|---|---|
| `india_trader\core.py` | Models, strict configuration and integer money |
| `india_trader\market.py` | Tick/bar state and signal hypotheses |
| `india_trader\engine.py` | Risk and durable execution state machine |
| `india_trader\broker.py` | Paper broker and restricted Kite REST adapter |
| `india_trader\events.py` | Event ingestion, RSS, local retrieval and optional AI |
| `india_trader\runtime.py` | Connected loop and live authorization |
| `india_trader\replay.py`, `reports.py` | Synthetic exercise, historical replay and evidence gate |
| `india_trader\cli.py` | User commands |
| `tests\test_agent.py` | Offline regression/failure-path tests |
| `knowledge\` | Original local reference notes, not comprehensive market intelligence |
| `data\` | Private ledgers, events, reports, qualification and kill switch |

Use local encrypted storage and appropriate Windows account/file permissions.
Do not put real ledgers, credentials or licensed raw feeds in a public Git
repository, shared folder or unapproved synchronization service. `.gitignore`
helps avoid accidents but is not an access-control or encryption mechanism.

Back up ledgers using a consistent SQLite backup while stopped/flat or through
an appropriate SQLite backup procedure. Copying only a live `.db` while ignoring
its WAL can lose committed state. Do not archive/delete an active database.
Agree retention requirements with your broker/compliance adviser.

This is not a hosted monitoring service. Console/audit alerts need an attentive
operator; out-of-band paging, a separate broker-side risk supervisor, automated
data entitlement validation and multi-host failover are not implemented. Add
those only as separately specified, tested changes, not as claims about v0.1.
