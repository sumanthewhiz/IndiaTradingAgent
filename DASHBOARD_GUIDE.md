# Trading dashboard: credentials-only setup

Version 0.2 | 21 September 2026 | Windows | Gemini + Zerodha

## Start here

From this project directory:

```powershell
.\.venv\Scripts\python.exe -m india_trader dashboard
```

Or run `Start-IndiaTradingAgent.ps1` if your normal PowerShell policy permits it.
Do not weaken enterprise execution policy to run a launcher; the Python command
above is sufficient.

The application opens an authorized browser tab at **http://127.0.0.1:8787**.
Keep the application terminal running. Re-running the launcher while the server
is already running opens an authorized tab instead of starting another engine.
The plain URL alone cannot read account data: the launcher supplies a temporary
UI capability in a URL fragment, which the page removes from the address bar.

The HTML page is `india_trader\web\dashboard.html`. It is backed by the local
Python service, not a static mockup. Opening the file directly will not enable
broker access.

## Enter only credentials

Open **Credentials** and supply:

1. **Gemini Developer API key.** Create it in
   [Google AI Studio](https://aistudio.google.com/api-keys). The Gemini app/AI Pro
   subscription is distinct from API billing and entitlements.
2. **Zerodha Kite API key.**
3. **Kite API secret**, used for the official sign-in/token exchange.
4. Optionally, an already-issued **current Kite access token**. Without one,
   select **Sign in with Zerodha** after saving.

Use **Save & authorize live startup**. This is explicit authorization to start
real-money trading under the displayed fixed policy once validation succeeds.
It is not merely a draft form save.

You do not need to edit TOML, supply a daily manifest, pick stocks, choose an AI
endpoint, set token quotas, or paste news feeds. Existing `config.local.toml`
and `session.local.json` files are left untouched and are used only by the
advanced legacy CLI workflow.

New keys are protected with Windows DPAPI for the current Windows user.
The app never sends saved key values back to the browser. Empty fields on a
later save retain their stored values. Do not use your trading password,
bank password, PIN, or OTP in these fields.

### Broker setup cannot be bypassed

Your Kite developer application must use this registered redirect URL:

```text
http://127.0.0.1:8787/broker/callback
```

The broker must provide the required API/algorithmic-trading permissions,
registered static IP where required, streaming/historical data entitlement,
NSE cash trading, and CNC/LIMIT/SL support. The app does not manufacture a
broker approval or bypass account restrictions.

If the dashboard reports **Missing broker permissions: NSE cash exchange**,
the authenticated Kite profile does not list `NSE` among its enabled exchanges.
For example, a profile reporting only `MF` exposes mutual-fund access, not the
NSE cash access this agent requires. `CNC`, `LIMIT` and `SL` can still appear in
the profile; their presence does not replace the exchange permission.

Check that you logged in to the intended Zerodha client account, and have
Zerodha confirm/activate the NSE equity segment. If the account is inactive or
requires Re-KYC, follow the broker's
[official reactivation process](https://support.zerodha.com/category/your-zerodha-account/your-profile/kyc-re-activation/articles/re-activate-my-account).
A paid API/data subscription does not itself activate cash-market trading.
After activation, sign in again to obtain a fresh broker session, then resume.
The app will not substitute another exchange or bypass the missing permission.

Kite access tokens expire at **06:00 IST the following day**, or earlier if
revoked. Persistent API keys cannot bypass this requirement. When needed, the
dashboard shows **Broker sign-in required**. Complete the login/2FA on Zerodha's
own page; the callback stores the new access token and continues automatically.
Your Gemini key, broker API key, and API secret do not need to be entered again.

Only Zerodha is implemented for the dashboard. Credentials for a different
broker cannot be made compatible by pasting them into these fields.

### A 403 does not always mean an IP problem

The app now shows the denied HTTP method/endpoint, broker error type and a
bounded, redacted reason rather than just "HTTP 403."

| Broker response | Meaning and next step |
|---|---|
| `GET /quote` or historical candles; `403 PermissionException`, "Insufficient permission for that call." | The current Kite app is being denied market-data access. In the developer portal's **My Apps**, confirm the paid **Connect** subscription is active for the same API key saved here. The Personal/free plan does not include live or historical data. |
| `403 TokenException` or `401` | Session expired/invalidated. Use **Sign in with Zerodha** for a fresh broker session. |
| An error explicitly reporting IP/whitelist rejection | Check the actual outbound static IP of the trading host against **Developer Profile -> IP Whitelist**. A VPN/proxy or changed connection can use a different outbound IP. |
| Another permission or non-JSON error | The app remains blocked and shows the request context; contact Zerodha with the sanitized endpoint/error details rather than disabling safeguards. |

The current official Connect price is INR 500 per app/month; confirm it in the
[broker's plan documentation](https://support.zerodha.com/category/trading-and-markets/general-kite/kite-api/articles/what-are-the-charges-for-kite-apis).
IP whitelisting enables API order placement; it does not purchase market-data
permissions. A paid subscription on another app/key does not establish
entitlement for the key saved here. If this exact app is already subscribed,
ask Zerodha to check that key's data entitlement before changing keys or IPs.

The agent will not buy a subscription, change your whitelist, submit a test
order to probe permissions, or retry an uncertain order automatically.

## What the system handles

### Automatic preparation

After authorization, the controller:

- Verifies that the current code passes its offline regression suite.
- Validates the broker session, account identity, cash permissions and account
  ownership; rejects incompatible products/collateral and unrelated holdings.
- Checks Gemini model/key metadata without performing an unnecessary paid
  inference simply to start the app.
- Obtains current broker instruments, tick sizes, circuit bands and quotes.
- Loads official NIFTY 200 constituents. If unavailable, it can use a recent
  cached constituent list or a clearly identified bundled cash-stock candidate
  list, never falsely calling the latter current index membership.
- Screens liquid, narrow-spread, affordable shares and rejects large opening
  gaps. It then ranks at most 30 candidates using completed daily history,
  benchmark-relative strength, current turnover/spread and issuer events.
- Checks official NSE issuer announcements and RBI releases. It maps issuer
  names to symbols and excludes observed results/board-meeting/corporate-action
  risks from selection.
- Selects up to five initial stocks with industry diversity. Those seed picks
  remain stable, while the worker now discovers and admits additional stocks
  throughout the entry session. The next session's seed selection is screened afresh.
- Loads completed broker five-minute candles to warm indicators. In-progress
  startup bars are not invented or treated as complete trade signals.
- Starts a separately supervised order-management process, with the same
  persistent account ledger and cash allocation.

### Preparation and the opening session

NSE's 09:00-09:15 cash session is a **pre-open auction**. Normal continuous
trading starts at **09:15**, not 09:00. This application does not submit auction
orders, implement auction-specific matching rules or promise to capture every
opening move.

The current automatic profile separates preparation from entry eligibility:

| IST | Automatic work |
|---|---|
| 08:30 onward, after valid daily broker login | Verify keys/cash, collect issuer and official global-policy context, screen NIFTY 200 and cache completed history |
| Before 09:15 | Publish **Pre-market research ready**; previous-close research prices are not executable quotes and no worker/order is launched |
| 09:15 onward | Start the continuous-market worker, subscribe to fresh quotes, and warm indicators without inventing missing data |
| 09:15-09:20 | Establish a five-minute opening range |
| 09:20-09:25 | Wait for a completed breakout-confirmation candle |
| 09:25 earliest | An opening entry can qualify, only after all signal, market, cost, calendar and risk checks |
| 14:30 / 15:10 | Unchanged: stop new entries / request scheduled exits |

Pre-market source checks occur at bounded intervals, at most every five minutes;
completed history is cached for the session. The controller schedules its next
check at the 09:15 transition instead of waiting until 09:35. Broker/data latency
can delay readiness; the app will not waive freshness checks to catch a move.

If startup misses part of the first five-minute bar, the worker waits for that
bar to complete and fetches the actual broker candle before using it. Ticks
already covered by that historical candle are not replayed into live signals.
A historical breakout is never submitted retrospectively.

**An existing same-day plan keeps its existing opening profile.** Plans made by
the earlier release retain their 15-minute range and 09:35 entry setting for
that session. New-day plans use the five-minute range/09:25 profile; changing the
opening rule never resets cash, losses, trade counters or owned exposure.
The manual CLI's default remains the original 15-minute range unless explicitly
configured otherwise.

### Delayed historical candles and late-session restarts

Historical candles and current quotes are separate broker data products. A
recent quote does not prove that the historical endpoint has published every
completed five-minute candle.

When only the end of an otherwise valid historical series is missing, startup
makes at most **one additional read for the missing interval**, bounded to the
last 15 minutes. It keeps the original requested boundary, excludes open
candles and validates the combined history. It never fills a hole with a flat
price, zero volume, an index candle or invented data.

If the data is still missing, the dashboard reports **Waiting for broker
candles**, naming the stock, required coverage and latest available coverage.
The controller rechecks after 15 seconds while startup remains authorized and
within its session rules. The same classification is carried through a worker
startup failure instead of presenting it as a generic trading block. A newly
discovered stock with pending history is deferred with an explicit reason;
existing valid symbols are not evicted just to conceal the missing history.

Bad prices, duplicated/out-of-order bars, internal gaps, wrong-session data,
permission errors and unresolved ownership remain genuine blockers. Waiting
does not authorize entries or override an existing risk halt. The missing
broker data may remain unavailable; no retry can guarantee publication.

For a **flat restart after the configured entry cutoff (normally 14:30 IST)**,
the app first checks broker positions and working orders, then reports
**Entry window closed** instead of requesting irrelevant entry-history warm-up.
The normal after-hours state remains **Market closed / waiting**. An owned
position or working order does not take this flat-account shortcut; its existing
recovery path remains required. Capital, losses, order counts and entry hours
are unchanged, and these checks add no LLM generation calls.

### Global cues: actual coverage, not an exhaustive promise

Pre-market preparation now also collects the US Federal Reserve's official
monetary-policy releases and ECB official releases, alongside existing NSE
issuer announcements, RBI releases and completed broker price history.
Recent material monetary-policy releases can create a conservative
09:15-09:45 opening blackout. They are not interpreted as certain buy signals.
The snapshot is cached for 30 minutes and contains source timestamps, recent
headlines, availability and coverage gaps. This adds no LLM polling or paid
search loop.

**Agent activity -> Global & pre-market context** shows what was actually
available. There is no licensed live GIFT NIFTY, overseas index/futures, crude,
FX or US-yield quote feed in this version. Public macro-download endpoints
tested during development were unavailable; no access restriction was bypassed.
Official releases are not a complete scheduled-event or geopolitical calendar.

Missing optional global context is reported, not replaced with invented facts.
The existing mandatory NSE/RBI feed requirements remain. No agent can guarantee
all available information, perfect correlations, or that no profitable window
is ever missed. Broader context and earlier preparation are research changes,
not proof of improved returns.

The normal session is weekday NSE cash trading. Holiday/closed-market quotes,
missing historical coverage and insufficient liquidity cannot qualify as fresh
trading opportunities. Special sessions are not automatically supported.

### Daily watchlist refresh and opportunity ranking

At the first eligible startup **each trading date**, the system obtains current
constituents/instrument metadata and fresh quotes instead of blindly reusing
yesterday's list. It computes a new list even if yesterday's session ended with
zero trades. The refreshed date, time and candidate-universe count appear on the
watchlist panel. The ranking factors and history dates are retained in the plan.

Daily ranking combines:

- Stock performance relative to NIFTY over the previous **5 and 20 completed
  sessions**, plus today's relative strength and direction from the open.
- Current traded turnover versus the stock's recent average, adjusted for
  elapsed session time. This is a **linear pace estimate**, not a claim to model
  the market's full time-of-day volume curve.
- Spread cost, ordinary liquidity, whole-share affordability and ATR volatility.
- Recent issuer announcements matched by company/symbol. Observed results,
  board-meeting, default, suspension and other material risks exclude a candidate.
  Limited order/contract-win context can contribute a small capped ranking
  factor only after at least 30 minutes; a headline never directly triggers a buy.
- Industry diversity, so the five-slot watchlist is not filled by one sector.

The initial quote screen covers up to the available NIFTY 200 cash members.
Only the top 30 liquid/affordable, non-excluded candidates require daily-history
requests, plus the benchmark. Completed history is cached by **date, instrument
token and ranking version**. After those reads, current quotes and event context
are checked again. This work is deterministic and uses no LLM tokens.

A candidate's historical session must match the benchmark, and the historical
last close must agree with the current quote's previous close within the
conservative check. Mismatches, insufficient history and extreme volatility do
not silently become trading signals.

**New ranking does not guarantee new names.** If the same shares still rank
highest and meet your small funded-capital limit, they may correctly remain.
There is no random rotation or extra buying merely to create activity. A
watchlist ranking is not a prediction of maximum gain or evidence of positive
expectancy; the existing entry/exit, benchmark, cost and risk gates still apply.

Same-day restarts keep the initial selection and original refresh timestamp,
and restore the recorded intraday admissions. They do not reset risk. If owned exposure carries into
another date, the old plan is retained only for recovery. Once a normal new
session is eligible and flat, a new list is built; no position is abandoned to
make room for another ticker.

Daily selection archives are stored under the account's
`watchlists\YYYY-MM-DD.json`; completed-history caches are under `history\`.
An upgrade during a session does not replace the initial seed selection. The
intraday-discovery path below can extend its monitored universe during that session.

### Intraday discovery: look beyond the initial picks

The first five names are now a starting point, not the only possible trades.
The managed live worker performs a **read-only batched quote scan of the current
NIFTY 200 candidate universe every 60 seconds**, from 09:20 until the unchanged
14:30 entry cutoff. It also monitors issuer news across that universe, rather
than fetching news only for the initial picks.

New issuer events can bring the next scan forward, but scans are never closer
than **30 seconds**. Event detection still depends on the news source's delivery
and polling cadence; this is not a tick-by-tick full-exchange HFT scanner.
Each scan compares current spread/turnover and market-relative strength with
recent completed history, price movement since the previous scan, an estimated
turnover acceleration, and issuer-event context.

Discovery has no order method and its broker client has order routes disabled.
An interesting stock is only a *candidate*. Before it enters live monitoring:

1. Its current constituent and broker instrument records must resolve to a
   cash EQ share with valid tick size and circuit bands.
2. Liquidity, affordability, history/price-basis consistency, volatility,
   issuer exclusions and active event pauses must permit consideration.
3. Actual contiguous completed intraday candles must warm its indicators.
   Stale scans and crossed warm-up boundaries defer admission; no candle is
   fabricated and no historical signal is replayed as a fresh trade.
4. Its admission is committed to the account ledger before subscribing.
5. Fresh streamed quotes and a **new** completed-bar entry signal must pass
   the existing funding, cost, risk, news, ownership and order-count gates.

This can produce orders in stocks outside the original five. It does **not**
increase the allocated capital, borrow funds, add derivatives/short selling,
raise the daily-loss limit or bypass the maximum entry-attempt count.
The requested increased aggression is implemented as **broader and more frequent
opportunity discovery**, not a promise to buy every mover.

Resource and churn limits:

| Control | Behavior |
|---|---|
| Quote universe | Current NIFTY 200 candidates plus the reference index; documented fallback universe if the source is unavailable |
| Full streaming pool | At most **15 cash stocks**, plus the reference index |
| New admissions | At most **two per scan** |
| History work | At most **three uncached stock daily-history reads per scan**, plus benchmark/cache access and bounded candidate warm-up |
| Daily scanning | At most **600 scans**, persisted across same-day restarts |
| Rotation | Initial picks, owned shares and symbols with active orders are pinned; unowned additional names may be replaced after at least ten minutes when they no longer rank in the desired group |
| LLM usage | **Zero calls for price scanning, ranking, admission or rotation** |

New-history acquisition is prioritized by the current quote screen, movement
and newly seen issuer events; it progresses through candidates over successive
scans rather than downloading every history repeatedly. Daily history caching
and the original AI request/token/spend caps remain in force. Headlines outside
the active trading pool do not independently consume optional-AI calls.

News about an untracked candidate can still be logged and create a
symbol-specific material-event pause before the stock is admitted. Promotion
cannot erase that pause. The required full-universe news check is completed
before first admission, and later required-feed failures still prevent trading
through the existing freshness rules.

The **Intraday opportunity discovery** panel shows the scan universe, last
scan time, trigger, current liquidity count, tracked count and new admissions.
The stock list marks **INTRADAY DISCOVERY** entries. Its latest candle decision
explains whether the stock has an actual signal or is waiting/rejected.
The audit includes `DISCOVERY_SCAN`, `DISCOVERY_ADMITTED`, `DISCOVERY_RETIRED`,
deferred/failed subscriptions and discovery errors.

Discovery failures are explicit; stale results are not displayed as fresh scans
or used for admission. Existing positions continue under their own order/risk
manager. No owned stock is evicted merely because another stock ranks higher.
Admission records and active subscriptions persist in `live.db`; on restart,
owned dynamic symbols are recovered even if absent from a later discovery list.
Do not delete that ledger or run a second host against the same account.

Coverage remains limited to the configured liquid-cash universe, actual data
entitlements, supported setups and the entry window. Broader scanning does not
prove profitable expectancy or guarantee capture of every opportunity.

### What a news-feed block means, and how recovery works

NSE corporate announcements and RBI releases are required inputs. New entries
require a sufficiently recent, fully verified snapshot from each source. A
permanent validation failure or an expired snapshot blocks entries while the
worker can keep monitoring prices and managing already-owned risk. It is not
an exchange rejection of a trade or proof of a broker/API-key problem.

The managed collector now:

- Processes the complete received RSS document, bounded at two megabytes and
  5,000 items, rather than silently inspecting only its first 250 announcements.
- Uses ETag/Last-Modified conditional requests. A `304 Not Modified` can
  revalidate a previously parsed document, but a failed request does **not**
  turn cached data into a fresh heartbeat. Document-age checks still apply.
- Retries a timeout, incomplete response, selected HTTP 5xx errors or malformed
  XML at most **twice**, after 1 and 3 seconds, requesting a fresh complete
  document. Eligible transient failures shorten the next poll to 30 seconds;
  normal healthy operation remains on the two-minute cadence.
- Does not bypass HTTP 403/access challenges, retry HTTP 429 immediately,
  synthesize news, disable source requirements or use unlimited web searches.
- Preserves the last successful timestamp and shows the actual error code,
  source, HTTP status where available and retry count. Source/recovery events
  appear in the audit timeline.
- Processes a healthy source independently: an NSE outage does not discard an
  RBI event.
- For an unfinished NSE publication or a transient transport/server failure,
  retains the **original** last-verified heartbeat only until its already
  configured **180-second maximum age**. No new heartbeat is emitted for the
  failed source. This is explicitly shown as **Live - news retry**, with the
  actual source error and last-success timestamp, rather than healthy news.
- At expiry, or immediately for access denial, malformed complete XML, invalid
  timestamps or other non-transient failures, blocks entries. A cold startup
  without any verified snapshot is not allowed to use this grace path.

A captured NSE response returned HTTP 200 but ended halfway through a
`<pubDate>` value, with no closing RSS tags. The publisher's declared ETag size
matched the received unfinished body; this was not the app's byte limit.
The app identifies that case as `incomplete_snapshot`. It does not append
imaginary closing tags, invent a date or accept partially parsed announcements.
The last complete verified document remains the only permitted fallback, within
its original freshness deadline. Persistent upstream failure must still block
entries; no client can guarantee publisher availability.

### Autonomous decisions, with explicit limits

The dashboard uses a **balanced participation** profile for opening-range
breakout, VWAP pullback and volume-confirmed intraday momentum breakout. It
seeks more qualifying entries than the older selective profile without
increasing capital, position-size or modeled loss limits. These are research
hypotheses, not a proven profitable or expert-human-equivalent strategy.

**Relative-strength alignment:** the index-above-open path remains available.
If NIFTY is flat or below its opening reference, a stock is no longer rejected
solely for that reason. The alternate path requires the stock to be up from
its own open, outperforming the benchmark by at least 20 basis points, above
VWAP and EMA20, with rising recent completed closes. The benchmark must have
three recent completed bars and must not be falling more than 50 basis points
over that recent interval. Quote-freshness checks are unchanged.

The balanced profile also permits a **recent relative-strength recovery**:
the stock may still be below its session open, but its last three complete
five-minute bars must show a positive return and rising closes, at least
10 basis points of outperformance over the benchmark's matching interval,
and a close above VWAP and EMA20. The benchmark cannot have fallen more than
50 basis points in that interval. Gaps, partial bars and stale index quotes
cannot satisfy this route; a rising price alone is not a signal.

**Momentum breakout:** after at least 12 complete five-minute bars, a bullish
bar must close above the previous three completed bars' high, above VWAP/EMA20,
with volume at least 1.2 times the prior three-bar average. This can recognize
a continuing trend that never pulls back to VWAP. A structural stop is derived
from the breakout bar/base; the ordinary cost-aware sizing and risk gate can
still reject the candidate. There is no immediate buy just because a stock has
already risen.

The opening-range setup retains its minimum 1.5x opening-range volume
confirmation. VWAP pullback retains 20 complete bars and now requires 1.2x
confirmation volume. The old selective continuation profile requires 20 bars
and 1.5x volume; the manual CLI still defaults to its original selective rules.

**Trade economics:** the balanced profile still uses a target at three times
the structural stop distance. After modeled round-trip fees, its target net
must be at least **1.0x the planned stop-limit loss including fees**, and at
least **2.0x modeled fees**. The older thresholds were 1.5x and 3.0x respectively.
This deliberately admits some smaller, cost-efficient moves; it does not
predict their win probability. A lower reward/risk threshold can increase
losses or require a higher hit rate to break even. Fee-dominated, unfunded and
over-budget trades remain rejected; targets are not moved farther away just
to make a failing calculation pass.

The specific published signal-profile upgrade can be adopted on a paused,
flat restart without changing the day's watchlist/opening window or resetting
losses, cash or entry counters. Owned exposure/quarantine cannot be bypassed to
upgrade. The manual CLI's default absolute-benchmark profile remains unchanged.
Changes are versioned through the code/test fingerprint and audit trail.

Each completed stock candle now records **SIGNAL_EVALUATED**: the market and
setup conditions, failed checks, selected setup and whether an entry was
attempted. If a qualifying signal is rejected by risk/execution checks, that
reason is stored too. The watchlist and activity timeline show the latest
explanation. There is no LLM charge for these checks. Historical periods before
this instrumentation cannot be reconstructed exactly from the old event log.

**Entry opportunities & blockers** summarizes today's entry-window candle
evaluations, qualified signals, entry plans and common failed checks. Multiple
checks can fail on one candle, so their counts are not probabilities. New
execution rejections also show the one-share cost/risk versus the actual budget,
or the modeled target, loss and fee calculations. It makes no profitable-trade
claim about rejected signals. A market-wide event pause is shown explicitly
even when the market connection itself is live.

The hard cash and loss controls remain unchanged:

| Control | Automatic policy |
|---|---|
| Allocation | Lesser of usable funded cash and **INR 25,000** |
| Replenishment | No automatic increase after deposits/profits; losses reduce usable allocated cash |
| Product | Fully funded NSE cash longs, `CNC`; no short selling, futures/options or leverage |
| Simultaneous exposure | One position at a time |
| Position notional | At most 25% of the frozen allocation |
| Cash buffer | 10% |
| Per-trade modeled risk | 0.25%, including modeled costs and stop-limit allowance |
| Daily loss/giveback threshold | 0.75% |
| Entry attempts | At most three per session |
| Consecutive nonpositive trades | Stop after two |
| New entries | End at 14:30 IST |
| Scheduled flatten | Request exits from 15:10 IST |
| Funding | No bank, withdrawal, top-up, loan or money-transfer interface |

These are **planned/modelled thresholds, not guaranteed maximum losses**.
Stop-limit orders can fail to fill, execution can slip or be rejected, and a
position can remain open through an outage or market close. The dashboard
will not claim that a stop request proves the account is flat.

No trades is still a legitimate outcome. The system does not force a daily
trade quota, switch to riskier products, bypass hard safety checks or increase
size to meet a daily profit target.

Cash already loaded into the broker account before first startup can be used.
Kite can report today's deposits in `opening_balance + intraday_payin` while
leaving its separate `cash` field at zero. The app accepts the greater of those
two funding bases; it never adds the pay-in to an already-inclusive `cash` field.
Negative opening balances are preserved. The result is capped by both
`live_balance` and `net`, with positive ad-hoc margin excluded, so funds blocked
by the broker or supplied as additional margin do not become trading capital.
Collateral and derivative-margin checks remain enforced.

The startup minimum remains INR 1,000. An insufficient-cash message includes
the amount the app actually calculated. Passing that minimum does not guarantee
that a whole share meets position-size, trading-cost or risk requirements.
This does not initiate a deposit, and subsequent pay-ins do not replenish the
engine's frozen allocation.

### Bounded AI use

The automatic profile uses **Gemini 3.1 Pro Preview**, low thinking level and
structured JSON. A key/model check is not proof that paid generation will
succeed; billing, model availability and provider limits still apply.

Ordinary price monitoring, indicators, sizing, exits and reconciliation use
**zero LLM tokens**. Only selected public news is eligible for AI classification.
Its only possible action is an additional pause, never an order or a risk override.

If optional AI times out, is rate-limited, returns malformed output or otherwise
fails, the event is recorded as **AI failed / no verdict**, including a bounded
error category/HTTP code where available. It no longer invents an extra
15-minute market pause solely because that optional service failed. Existing
deterministic high-impact-news pauses remain unchanged, and all data/signal/risk
checks still apply. This is not represented as a successful model assessment.
Reserved AI budget is retained and there is no automatic generation retry.

Recognized official RBI **money-market operations summaries**, standard
**VRRR notices/results**, **government-security underwriting notices**, and
**Government Stock - Auction Results with NIL dealer devolvement** are recorded
as operational context rather than automatically pausing every stock because
the text contains "RBI" or "results". This is source/title-specific, not an
assertion that liquidity operations can never move prices. Such routine
releases do not consume the optional AI budget.

Emergency/policy/CRR/MPC changes, distress, non-NIL devolvement, other publishers
and explicitly high/critical incoming events retain their normal treatment.
An old false pause can be corrected only against its exact, non-truncated
audited routine release while flat/reconciled and without any other active global pause.
Ordinary material-event pauses end relative to the publication time, so an
updated description does not restart the same reaction window. Critical
incoming events still receive a fresh full pause from receipt time.

The profile permits at most two calls/day, 8,000 reserved tokens/day, a
30-minute cooldown, a 20-second request timeout and an estimated USD 0.10/day
spend ceiling. Up to 2,048 generated tokens are reserved per request, including
the allowance needed for Pro thinking. Failed/uncertain requests retain their
reservation. There are no automatic inference retries or paid web-search loops.

Gemini request bodies contain selected public headlines and original local
knowledge excerpts, not account balances, positions, broker credentials or your
private files. Provider prices/billing can change; the local cost estimate is
not a provider-enforced billing limit.

## Read the dashboard correctly

| State | Meaning |
|---|---|
| Waiting for configuration | Required credentials are not saved |
| Validating / checking software | Authentication and local release checks are in progress |
| Pre-market research ready | Research is prepared, but normal cash trading has not opened; no live order worker is running |
| Broker sign-in required | Missing/expired broker session or account permission problem |
| Preparing / warming up | Stock selection, feeds, history and indicators are loading |
| Waiting for broker candles | A required historical interval is not yet available; bounded automatic recheck, no invented bars or new entries |
| Entry window closed | Past the configured entry cutoff and broker-confirmed flat; no unnecessary entry warm-up or catch-up trades |
| Live engine | A running worker has a fresh eligible live snapshot; actual orders still require every strategy/risk gate |
| Live - news retry | A transient source failure is being retried; the original fully verified snapshot remains within its unchanged freshness deadline |
| Trading blocked | A risk, data, ownership, execution or authentication condition prevents new entries |
| RECONNECTING | A transient broker-stream failure ended flat and reconciled; a bounded fresh-worker retry is pending |
| Market closed / waiting | Waiting for the supported market session |
| Recovery only | Managing previously owned exposure, not opening new positions |
| Stopping & reconciling | Exit/cancellation requested, but flatness is not yet confirmed |
| Stopped by you | Explicit stop retained across launches |
| Dashboard disconnected | No fresh local status; inspect the broker if any exposure may remain |

**Saving credentials alone never produces a green LIVE badge.** The marker
depends on an actually running worker and recent runtime/broker/feed evidence.
It also changes when the heartbeat becomes stale.

The top-right **KITE account badge is independent of trading readiness**.
After a successful authenticated broker-profile response, it shows a masked
client ID even if the cash or market-preparation checks block startup. It
does not require a trading ledger or a placed order. Saved but not yet verified
credentials show **ACCOUNT NOT VERIFIED**, not **NO ACCOUNT**. Identity is
bound to the current broker API key/session token; replacing them requires a
fresh verification, and an expired/rejected session is marked for sign-in.

The panels show:

- Today's net P&L, realized closed-trade P&L, open exposure and frozen allocation.
- Actual owned positions, planned stop triggers and targets.
- Agent transactions/fill deltas, completed trades and order/protection states.
- Equity samples, selected stocks, ranking reasons and the most recent
  completed-candle trade/no-trade explanation.
- Data-source health, decision/rejection timeline and AI reservations.

Fees and P&L are **modeled estimates**, not a contract note or tax statement.
The dashboard excludes AI/data subscriptions, hosting and income tax; displayed
trading P&L is not an all-in business-profit calculation.
Open P&L can use a last observed quote; the timestamp/stale marker matters.
The transactions are this agent's owned fills, not a complete import of
unrelated manual activity. Use a dedicated account and avoid other trading
applications using it simultaneously.

## Persistence and restart

Application state is kept under:

```text
%LOCALAPPDATA%\IndiaTradingAgent
```

Important contents:

- `credentials.dat`: DPAPI-encrypted API credentials and saved live authorization.
- `ui-session.dat`: encrypted temporary browser-launch capability.
- `broker-connection.json`: masked, session-bound broker identity metadata;
  no API credentials or account balances.
- `accounts\<account-hash>\live.db`: persistent orders, fills, risk, cash, AI
  budgets and the current runtime snapshot. SQLite WAL allows dashboard reads
  alongside status updates without replacing a file every second.
- `accounts\<account-hash>\watchlists\`: dated selection/ranking archives.
- `accounts\<account-hash>\history\`: daily-only completed-candle metric caches.
- Account plan and redacted worker log in the same account folder. Legacy
  `runtime.json` files may remain from older releases but are no longer read
  or rewritten as live status.
- `PAUSED`: persistent explicit stop marker.

On an ordinary subsequent launch, saved credentials are reused and the
controller automatically prepares/starts during the supported session. A broker
token that expired overnight still requires official broker sign-in.

An explicit **Stop & flatten** withdraws automatic-start authorization until
you select **Resume live** or explicitly save/authorize again. Restarting the
application does not undo a stop, erase losses, clear a quarantine, or replenish
capital. The initial daily seed plan changes on a new session while flat.
Additional intraday memberships can change without restarting, but owned
positions/working orders are pinned and all account/risk limits remain unchanged.

Do not delete or replace ledgers to make a risk stop disappear. Do not run
multiple hosts against the same account. Local process/account locks prevent
accidental duplicate processes on this machine, not across multiple machines.
Stop, confirm flatness and back up the ledger before upgrading the application.

Credential replacement/removal is blocked while a worker or unresolved owned
exposure is active. Broker-session renewal for recovery is restricted to the
original account. If a token expires mid-session, the worker stops with its
ledger intact; the UI offers official broker reauthentication rather than
blindly retrying orders.

## Stop and recover

Use **Stop & flatten**, then confirm the order/position state in the dashboard
**and the broker terminal**. The stop mechanism does not depend on an AI answer.
An already accepted broker stop may still be working.

If the feed, broker API, disk or process fails, the app cannot guarantee an exit.
For unknown submissions or stop/cancel races, inspect the broker order book
before any manual sell. Do not create a duplicate exit or remove recovery
credentials while exposure is unresolved.

Do not close the application while a position needs active management. A
graceful application shutdown requests flattening and waits for the worker,
but killing the process/turning off the machine removes that supervision.
No out-of-band pager, distributed failover or independent brokerage risk
supervisor is provided by this version.

### Broker WebSocket interruptions

WebSocket **1006** means the data connection ended without a normal closing
handshake. It can result from a network interruption or the remote connection
being dropped; it is not itself an order rejection or proof that the API key
or static-IP whitelist is wrong.

The app records `STREAM_ERROR` with the sanitized code, reason and connection
generation. It immediately blocks new entries, invalidates pre-disconnect quote
readiness and continues broker order/position reconciliation. A reconnect does
not replay an unconfirmed order or treat missed candles as complete.

When an interrupted worker has **no position or active order** and has completed
a broker reconciliation after the fault, it can finish and be restarted by the
controller. The retry uses the same keys, ledger, frozen capital and risk
counters. It reloads actual completed history and requires at least three
seconds of fresh multi-instrument samples plus post-connect broker
reconciliation before clearing the exact transient-stream halt.

The SDK's reconnect attempts are bounded, and automatic fresh-worker restarts
are capped at **three within a rolling 15-minute window**, with increasing
delays. Repeated failures beyond that remain blocked for inspection. Invalid
authentication/token indications request broker sign-in; protocol/policy
failures are not assumed to be harmless transient drops. An explicit operator
Stop remains authoritative.

If exposure exists during the outage, its native accepted protection stays at
the broker. On returned fresh data, the engine manages the pending exit under
the existing order-ownership rules; it does not resume new entries by wiping
the halt or abandoning the position. Unknown submissions, quarantine, clock
faults and daily-loss halts remain enforced.

Dynamic subscribe/unsubscribe and socket-close commands are now dispatched to
the WebSocket's owning Twisted event-loop thread. These commands never place
trades. A retired symbol is removed from the SDK's intended resubscriptions
even if the connection is currently down.

The older generic `"Broker WebSocket reported an error."` halt can recover on
an explicitly authorized restart, but only after fresh-history/quote and
flat/reconciled checks. Merely reconnecting the TCP socket or refreshing the
dashboard does not establish readiness.

### Broker reconciliation read outages

Reconciliation reads the broker's order book, positions and cash. A temporary
timeout, dropped connection, incomplete response or retryable read-only HTTP
error must stop **new entries** until current ownership/funding is verified;
it is not proof that an order failed or that the account has insufficient cash.

Transient GET failures now report the exact endpoint and a sanitized category
(timeout, DNS, connection interruption, partial response or HTTP status).
Rechecks back off after failure at 2, 4, 8, 16 and up to 30 seconds, respecting
numeric broker `Retry-After` delays when longer. Order-update events do not
bypass that backoff. Each attempt is a fresh read batch, not a retransmitted
order, and stale/future-dated batches do not establish readiness.

Once a fresh complete batch agrees with the owned ledger, read readiness
recovers and `BROKER_RECONCILIATION_RECOVERED` is recorded. A temporary read
outage does not permanently latch a strategy halt. The exact older
`Broker reconciliation failed: Broker transport failure; reconcile before action.`
halt can be removed on a verified flat reconciliation, without changing capital,
loss/trade counters or ownership records.

Accepted broker-native protection is not cancelled while reconciliation data
is unavailable merely to replace it with a new exit. Pending entry cancellation
can still be requested under existing rules, but uncertain POST/DELETE outcomes
remain UNKNOWN and are never blindly resubmitted. Native orders are not
guaranteed fills; inspect the broker directly if connectivity remains unavailable.

Authentication/permission failures, TLS validation problems, malformed broker
data, unexpected positions and order mismatches remain explicit blockers.
Neither certificate checks nor stale-data/ownership restrictions are bypassed
to recover from a read failure.

### Owned-position quote gaps

The application treats a quote as executable only within the existing
three-second quote-age limit (and the unchanged one-second future-time limit).
That freshness rule has not been loosened. The timestamp remains the exchange
timestamp, not the time a stale packet happened to be processed.

The latest received full quote is now tracked independently of the ordered
indicator/candle queue. The main thread refreshes these risk quotes before and
after broker reconciliation/order work and before position-feed checks. A
slower synchronous broker operation must not make an already-arrived fresh
quote invisible while older candle events are still queued. Quote refresh by
itself does not evaluate an entry signal or fabricate candles.

If an owned position briefly lacks a fresh quote:

- New entries remain gated. Existing accepted broker protection is left
  working; the first stale observation does not automatically force a sale.
- A dedicated read-only client can request `GET /quote` for that owned symbol,
  with one request outstanding and a minimum five-second cadence. Repeated
  failures back off to 30 seconds, respecting longer broker retry delays.
- A returned quote must match the instrument and current trade, contain
  executable bid/ask depth, fit price bands and satisfy the original exchange
  time/freshness checks. A delayed reply for an already-closed/replaced trade
  is discarded.
- That REST snapshot can inform the owned position's risk/exit logic only.
  It cannot refresh indicator history, satisfy the entry stream gate, or create
  a new entry signal.

A sustained missing/stale position quote of **15 seconds** still latches the
data-outage exit intent. It does not cancel protection or submit a replacement
exit at a stale price; fresh executable data and confirmed order ownership are
still required. Actual stop, target, holding-time and daily-risk exits retain
their existing rules and can take precedence.

Cancelling protection for a replacement exit also requires a reconciled broker
snapshot no older than 15 seconds, measured from when the read batch began.
Finishing a slow read does not make old ownership data fresh. A newly
acknowledged stop must be reconciled before it can be replaced.

`POSITION_FEED_STALE`, `POSITION_FEED_RECOVERED`, `POSITION_QUOTE_REFRESHED`,
`POSITION_QUOTE_REFRESH_FAILED` and `POSITION_QUOTE_DISCARDED` provide measured
quote time, age, receipt/source and failure evidence. A known transient warning is no longer
left permanently displayed after the position closes: only the exact
position-feed halt can clear after a fresh, flat, non-quarantined broker
reconciliation without an outstanding clock/authentication fault. Daily risk
is checked again, and cash, losses, completed trades and entry counts are not reset.

The earlier generic stale-feed warning did not retain enough receipt-time
evidence to prove whether every historical gap was a network delay or local
processing backlog. This repair addresses the confirmed processing/latched-halt
failure paths; it does not promise uninterrupted feeds or guaranteed stop fills.

### Worker exits and local file contention

The dashboard displays the current worker's redacted `STOPPED:` reason when
one is available, instead of always asking you to find the log. The reason is
cleared before a new worker launch so an old failure cannot label a later run.

An older release wrote `runtime.json` every second. On Windows, an overlapping
dashboard read could prevent the file replacement and raise `PermissionError`.
Live status now uses the existing SQLite ledger, not that replaceable JSON file.
This change does not reset positions, cash, risk limits or AI budgets.

Less frequent atomic metadata/credential writes retry only specific transient
Windows sharing/access errors, with five short delays totaling at most 0.31
seconds. Persistent permission, lock or storage failures still fail explicitly;
the app does not change file permissions, disable security software, discard
the previous valid file or pretend a failed write succeeded.

### Future exchange timestamps and clock recovery

The exchange timestamp must not be more than **one second ahead of receipt
time**. Receipt is captured when the WebSocket callback arrives, not after the
tick waits in the processing queue. Rejected future ticks do not update candles,
VWAP or executable-quote state. The audit records the measured lead and both
timestamps so a future fault can be diagnosed precisely.

Keep Windows **Set time automatically** enabled and the approved Windows Time
service running. A running service is not proof of synchronization: use
Settings -> Time & language -> Date & time -> **Sync now**, or verify
`w32tm /query /status`. A service-start/resync may require administrator access.
`Sync-WindowsClock.ps1` requests synchronization through the existing configured
Windows source when run by an administrator. It does not change the time-server
policy, elevate itself or bypass management restrictions.

After correcting the clock, explicitly resume/restart the paused agent. Only
the exact previous clock halt can recover automatically, and only while flat,
reconciled, not quarantined and receiving fresh valid samples from **every**
subscribed instrument over at least three seconds. Risk halts, position ownership,
losses, the cash allocation and order state are not reset. A new clock fault
within that run stays latched for review.

Do not increase timestamp tolerance, reinterpret a broker Unix timestamp as a
different timezone, or replace an exchange time with the PC time to conceal
clock drift. The installed SDK's naive timestamp is host-local; the adapter
preserves the underlying instant when converting it to IST.

## Automatic mode versus the older manual workflow

The original `run --mode live` command still enforces its manual research/
qualification and session-file gates.

The new dashboard is a **separate, explicit opt-in autonomous-live workflow**
requested for credentials-only operation. It automates selection/session
preparation and uses saved authorization plus software, broker, ownership,
cash, data and execution gates. It **does not** create fictitious backtests,
claim that synthetic profits prove an edge, or manufacture a broker approval.
It does not wait for the old manually supplied 30 replay/20 shadow reports.

This distinction is significant: automated startup is not proof of economic
viability. No positive live expectancy, guaranteed income, exhaustive news
coverage, or expert-human-level judgment has been established.

Use the retained replay/shadow tools and independent review before risking
meaningful capital. The system has only been exercised with offline/mocked
execution scenarios, public feeds and read-only broker diagnostics during
development. Those checks do not establish profitable live performance.

## Source and entitlement boundaries

Automatically configured public sources:

- NIFTY 200 constituents:
  `https://www.niftyindices.com/IndexConstituent/ind_nifty200list.csv`
- NSE announcements:
  `https://nsearchives.nseindia.com/content/RSS/Online_announcements.xml`
- RBI releases:
  `https://www.rbi.org.in/pressreleases_rss.xml`
- Official global policy context:
  `https://www.federalreserve.gov/feeds/press_monetary.xml`
  `https://www.ecb.europa.eu/rss/press.html`
- Quotes, depth, instrument metadata and historical candles: your licensed
  Kite Connect data entitlement.

The public CSV/RSS endpoints and their formats were exercised with ordinary
requests during development. No CAPTCHA/cookie/access-control bypass is used.
A required-source error blocks entries; the app does not invent a heartbeat.

RSS is not exhaustive, guaranteed timely market intelligence. The bundled
knowledge is a set of original operational notes, not every prior trade or
all licensed historical data. Signals can be wrong, public announcements can
arrive late, and losses are possible despite all controls.
