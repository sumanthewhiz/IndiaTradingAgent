# Sources, provenance and current-rule verification

## Clock and opening-session update (25 September 2026)

- NSE pre-open session FAQ: distinguishes the 09:00 auction from normal
  cash trading starting at 09:15:
  <https://nsearchives.nseindia.com/web/mediaattachment/2026-09/Annexure_Faq_on_Pre-open_Session_in_Equity_CM_Segment_20260903181927.pdf>
- US Federal Reserve official monetary-policy RSS:
  <https://www.federalreserve.gov/feeds/press_monetary.xml>
- ECB official press-release RSS:
  <https://www.ecb.europa.eu/rss/press.html>

Both global release feeds were reached and parsed during verification. They
are dated official policy releases, not live global price feeds or a complete
economic calendar. No licensed GIFT NIFTY/US futures/crude/FX feed is claimed.

The timestamp incident was grounded in read-only Windows time-server samples
and an authenticated broker HTTP Date comparison. Starting Windows Time required
the user's administrator action; afterward repeated NTP samples and a short
read-only Kite stream confirmed the original timestamp tolerance was satisfied.
No spoofed timestamps, time-server-policy changes or order probes were used.

## Feed recovery and daily-universe update (24 September 2026)

The managed daily universe now uses the official **NIFTY 200** constituent CSV:
<https://www.niftyindices.com/IndexConstituent/ind_nifty200list.csv>.
An ordinary public request returned 200 EQ constituents with company names and
industry metadata during verification. The earlier NIFTY 50 reference below
describes the original release, not the current default candidate universe.

The NSE RSS returned about 900 announcements and supplied ETag/Last-Modified
headers. Conditional requests returned 304 when unchanged. A separate
verification request also encountered intermittent invalid XML; the client
does not infer that every past generic error had that same cause.

No undocumented endpoint, browser-cookie workaround or access-control bypass
was introduced. Positive source revalidation and bounded document-age checks
remain prerequisites for news health. Broker history/quotes are accessed through
the existing licensed Kite endpoints. New ranking weights are transparent
research heuristics, not broker/exchange-endorsed trading advice or tested return
forecasts.

## Dashboard additions (21 September 2026)

- Official NSE announcement RSS:
  <https://nsearchives.nseindia.com/content/RSS/Online_announcements.xml>
- Official NIFTY constituent CSV:
  <https://www.niftyindices.com/IndexConstituent/ind_nifty50list.csv>
- Official RBI release RSS:
  <https://www.rbi.org.in/pressreleases_rss.xml>
- Broker history and authentication, including next-day 06:00 token expiry:
  <https://kite.trade/docs/connect/v3/historical/>
  <https://kite.trade/docs/connect/v3/user/>
- Gemini model, thinking and compatibility:
  <https://ai.google.dev/gemini-api/docs/models/gemini-3.1-pro-preview>
  <https://ai.google.dev/gemini-api/docs/gemini-3>
  <https://ai.google.dev/gemini-api/docs/openai>
  <https://ai.google.dev/gemini-api/docs/api-key>

The three public CSV/RSS feeds returned parseable data during local development.
That is not a promise of future availability, complete event coverage, or a
license to redistribute data. Real broker credentials and paid model calls were
not used for development verification.

Prepared 20 September 2026. These are reference links, not a representation that
every circular/amendment through that date was exhaustively verified.

Public search results can contradict one another, confuse versions or invent
precise details. This implementation uses official broker interfaces and
conservative restrictions, and requires current broker confirmation before live
activation. It does not rely on unofficial claims of automatic regulatory
exemption or guaranteed acceptance of a particular order type.

## Primary reference sources

1. **SEBI: Safer participation of retail investors in Algorithmic trading,
   4 February 2025.** Framework and responsibilities.
   <https://www.sebi.gov.in/sebi_data/attachdocs/feb-2025/1738665456458.pdf>

2. **SEBI: implementation timeline extension, 30 September 2025.**
   Read the actual circular and later amendments, rather than a cached blog date.
   <https://www.sebi.gov.in/sebi_data/attachdocs/sep-2025/1759232056254.pdf>

3. **NSE: Retail Algo FAQs, 3 November 2025.**
   Relevant to retail algo implementation, order/validity restrictions and scope.
   <https://nsearchives.nseindia.com/web/sites/default/files/inline-files/FAQ_Retail%20Algo_03112025_NSE.pdf>
   Related implementation circular:
   <https://nsearchives.nseindia.com/content/circulars/INVG67858.pdf>

4. **Zerodha: static IP and developer-account setup.**
   Broker support describes the static-IP requirement for API order placement
   from 1 April 2026. Reconfirm applicability and current instructions with the
   broker. Market-data access and order permissions are distinct.
   <https://support.zerodha.com/category/trading-and-markets/general-kite/kite-api/articles/static-ip>

5. **Kite Connect v3 orders, user/margins, WebSocket and exceptions documentation.**
   <https://kite.trade/docs/connect/v3/orders/>
   <https://kite.trade/docs/connect/v3/user/>
   <https://kite.trade/docs/connect/v3/websocket/>
   <https://kite.trade/docs/connect/v3/exceptions/>

6. **Official Zerodha Python client.**
   The source confirms full-mode packet fields, host-local timestamp decoding,
   WebSocket callbacks, regular order fields and optional `algo_id`.
   <https://github.com/zerodha/pykiteconnect>
   Source revisions inspected during authoring:
   `kiteconnect/connect.py` blob `652901035de2dd2f4747f3bf3be13fee8fd013c8`;
   `kiteconnect/ticker.py` blob `c907cede7918dc96403c31c5a5673eaed86c33c3`.
   These are Git blob identifiers, not pinned package releases or a substitute
   for checking the installed client version.

7. **Zerodha order-rate support information.**
   Treat applicable limits as broker-specific and subject to change. The
   prototype deliberately operates far below a high-frequency trading design.
   <https://support.zerodha.com/category/trading-and-markets/alerts-and-nudges/kite-error-messages/articles/order-rate-limits-on-kite>

8. **SEBI, July 2024: equity cash intraday study.**
   Approximately seven out of ten individual intraday traders in the studied
   equity cash population made losses. This is historical descriptive evidence,
   not a forecast or an estimate of this strategy's performance.
   <https://www.sebi.gov.in/media-and-notifications/press-releases/jul-2024/sebi-study-finds-that-7-out-of-10-individual-intraday-traders-in-equity-cash-segment-make-losses_84948.html>
   <https://www.sebi.gov.in/sebi_data/attachdocs/jul-2024/1721818140715.pdf>

9. **SEBI, September 2024: equity derivatives P&L, FY22-FY24.**
   Reports 93% of individuals in that studied F&O population incurred losses.
   This supports caution about leveraged derivatives; it is not mixed with the
   cash-intraday study or asserted to be a September 2026 success rate.
   <https://www.sebi.gov.in/sebi_data/attachdocs/sep-2024/1727085659479.pdf>

10. **Exchange and broker operational references to check daily.**
    <https://www.nseindia.com/>
    <https://www.nseindia.com/all-notifications-circulars>
    <https://www.bseindia.com/>
    <https://www.rbi.org.in/>
    <https://zerodha.com/charges/>
    <https://zerodha.com/brokerage-calculator/>
    <https://zerodha.com/varsity/>
    <https://www.incometax.gov.in/>

11. **Optional AI provider API/pricing.** Confirm the selected model supports
    the implemented request fields and enforce independent billing limits.
    <https://platform.openai.com/docs/api-reference/chat>
    <https://openai.com/api/pricing/>

## Important distinctions

- An API order acknowledgment does not establish exchange acceptance or a fill.
- A custom client tag is a reconciliation reference, not server-guaranteed
  idempotency and not an exchange-approved algo ID.
- Static IP configuration, low order rate and limit orders do not automatically
  constitute complete regulatory approval.
- This version makes no claim that a 10-orders-per-second threshold alone
  exempts every retail use from all requirements. Ask the broker about the exact
  personal algo classification and counting/tagging rules.
- Broker support for an order type in general does not prove that the same type
  is permitted for your API/algo classification today.
- Fully funded CNC orders can be closed on the same day where the broker allows;
  that does not make failed end-of-day exits or next-day delivery harmless.
- The example fees are modeling inputs. They are not verified current statutory
  rates. No option lot sizes, expiry weekdays or current cash-market price levels
  are asserted by this project.

## Knowledge maintenance

Review issuer/exchange/broker events daily, code/dependency changes before reuse,
and strategy evidence whenever signals, data, news coverage, model or costs change.
Maintain a dated record of broker confirmations and actual contract-note
reconciliation. Primary-source publication dates and retrieval dates are not
interchangeable.

No commercial data corpus, paid book, proprietary course, private research or
complete historical exchange tape is bundled or reproduced. The local knowledge
notes are original operating summaries and hypotheses, not a claim to have
absorbed every trader's experience.
