# Data contracts and provenance

All timestamps must carry a timezone offset. The engine operates in IST
(`+05:30`, no daylight-saving adjustment). Prices in input CSV are rupees;
internal prices, ledger values and report fields ending in `_paise` are integer
paise. Quantities are integer shares. Reference index ticks cannot be traded.

## Tick CSV

Exact header/order:

```csv
timestamp,symbol,last,bid,ask,volume,bid_size,ask_size
2026-09-21T09:15:00+05:30,INDEX,25000.00,25000.00,25000.00,0,0,0
2026-09-21T09:15:00+05:30,DEMO,100.00,99.99,100.01,1000,10000,10000
```

These are format examples, not real observations. Requirements:

- Global chronological order; multiple symbols may share a timestamp.
- One authorized session per replay file.
- `volume` is cumulative session traded volume, not per-tick incremental volume.
- `bid_size` / `ask_size` are displayed best-level quantities, not total daily volume.
- Positive bid/ask/last, ask >= bid, nonnegative volume and sizes.
- Cash prices, instrument tick sizes and circuit bands must use the same
  point-in-time corporate-action basis.
- Full opening coverage: either timely streaming from 09:15:03 or earlier, or
  actual contiguous completed broker candles through managed warm-up. A
  partially observed opening bar is not a complete signal.
- Do not fill gaps with repeated fictional quotes or copy the first known quote
  backward to manufacture the opening auction outcome.

The broker's full-mode index packets do not contain stock-style depth/volume.
The connected adapter explicitly maps reference indices to zero quantity and
does not mistake index volume for tradable liquidity.

For historical vendors, use real executable bid/ask/depth observations with the
appropriate entitlement. Candle-to-tick interpolation is not an acceptable
substitute for qualifying fill quality.

## Replay instrument master

```json
{
  "dataset_kind": "synthetic",
  "instruments": [
    {"symbol":"DEMO","token":1,"tick":1,"lower":9000,"upper":11000,"reference":false},
    {"symbol":"INDEX","token":2,"tick":1,"lower":1,"upper":1000000000000,"reference":true}
  ]
}
```

`tick`, `lower` and `upper` are **paise**, not rupees. `token` must be an integer
identifier. Exactly the configured universe plus the benchmark must be present.
The real connected adapter downloads broker metadata; these example tokens
must not be used for a live subscription.

Set `dataset_kind="licensed"` only for legitimately obtained **real** data.
This label is an operator attestation, not cryptographic proof of a vendor
license or authenticity. Reports record input file hashes to expose later
changes, not to prove that the original dataset was unbiased.

Keep an adjacent provenance record with vendor/source, entitlement, coverage
period, venue, acquisition date, timezone, corporate-action convention, missing
intervals, transformation version and whether quotes represent exchange time
or observed receipt time. Input hashes are computed incrementally, not by loading
an entire tick file into RAM.

## Event inbox JSONL

One UTF-8 JSON object plus newline per record. The writer should append whole
records and must not truncate the active file. The reader waits for a completed
line rather than consuming a half-written event.

Heartbeat:

```json
{"type":"heartbeat","source":"licensed-wire","at":"2026-09-21T09:30:00+05:30"}
```

News/event:

```json
{"type":"news","source":"licensed-wire","at":"2026-09-21T09:31:00+05:30","symbols":["DEMO"],"severity":"high","public":true,"headline":"Example material issuer announcement"}
```

`DEMO` is valid only in a configuration whose allowlist contains it.

- Sources must appear in `news.allowed_sources`.
- Every source in `news.required_sources` must supply fresh health evidence.
- Headline length: 1-1,000 characters.
- Allowed severity: `low`, `medium`, `high`, `critical`.
- `public` is an actual JSON boolean. False prevents external AI use.
- Symbols must be allowlisted cash symbols, or `["*"]` for market-wide/unknown scope.
- Publication time cannot be in the future; ordinary news older than one hour is
  not accepted for intraday reaction. Use pre-session review for older context.
- Apply explicit new events for corrections/retractions; do not overwrite history.
- Headlines are de-duplicated per session by normalized content and symbol set.

The baseline high-impact keyword filter covers only a small English vocabulary.
It is not comprehensive NLP. Your licensed mapper must identify issuer aliases,
Indian-language announcements, materiality, corrections, macro events and scope.
Unknown but material scope should conservatively pause the universe, not be
invented as a bullish trading signal.

Do not send passwords, private research, personal information, broker responses,
positions or account numbers as news. Marking private material `public=true`
does not make it safe or licensed to transmit to an AI provider.

## Scheduled events versus unscheduled events

Use the daily session `blackouts` for known no-entry windows. They are not a live
news feed and do not automatically close an existing position. For desired
pre-event flattening, deliver an affected high-impact event with adequate lead
time or halt and confirm flatness manually.

Unscheduled material news triggers a local pause and can request a managed exit.
There is no assurance that a public feed reports an event before the market
moves. Do not backtest against a story's retrospectively corrected publication
time if the trader could not have received it then.
