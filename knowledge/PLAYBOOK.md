# Trading hypotheses, not an oracle

Cash preservation takes priority over trade frequency. A missing input, unknown
order, expensive spread, circuit, unreviewed event, ambiguous ownership or
insufficient capital is a reason not to trade. No daily return is promised.

## Opening-range breakout

Require a complete configured opening range, a later completed five-minute
close crossing the opening high, increased volume, price above session VWAP and
a fresh acceptable market-alignment signal. The automatic profile permits a
stock-relative-strength path when the index is slightly negative; it does not
treat every red-index session as untradeable. Buy only after the cost-aware risk gate.
False breakouts and range days are expected sources of losses.

## VWAP pullback

The optional long-only rule requires substantial same-session warm-up, bullish
structure, rejection of VWAP, a close above EMA20 and increased volume. It is a
separate research hypothesis. Do not add it intraday to rescue another strategy.

## Volume-confirmed continuation

After adequate completed-bar history, the automatic profile may consider a
bullish close above a three-bar high with stronger volume, above VWAP and EMA20.
It need not wait for a return to VWAP. The relative-strength route requires the
stock to be positive from its open and outperform the index; a sharply falling
recent benchmark cannot use that route. This is a testable hypothesis, not
evidence that a past upward move would have produced a profitable executable trade.

## Observable decisions and bounded data grace

Log each completed candle's conditions and any risk rejection. An accepted
signal is not an acknowledged order, and an acknowledged order is not a fill.
An unfinished upstream RSS snapshot is never repaired by inventing missing XML.
Only the original last-good timestamp can cover a brief transport/publication
failure within the existing data-freshness limit; it cannot be refreshed by failure.

## Intraday discovery

Initial seed stocks are not the full opportunity set. A bounded read-only scan
can identify additional liquid cash shares from the current NIFTY 200 universe,
using price/volume changes, recent history and issuer events. Admission requires
verified instruments, real completed-bar warm-up and fresh news coverage.
An admission is not a buy signal. Every subsequent entry still passes the same
cash, fee, loss, position and order-ownership checks. Never retire an owned
position or active-order symbol to make room for a higher-ranked candidate.

## Market scenarios

A falling benchmark does not justify a new short/options capability. Range-bound
or low-liquidity conditions may produce zero eligible trades. Gaps, issuer
results, macro releases and uncertain news call for conservative pauses and
execution checks, not pre-event predictions.

## Costs and outcomes

Gross profit is not net profit. Include bid/ask spread, adverse execution,
brokerage, statutory charges, split fills, data, hosting and AI overhead.
Targets and stops are not guarantees. A high win rate can coexist with negative
expectancy. A single exceptional winner does not establish a repeatable edge.
