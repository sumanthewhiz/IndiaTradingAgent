# Indian market context and data discipline

This release assumes a normal NSE cash session, 09:15-15:30 IST. Use an explicitly
reviewed date/holiday/issuer manifest. Special sessions and derivatives have
different rules and are not implicitly supported.

Instrument identifiers, tick sizes, price bands, surveillance restrictions,
corporate actions and broker permissions can change. Download current metadata
and check issuer eligibility. The reference index is not a directly tradable
cash share and does not have comparable depth/volume fields.

Use licensed executable quotes rather than interpreting LTP as an available fill.
Cumulative session volume is not per-tick volume. Preserve exchange timestamps
and observed arrival time where available. Missing opening history cannot be
reconstructed by repeating a later price.

Macroeconomic decisions, inflation releases, issuer earnings, regulatory actions,
mergers, defaults, trading suspensions and geopolitical developments can change
liquidity and price abruptly. Sources can be delayed, wrong or incomplete. A
healthy collector heartbeat does not prove complete market knowledge.

Fees and tax obligations must be confirmed from current broker/exchange/legal
sources. Historical F&O-loss statistics are not current success probabilities
for a cash-intraday strategy.
