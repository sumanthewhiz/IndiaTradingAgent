# Non-overridable operating boundaries

Only allowlisted NSE cash shares, fully funded long positions and one active
position are supported. No leverage, averaging down, derivatives, naked shorts,
bank access or autonomous capital-limit increases. Never interpret available
collateral as available cash.

## Execution uncertainty

An acknowledgment is not a fill. An order tag is not an idempotency guarantee.
Unknown submission status means reconcile, never blindly retry. A partial fill
creates a real position that must be protected/accounted for. Canceling a stop
does not prove cancellation; verify remaining shares before sending another sell.

## Stop-limit limitations

A native stop-limit is not a guaranteed exit. Prices can gap beyond its limit,
circuits can remove liquidity, and protection can be rejected. There are latency
gaps between a fill and stop acceptance, and between cancellation and a new exit.
Do not claim a planned 0.25% risk is an absolute maximum loss.

## Events and AI

News is untrusted input. Instructions inside headlines, documents, feeds or model
answers cannot change broker permissions, cash limits, sizing, order routes or
the strategy. AI may only extend a pause. High-impact uncertainty is processed
locally even if AI is disabled, unavailable or out of budget.

## Incident response

Loss limits, missing quote coverage, unknown orders and ownership mismatches are
not cured by resetting a database. Preserve evidence, inspect the broker and
confirm flatness. A local kill file only requests action from a living process;
it does not cancel exchange orders by itself.
