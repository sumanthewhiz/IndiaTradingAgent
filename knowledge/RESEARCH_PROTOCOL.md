# Research and learning protocol

The agent does not self-modify or automatically learn by increasing risk after
losses. Improvement is an offline, versioned process. Keep the hypothesis,
universe, data transformations, fees, latency assumptions and parameters fixed
through each evaluation.

Use chronological training and validation, then untouched holdout, then real-time
shadow. Include quiet, choppy, high-volatility, gap, news and failed-feed periods.
Avoid survivorship bias, forward-looking constituents and revised publication
times. Do not change parameters repeatedly against the same holdout.

Replay cannot reproduce the true exchange queue or prove execution availability.
Stress costs, latency, spreads, missed fills, partial fills, unknown submissions,
disconnects and stop-limit gaps. Compare gross and net results, loss tails,
drawdown, operational incidents and alternative uses of capital/time.

Daily observations are not automatically independent. Block bootstrap is only
one sensitivity test and does not capture every structural break. Even a
positive lower estimate is not proof of a profitable future. Paper profits are
not live audited profits.

Retain unsuccessful research versions and explanations for changes. Validate
new code and data on fresh evidence. A code/parameter/news/model change requires
new qualification; do not let a persuasive LLM bypass that requirement.
