# Datasets

A dataset is a source-independent contract. Every provider that serves a dataset must return
the same columns, and every row stored in gold has the same schema, primary key, and quality
guarantees, regardless of where it came from.

Run `finsight datasets` to list datasets and `finsight datasets <name>` for a schema.

## `ohlcv`: price bars

Open, high, low, close, adjusted close, and volume per symbol and interval.

### Schema (silver and gold)

| Column | Type | Key | Description |
|---|---|---|---|
| `symbol` | string | ✓ | Ticker in provider notation, e.g. `AAPL`, `BRK-B`, `^GSPC`, `EURUSD=X` |
| `date` | date | | Trading date in the exchange's local timezone |
| `ts` | timestamp[us, UTC] | ✓ | Bar start time in UTC. Daily bars start at local midnight |
| `open` | double | | Opening price |
| `high` | double | | Highest price |
| `low` | double | | Lowest price |
| `close` | double | | Closing price |
| `adj_close` | double | | Close adjusted for splits and dividends |
| `volume` | int64 | | Volume (as traded for `raw`, split-adjusted for `adjusted`) |
| `interval` | string | ✓ | `1m`, `5m`, `15m`, `30m`, `1h`, `1d`, `1wk`, `1mo` |
| `adjustment` | string | ✓ | `raw` or `adjusted` |
| `provider` | string | ✓ | Provider the bar came from |
| `run_id` | string | | Run that last wrote this bar |
| `ingested_at` | timestamp[us, UTC] | | When the bar was fetched |

**Primary key:** `(provider, symbol, interval, adjustment, ts)`.
**Gold partitioning:** `provider / interval / adjustment / symbol`.

### Provider contract (bronze)

Providers return `ts, date, open, high, low, close, adj_close, volume` for a single symbol.
FinSight adds `symbol, provider, interval, adjustment, run_id, fetched_at`. A frame with missing
columns, or values that cannot be cast to the contract types, is rejected as a `ContractError`
and that symbol is marked `failed`.

### Price adjustment

| Mode | open / high / low / close | `adj_close` | `volume` | Use it for |
|---|---|---|---|---|
| `raw` | As traded on the day | Split- and dividend-adjusted | As traded | Reconstructing historical quotes, order sizes, backtests that model corporate actions explicitly |
| `adjusted` | Split- and dividend-adjusted | Same as `close` | Split-adjusted | Returns, indicators, and charts that need a continuous series |

Both modes can be stored side by side. They are separate series because `adjustment` is part of
the primary key.

For example, AAPL closed at **$499.23** on 2020-08-28, the day before its 4-for-1 split. In
`raw` mode that is the stored `close`, with 46.9M shares of volume. In `adjusted` mode the close
is about $121 and volume is about 188M.

> **Note on Yahoo:** Yahoo's own "Close" is already split-adjusted. The `yahoo` provider rebuilds
> true as-traded prices for `raw` mode from the symbol's full split history. For weekly and
> monthly bars, a split that falls inside a bar is attributed by the bar's start date.

### Quality checks

Error checks block publication of the symbol. Warning checks are recorded in the manifest, and
the data is still published.

| Check | Severity | Rule |
|---|---|---|
| `not_null` | error | `symbol`, `date`, `ts`, `close` are never null |
| `unique_key` | error | The primary key is unique |
| `positive_prices` | error | open, high, low, close > 0 |
| `non_negative_volume` | error | volume ≥ 0 |
| `within_requested_window` | error | `date` lies within the requested window. Weekly and monthly bars may carry their period's start date |
| `high_gte_low` | warning | high ≥ low |
| `open_close_within_range` | warning | low ≤ open, close ≤ high (with a 1e-6 relative tolerance) |

Rows where open, high, low, and close are all empty (for example, holiday placeholders) are
dropped during conforming. The count is recorded as `dropped_rows`.

### Known limitations

- **Adjusted values are as of fetch time.** A new dividend or split changes every earlier
  adjusted price, and also `adj_close` in `raw` mode. Re-fetch the full history of affected
  symbols after a corporate action to restate them. Incremental refreshes, such as the
  `finsight_daily` DAG, only rewrite the window they fetch.
- **Intraday history is limited by the provider.** For example, Yahoo keeps 1-minute bars for
  about 30 days.
- Pre-market and post-market bars are not requested.

## Planned datasets

The contract model is designed for these next:

| Dataset | Primary key (proposed) | Notes |
|---|---|---|
| `dividends` | `(provider, symbol, ex_date)` | Cash amount, pay date |
| `splits` | `(provider, symbol, effective_date)` | Ratio |
| `corporate_actions` | `(provider, symbol, action_type, effective_date)` | Symbol changes, spin-offs, delistings |
| `fundamentals` | `(provider, symbol, period_end, statement, line_item)` | Statement line items |

See [providers.md](providers.md#adding-a-dataset) for how to add one.
