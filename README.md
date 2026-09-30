# Customer Identity Resolution

The same person shows up as several "customers": two web-store accounts, anonymous marketplace orders, masked emails. No field is shared across every channel, and the PII is SHA-256 hashed, so only **exact** matches are possible.

This project resolves those records into one customer ID per real person, using deterministic rules in DuckDB SQL. It then measures what that changes: customer counts, repeat rate, and how many "new" customers had actually bought before.

## What's here

| Path | What it does |
|---|---|
| `identity_resolution/resolve.py` | The pipeline: raw order files in, unified IDs out. All match rules and the junk-value threshold live in one config block |
| `synthetic/generate_data.py` | Generates a realistic, messy multi-channel dataset to run everything against |
| `notebooks/walkthrough.ipynb` | Profile the identifiers → design the rules → resolve → validate → measure the impact |
| `tests/test_resolve.py` | Tests for each design decision (transitive merges, hub guardrail, null safety, zip normalization, dedupe) |

## Quickstart

```bash
pip install -r requirements.txt
python synthetic/generate_data.py --people 20000 --out data
python -m identity_resolution.resolve --data "data/*.csv.gz" --out output
pytest -q
```

Output:
- `identity_map.parquet`: one row per order, with its `person_id` and `household_id`
- `unified_customers.parquet`: one row per person
- `run_summary.json`: monitoring metrics (merge counts, largest cluster, hub values blocked, % unresolvable)

## How it works

```
ingest → normalize → match on exact keys → drop hub values → connected components → person_id / household_id
```

**Two ID levels, on purpose:**
- **`person_id`**: store customer ID, full email, card fingerprint, payment account reference, phone, and (inside the marketplace) name + address + zip. Use it for KPIs: customer count, LTV, CAC, new vs. returning.
- **`household_id`**: `person_id` plus shared address + zip. That's often the only bridge to marketplace orders, but families and roommates share addresses, so use it for suppression lists and household retention, not for person-level metrics.

**Connected components.** If A matches B on email and B matches C on card, then A, B and C are one person. This is implemented as min-label propagation in SQL, which is deterministic and converges in a few passes.

## Lessons baked into the rules

- **Profile before you design rules.** Every rule traces back to a check in the walkthrough notebook.
- **Hashed emails are often `sha256(local_part)@domain`.** Generic addresses like `info@` and `hello@` then hash identically across unrelated businesses, so match on the full string only.
- **Test "consistent hashing" claims.** A name hash that changes on every order (a salted-hash defect upstream) is useless for matching, even when documentation says otherwise.
- **Cap every identifier.** Store phone numbers, apartment buildings and reshippers are shared by hundreds of customers. Without a cap, one of them chains strangers into a single "customer".
- **Load everything as text.** Zips have leading zeros (`07757`), and IDs get exported as floats (`6010141245495.00000`). Type inference corrupts both.
- **`hash(NULL)` is not NULL in DuckDB.** Without a null-safe wrapper, every blank value becomes one giant hub.
- **Incremental files re-send orders.** The latest file wins per `(channel, order_id)`.

## Results on the synthetic dataset (20,000 people, ~34K orders)

| | Channel-native | Unified person | Unified household |
|---|---|---|---|
| Customers | 24,670 | 24,066 | 21,978 |
| Repeat-purchase rate | 23.3% | 25.5% | 31.4% |

**Why the "new" customer count matters:** every returning customer counted as new inflates the denominator of CAC, so reported CAC is understated by the same ratio. Those customers can also be suppressed from prospecting audiences.

## Running it in production

The same SQL runs incrementally on each new file drop:
1. Load only new files and upsert on `(channel, order_id)`.
2. Relabel only the components the new values touch.
3. When two customers merge, keep the older ID and write a merge-history row, so downstream audiences stay stable.
4. Alert on spikes in merge rate, largest cluster or hub count.

The logic is plain SQL, so it ports to Snowflake, BigQuery or dbt unchanged.

## Stack

Python · DuckDB · pandas · Jupyter · Parquet · pytest
