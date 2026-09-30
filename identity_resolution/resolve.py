"""
Deterministic customer identity resolution for multi-channel commerce data.

Takes raw order exports from a web store (Shopify-style: customer ID, email, phone,
card tokens) and a marketplace (Amazon-style: no customer ID, sparse email, name +
address) where PII is SHA-256 hashed, and assigns every order a unified customer ID
at two confidence levels:

  person_id     high-confidence keys only: store customer ID, full email, card
                fingerprint, payment account reference, phone, and (inside the
                marketplace) name + address + zip
  household_id  person_id plus shared shipping address + zip

Usage:
  python -m identity_resolution.resolve --data "data/*.csv.gz" --out output/

Outputs (in --out):
  identity_map.parquet       one row per order: channel, order_id, person_id, household_id
  unified_customers.parquet  one row per person
  run_summary.json           monitoring metrics (merge rate, largest cluster, hubs blocked, ...)

Design notes:
  - Load everything as text: zips keep leading zeros; IDs exported as floats stay intact.
  - Hashed emails are often `sha256(local_part)@domain`, so generic local parts
    (info@, hello@) collide across unrelated businesses -> match on the full string only.
  - Any identifier value shared by > MAX_ENTITIES entities is treated as junk
    (store phone numbers, apartment buildings, reshippers) and ignored.
  - Orders can be re-sent in later incremental files -> latest file wins per (channel, order_id).
  - hash(NULL) is NOT NULL in DuckDB -> a null-safe macro keeps blanks from merging into one hub.
"""
import argparse, json, os, time
import duckdb

# ---------------------------------------------------------------- decision model
MAX_ENTITIES = 5
RULES = {
    # rule: (level, SQL expression over normalized orders)
    "email":    ("person",    "email"),
    "card":     ("person",    "card"),
    "par":      ("person",    "par"),
    "phone":    ("person",    "phone_h"),
    "addr_zip": ("household", "CASE WHEN addr1_h IS NOT NULL AND zip5 IS NOT NULL THEN hash(addr1_h, zip5) END"),
}


def load(con, data_glob, exclude_zero):
    zero_filter = "WHERE coalesce(order_value, 0) > 0" if exclude_zero else ""
    con.sql("CREATE OR REPLACE MACRO h(x) AS CASE WHEN x IS NULL THEN NULL ELSE hash(x) END")
    con.sql(f"""
    CREATE OR REPLACE TABLE orders AS
    WITH raw AS (
      SELECT * FROM read_csv('{data_glob}', header=true, all_varchar=true, union_by_name=true, filename=true)
    ),
    norm AS (
      SELECT
        lower(trim(channel))                                              AS channel,
        NULLIF(regexp_replace(trim(source_customer_id), '\\.0+$', ''), '') AS customer_id,
        NULLIF(trim(order_id), '')                                        AS order_id,
        try_cast(processed_at AS TIMESTAMP)                               AS processed_at,
        round(try_cast(order_value AS DOUBLE), 2)                         AS order_value,
        h(NULLIF(lower(trim(email_hashed)), ''))                          AS email,
        h(NULLIF(trim(customer_name_hashed), ''))                         AS name_h,
        h(NULLIF(trim(phone_hashed), ''))                                 AS phone_h,
        h(NULLIF(trim(ship_address_1_hashed), ''))                        AS addr1_h,
        CASE
          WHEN NULLIF(trim(ship_zip), '') IS NULL THEN NULL
          WHEN upper(trim(ship_country)) IN ('US','USA','') OR ship_country IS NULL
            THEN NULLIF(lpad(left(regexp_replace(ship_zip, '[^0-9]', '', 'g'), 5), 5, '0'), '00000')
          ELSE upper(replace(trim(ship_zip), ' ', ''))
        END                                                               AS zip5,
        h(NULLIF(trim(card_fingerprint), ''))                             AS card,
        h(NULLIF(trim(payment_account_reference), ''))                    AS par,
        filename
      FROM raw
    ),
    dedup AS (
      SELECT * EXCLUDE (filename) FROM norm
      QUALIFY row_number() OVER (PARTITION BY channel, order_id ORDER BY filename DESC) = 1
    )
    SELECT *,
      CASE
        WHEN channel = 'shopify' AND customer_id IS NOT NULL THEN 'S:' || customer_id
        WHEN channel = 'amazon' AND name_h IS NOT NULL AND addr1_h IS NOT NULL AND zip5 IS NOT NULL
             THEN 'A:' || hash(name_h, addr1_h, zip5)::VARCHAR
        ELSE 'O:' || channel || ':' || order_id
      END AS entity
    FROM dedup {zero_filter}
    """)


def build_edges(con):
    con.sql("CREATE OR REPLACE TABLE edges_raw AS " + " UNION ALL ".join(
        f"SELECT DISTINCT entity, '{r}' AS rule, '{lvl}' AS level, ({expr})::UBIGINT AS val "
        f"FROM orders WHERE ({expr}) IS NOT NULL"
        for r, (lvl, expr) in RULES.items()))
    con.sql("CREATE OR REPLACE TABLE value_stats AS "
            "SELECT rule, level, val, count(DISTINCT entity) AS n FROM edges_raw GROUP BY ALL")
    con.sql(f"CREATE OR REPLACE TABLE edges AS SELECT e.* FROM edges_raw e JOIN value_stats v USING (rule, val) "
            f"WHERE v.n BETWEEN 2 AND {MAX_ENTITIES}")


def resolve(con, levels, out_table, max_passes=100):
    """Connected components via min-label propagation over the entity <-> identifier-value graph."""
    lv = ",".join(f"'{l}'" for l in levels)
    con.sql(f"CREATE OR REPLACE TEMP TABLE e AS SELECT DISTINCT entity, hash(rule, val) AS v FROM edges WHERE level IN ({lv})")
    con.sql("CREATE OR REPLACE TEMP TABLE lbl AS SELECT DISTINCT entity, entity AS label FROM orders")
    for i in range(max_passes):
        con.sql("CREATE OR REPLACE TEMP TABLE vl AS SELECT e.v, min(l.label) AS label FROM e JOIN lbl l USING (entity) GROUP BY 1")
        con.sql("""CREATE OR REPLACE TEMP TABLE lbl2 AS
                   SELECT l.entity, least(l.label, coalesce(m.label, l.label)) AS label FROM lbl l
                   LEFT JOIN (SELECT e.entity, min(vl.label) AS label FROM e JOIN vl USING (v) GROUP BY 1) m USING (entity)""")
        changed = con.sql("SELECT count(*) FROM lbl JOIN lbl2 USING (entity) WHERE lbl.label <> lbl2.label").fetchone()[0]
        con.sql("CREATE OR REPLACE TEMP TABLE lbl AS SELECT * FROM lbl2")
        if changed == 0:
            break
    con.sql(f"CREATE OR REPLACE TABLE {out_table} AS SELECT entity, 'C-' || left(md5(label), 12) AS cid FROM lbl")
    for t in ("e", "lbl", "lbl2", "vl"):
        con.sql(f"DROP TABLE IF EXISTS {t}")
    return i + 1


def run(data_glob, out_dir, exclude_zero_orders=False, con=None):
    """Run the full pipeline; returns (duckdb connection, metrics dict). Tables left in `con`: identity."""
    t0 = time.time()
    os.makedirs(out_dir, exist_ok=True)
    con = con or duckdb.connect()
    con.sql("SET preserve_insertion_order = false")
    load(con, data_glob, exclude_zero_orders)
    build_edges(con)
    p_passes = resolve(con, ["person"], "person_map")
    h_passes = resolve(con, ["person", "household"], "household_map")
    con.sql("""CREATE OR REPLACE TABLE identity AS
               SELECT o.channel, o.order_id, o.processed_at, o.order_value, o.entity,
                      p.cid AS person_id, h.cid AS household_id
               FROM orders o JOIN person_map p USING (entity) JOIN household_map h USING (entity)""")
    m = metrics(con)
    m["passes"] = {"person": p_passes, "household": h_passes}
    m["runtime_sec"] = round(time.time() - t0, 1)
    return con, m


def metrics(con):
    m = con.sql("""
        SELECT count(*) AS orders,
               count(DISTINCT entity) AS source_entities,
               count(DISTINCT person_id) AS persons,
               count(DISTINCT household_id) AS households,
               round(100.0*count(*) FILTER (WHERE channel='amazon' AND entity LIKE 'O:%')
                     / NULLIF(count(*) FILTER (WHERE channel='amazon'),0), 1) AS pct_marketplace_unresolvable
        FROM identity""").df().iloc[0].to_dict()
    m["largest_person_cluster"] = con.sql(
        "SELECT max(n) FROM (SELECT count(DISTINCT entity) n FROM identity GROUP BY person_id)").fetchone()[0]
    m["hub_values_blocked"] = dict(con.sql(
        f"SELECT rule, count(*) FROM value_stats WHERE n > {MAX_ENTITIES} GROUP BY 1").fetchall())
    return {k: (int(v) if hasattr(v, "is_integer") and float(v).is_integer() else v) for k, v in m.items()}


def export(con, out_dir, m):
    con.sql(f"COPY (SELECT channel, order_id, person_id, household_id FROM identity) "
            f"TO '{out_dir}/identity_map.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)")
    con.sql(f"""COPY (SELECT person_id, any_value(household_id) AS household_id, count(*) AS orders,
                             round(sum(order_value), 2) AS revenue, min(processed_at) AS first_order,
                             max(processed_at) AS last_order, list(DISTINCT channel) AS channels
                      FROM identity GROUP BY 1)
                TO '{out_dir}/unified_customers.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)""")
    with open(os.path.join(out_dir, "run_summary.json"), "w") as f:
        json.dump(m, f, indent=2, default=str)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data", default="data/*.csv.gz", help="glob of raw order files")
    ap.add_argument("--out", default="output", help="output folder")
    ap.add_argument("--exclude-zero-orders", action="store_true",
                    help="v1.1: drop $0 orders (staff/test/sample orders) before matching")
    args = ap.parse_args()

    con, m = run(args.data, args.out, args.exclude_zero_orders)
    export(con, args.out, m)
    print(json.dumps(m, indent=2, default=str))


if __name__ == "__main__":
    main()
