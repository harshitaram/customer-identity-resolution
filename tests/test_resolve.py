import csv, gzip, os, sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from identity_resolution.resolve import run  # noqa: E402

HEADER = ["CHANNEL", "SOURCE_CUSTOMER_ID", "ORDER_ID", "PROCESSED_AT", "ORDER_VALUE", "CURRENCY",
          "EMAIL_HASHED", "CUSTOMER_NAME_HASHED", "PHONE_HASHED", "SHIP_ADDRESS_1_HASHED",
          "SHIP_ADDRESS_2_HASHED", "SHIP_CITY", "SHIP_STATE", "SHIP_ZIP", "SHIP_COUNTRY",
          "CARD_FINGERPRINT", "PAYMENT_ACCOUNT_REFERENCE"]


def order(channel, oid, cid="", email="", name="", phone="", addr="", zip_="10001", card="", par="", ts="2025-01-01 10:00:00.000"):
    return [channel, cid, oid, ts, "25.0", "USD", email, name, phone, addr, "", "", "NY", zip_, "US", card, par]


def write(tmp_path, files):
    for fname, rows in files.items():
        with gzip.open(tmp_path / fname, "wt", newline="") as fh:
            w = csv.writer(fh, quoting=csv.QUOTE_ALL)
            w.writerow(HEADER)
            w.writerows(rows)
    return str(tmp_path / "*.csv.gz")


def ids(con):
    return {r[0]: (r[1], r[2]) for r in con.sql("SELECT order_id, person_id, household_id FROM identity").fetchall()}


def test_transitive_merge_across_keys(tmp_path):
    # 1 ~ 2 via phone, 2 ~ 3 via card  ->  one person
    g = write(tmp_path, {"a.csv.gz": [
        order("shopify", "1", cid="11.00000", email="e1@x.com", phone="p1", addr="a1"),
        order("shopify", "2", cid="22.00000", email="e2@x.com", phone="p1", card="c1", addr="a2"),
        order("shopify", "3", cid="33.00000", email="e3@x.com", card="c1", addr="a3"),
    ]})
    con, _ = run(g, str(tmp_path / "out"))
    r = ids(con)
    assert r["1"][0] == r["2"][0] == r["3"][0]


def test_hub_values_are_ignored(tmp_path):
    # one phone shared by 6 different customers = a store number, not a person
    rows = [order("shopify", str(i), cid=f"{i}00.00000", email=f"e{i}@x.com", phone="STORE", addr=f"a{i}") for i in range(6)]
    g = write(tmp_path, {"a.csv.gz": rows})
    con, m = run(g, str(tmp_path / "out"))
    assert len({v[0] for v in ids(con).values()}) == 6
    assert m["hub_values_blocked"].get("phone") == 1


def test_blank_identifiers_never_link(tmp_path):
    # hash(NULL) is not NULL in DuckDB; blanks must not collapse into one value
    rows = [order("shopify", str(i), cid=f"{i}.00000", email=f"e{i}@x.com", addr=f"a{i}") for i in range(4)]
    g = write(tmp_path, {"a.csv.gz": rows})
    con, _ = run(g, str(tmp_path / "out"))
    assert len({v[0] for v in ids(con).values()}) == 4


def test_zip_formats_normalize_for_household_match(tmp_path):
    g = write(tmp_path, {"a.csv.gz": [
        order("shopify", "1", cid="1.00000", email="e1@x.com", addr="A", zip_="07757"),
        order("shopify", "2", cid="2.00000", email="e2@x.com", addr="A", zip_="07757-1234"),
        order("amazon", "3", name="n", addr="A", zip_="7757"),
    ]})
    con, _ = run(g, str(tmp_path / "out"))
    r = ids(con)
    assert r["1"][1] == r["2"][1] == r["3"][1]        # one household
    assert len({r["1"][0], r["2"][0], r["3"][0]}) == 3  # but three people


def test_marketplace_rebuilds_customer_and_zip_only_stays_alone(tmp_path):
    g = write(tmp_path, {"a.csv.gz": [
        order("amazon", "A1", name="n1", addr="A", zip_="10001"),
        order("amazon", "A2", name="n1", addr="A", zip_="10001"),
        order("amazon", "A3", zip_="10001"),
    ]})
    con, m = run(g, str(tmp_path / "out"))
    r = ids(con)
    assert r["A1"][0] == r["A2"][0] != r["A3"][0]
    assert m["pct_marketplace_unresolvable"] == 33.3


def test_email_hash_needs_full_address(tmp_path):
    # same local-part hash, different domains (think info@) -> not the same person
    g = write(tmp_path, {"a.csv.gz": [
        order("shopify", "1", cid="1.00000", email="abc@shopone.com", addr="a1"),
        order("shopify", "2", cid="2.00000", email="abc@shoptwo.com", addr="a2"),
    ]})
    con, _ = run(g, str(tmp_path / "out"))
    r = ids(con)
    assert r["1"][0] != r["2"][0]


def test_resent_orders_are_deduplicated(tmp_path):
    g = write(tmp_path, {
        "orders_001.csv.gz": [order("shopify", "1", cid="1.00000", email="e@x.com")],
        "orders_002.csv.gz": [order("shopify", "1", cid="1.00000", email="e@x.com")],
    })
    con, m = run(g, str(tmp_path / "out"))
    assert m["orders"] == 1
