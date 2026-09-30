#!/usr/bin/env python3
"""
Generate a synthetic multi-channel order dataset with the messy patterns real
commerce exports have, so the identity pipeline can be run and tested end to end.

Built-in patterns (each one is something the pipeline has to handle):
  - people with 2-3 web-store accounts that share a card or phone
  - households: two people at one address
  - marketplace orders with no customer ID; ~55% carry name + address, the rest only a zip
  - a handful of marketplace orders with an email
  - generic email local parts (info@, hello@) reused across unrelated domains
  - hub values: a store phone number typed in by many customers, an apartment building
  - zip formats: "07757", "7757", "07757-1234"; customer IDs exported as floats
  - blanks as "" instead of NULL
  - orders re-sent in later incremental files
  - optional: web-store name hashes salted per order (a realistic upstream defect)

Usage:
  python synthetic/generate_data.py --people 20000 --out data/
"""
import argparse, csv, gzip, hashlib, os, random
from datetime import datetime, timedelta

HEADER = ["CHANNEL", "SOURCE_CUSTOMER_ID", "ORDER_ID", "PROCESSED_AT", "ORDER_VALUE", "CURRENCY",
          "EMAIL_HASHED", "CUSTOMER_NAME_HASHED", "PHONE_HASHED", "SHIP_ADDRESS_1_HASHED",
          "SHIP_ADDRESS_2_HASHED", "SHIP_CITY", "SHIP_STATE", "SHIP_ZIP", "SHIP_COUNTRY",
          "CARD_FINGERPRINT", "PAYMENT_ACCOUNT_REFERENCE"]
DOMAINS = ["gmail.com"] * 6 + ["yahoo.com"] * 2 + ["hotmail.com", "aol.com", "icloud.com", "outlook.com"]
GENERIC_LOCAL = ["info", "hello", "contact", "sales"]


def sha(s):
    return hashlib.sha256(s.encode()).hexdigest()


def fmt_zip(z, rng):
    return rng.choice([z, z, z.lstrip("0"), f"{z}-{rng.randint(1000, 9999)}"])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--people", type=int, default=20000)
    ap.add_argument("--out", default="data")
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--salted-store-names", action=argparse.BooleanOptionalAction, default=True,
                    help="simulate an upstream defect: web-store name hashes change on every order")
    args = ap.parse_args()
    rng = random.Random(args.seed)
    os.makedirs(args.out, exist_ok=True)

    start = datetime(2023, 1, 1)
    store_phone = sha("5550000000")
    apartment = (sha("100 main st"), "10001")
    rows, oid, cid = [], 100000, 5000000000000

    for p in range(args.people):
        # identity of this person
        hh = p // 2 if rng.random() < 0.3 else p                # ~30% share a household
        addr = apartment if rng.random() < 0.004 else (sha(f"addr{hh}"), f"{rng.randint(1000, 99999):05d}")
        if rng.random() < 0.01:                                   # small-business buyer: generic local part
            email = f"{sha(rng.choice(GENERIC_LOCAL))}@biz{p}.com"
        else:
            email = f"{sha(f'person{p}')}@{rng.choice(DOMAINS)}"
        phone = store_phone if rng.random() < 0.01 else (sha(f"phone{p}") if rng.random() < 0.65 else "")
        card = f"card{p:07d}" if rng.random() < 0.2 else ""
        n_accounts = rng.choices([1, 2, 3], [0.9, 0.08, 0.02])[0]
        accounts = [cid + p * 10 + k for k in range(n_accounts)]
        emails = [email] + [f"{sha(f'person{p}alt{k}')}@{rng.choice(DOMAINS)}" for k in range(1, n_accounts)]

        n_orders = rng.choices([1, 2, 3, 4, 6], [0.62, 0.2, 0.1, 0.05, 0.03])[0]
        t = start + timedelta(days=rng.randint(0, 900))
        for _ in range(n_orders):
            oid += 1
            t += timedelta(days=rng.randint(10, 200))
            value = f"{rng.uniform(15, 120):.9f}"
            if rng.random() < 0.8:  # web store
                k = rng.randrange(n_accounts)
                name = sha(f"name{p}|{oid}") if args.salted_store_names else sha(f"name{p}")
                rows.append(["shopify", f"{accounts[k]}.00000", str(oid), t.strftime("%Y-%m-%d %H:%M:%S.000"),
                             value, "USD", emails[k], name, phone, addr[0], "", "", "NY",
                             fmt_zip(addr[1], rng), "US", card, ""])
            else:                   # marketplace
                full = rng.random() < 0.55
                rows.append(["amazon", "", f"111-{oid:07d}", t.strftime("%Y-%m-%d 00:00:00.000"), value, "USD",
                             emails[0] if rng.random() < 0.01 else "",
                             sha(f"name{p}") if full else "", "", addr[0] if full else "", "", "", "NY",
                             fmt_zip(addr[1], rng), "US", "", ""])

    rng.shuffle(rows)
    # split into a base file + incremental drops; re-send ~0.5% of orders in a later drop
    base, rest = rows[: len(rows) * 3 // 4], rows[len(rows) * 3 // 4:]
    drops = [rest[i::6] for i in range(6)]
    drops[-1] += rng.sample(base, max(1, len(base) // 200))
    for name, chunk in [("orders_000_base", base)] + [(f"orders_{i + 1:03d}_incr", d) for i, d in enumerate(drops)]:
        with gzip.open(os.path.join(args.out, f"{name}.csv.gz"), "wt", newline="") as fh:
            w = csv.writer(fh, quoting=csv.QUOTE_ALL)
            w.writerow(HEADER)
            w.writerows(chunk)
    print(f"wrote {len(rows):,} orders for {args.people:,} people to {args.out}/ ({1 + len(drops)} files)")


if __name__ == "__main__":
    main()
