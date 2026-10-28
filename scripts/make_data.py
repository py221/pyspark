"""Generate the small synthetic datasets used by every stage.

Pure standard library, fixed seed: running it twice gives identical files.

    python scripts/make_data.py

Files written to data/:
    access_log.txt        web server log, Common Log Format (some junk lines)
    orders.csv            sales orders (some missing discounts)
    customers.csv         customer dimension table for joins
    sensor_readings.tsv   messy tab-separated text (some short/broken lines)
    events.jsonl          nested JSON events with arrays
    houses.csv            regression dataset (price), a few missing values
    churn.csv             classification dataset (churned 0/1), missing values
    stream_source/        a handful of small JSON files for the streaming stage
"""
from __future__ import annotations

import json
import random
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

DATA = Path(__file__).resolve().parent.parent / "data"
SEED = 7


def write_access_log(rng: random.Random, n: int = 2000) -> None:
    paths = ["/", "/index.html", "/products", "/products/42", "/cart", "/checkout",
             "/api/search", "/api/items", "/static/app.js", "/static/site.css",
             "/login", "/about"]
    methods = ["GET"] * 8 + ["POST"] * 2
    statuses = [200] * 80 + [304] * 8 + [301] * 3 + [404] * 6 + [500] * 3
    hosts = [f"10.0.{rng.randint(0, 3)}.{rng.randint(1, 60)}" for _ in range(40)]
    start = datetime(2026, 3, 1, 0, 0, 0, tzinfo=timezone.utc)
    lines = []
    t = start
    for i in range(n):
        t += timedelta(seconds=rng.randint(20, 400))
        stamp = t.strftime("%d/%b/%Y:%H:%M:%S +0000")
        size = rng.randint(200, 90000)
        status = rng.choice(statuses)
        size_field = "-" if status in (304, 301) else str(size)
        lines.append(
            f'{rng.choice(hosts)} - - [{stamp}] "{rng.choice(methods)} '
            f'{rng.choice(paths)} HTTP/1.1" {status} {size_field}'
        )
        # a few malformed lines to practise defensive parsing
        if i % 400 == 399:
            lines.append("-- truncated line --")
    (DATA / "access_log.txt").write_text("\n".join(lines) + "\n")


PRODUCTS = [
    ("keyboard", "electronics", 45.0), ("monitor", "electronics", 210.0),
    ("headset", "electronics", 80.0), ("desk", "furniture", 260.0),
    ("chair", "furniture", 180.0), ("lamp", "furniture", 35.0),
    ("notebook", "stationery", 4.5), ("pen_set", "stationery", 9.0),
    ("backpack", "accessories", 60.0), ("bottle", "accessories", 18.0),
]
REGIONS = ["north", "south", "east", "west"]


def write_orders_and_customers(rng: random.Random, n_orders: int = 1200,
                               n_customers: int = 150) -> None:
    segments = ["consumer", "consumer", "consumer", "small_business", "enterprise"]
    countries = ["US", "CA", "MX", "UK", "DE"]
    rows = ["customer_id,segment,signup_date,country"]
    for c in range(1, n_customers + 1):
        sd = date(2024, 1, 1) + timedelta(days=rng.randint(0, 700))
        rows.append(f"C{c:04d},{rng.choice(segments)},{sd.isoformat()},{rng.choice(countries)}")
    (DATA / "customers.csv").write_text("\n".join(rows) + "\n")

    rows = ["order_id,customer_id,order_date,product,category,quantity,unit_price,region,discount"]
    for o in range(1, n_orders + 1):
        product, category, price = rng.choice(PRODUCTS)
        d = date(2026, 1, 1) + timedelta(days=rng.randint(0, 119))
        qty = rng.choice([1, 1, 1, 2, 2, 3, 5])
        unit = round(price * rng.uniform(0.9, 1.1), 2)
        # customers 1..150 but a few ids (C0151+) have no matching customer row
        cust = f"C{rng.randint(1, n_customers + 8):04d}"
        discount = "" if rng.random() < 0.15 else rng.choice(["0.0", "0.05", "0.1", "0.2"])
        rows.append(f"O{o:05d},{cust},{d.isoformat()},{product},{category},{qty},"
                    f"{unit},{rng.choice(REGIONS)},{discount}")
    (DATA / "orders.csv").write_text("\n".join(rows) + "\n")


def write_sensor_readings(rng: random.Random, n: int = 600) -> None:
    sites = ["plant_a", "plant_b", "warehouse", "lab"]
    notes = ["calibrated after maintenance", "normal", "spike observed near intake",
             "sensor replaced", "", "operator reported vibration", "all clear"]
    lines = []
    for i in range(n):
        d = date(2025, 1, 1) + timedelta(days=rng.randint(0, 600))
        fields = [f"S{rng.randint(1, 25):03d}", d.strftime("%Y%m%d"), rng.choice(sites),
                  rng.choice(["ok", "ok", "ok", "warn", "fail"]),
                  f"{rng.gauss(50, 12):.2f}", rng.choice(notes)]
        if rng.random() < 0.08:          # broken record: too few fields
            fields = fields[: rng.randint(2, 4)]
        lines.append("\t".join(fields))
    (DATA / "sensor_readings.tsv").write_text("\n".join(lines) + "\n")


def write_events(rng: random.Random, n: int = 500) -> None:
    oses = ["ios", "android", "web"]
    models = {"ios": ["phone_x", "phone_y"], "android": ["droid_a", "droid_b", "droid_c"],
              "web": ["chrome", "firefox", "safari"]}
    skus = [p[0] for p in PRODUCTS]
    price = {p[0]: p[2] for p in PRODUCTS}
    start = datetime(2026, 3, 1, tzinfo=timezone.utc)
    out = []
    for i in range(n):
        ts = start + timedelta(minutes=rng.randint(0, 60 * 24 * 6))
        os_ = rng.choice(oses)
        n_items = rng.choice([0, 1, 1, 2, 3, 4])
        items = [{"sku": (s := rng.choice(skus)), "qty": rng.randint(1, 4), "price": price[s]}
                 for _ in range(n_items)]
        ev = {
            "event_id": f"e{i:05d}",
            "ts": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "type": rng.choice(["view", "view", "add_to_cart", "purchase"]),
            "user": {"id": f"U{rng.randint(1, 80):03d}",
                     "country": rng.choice(["US", "CA", "UK", "DE"]),
                     "tags": rng.sample(["new", "vip", "mobile", "promo"], k=rng.randint(0, 3))},
            "device": {"os": os_, "model": rng.choice(models[os_])},
            "items": items,
        }
        if rng.random() < 0.1:
            del ev["device"]            # optional struct missing on some rows
        out.append(json.dumps(ev))
    (DATA / "events.jsonl").write_text("\n".join(out) + "\n")


def _maybe(rng: random.Random, value: str, p: float) -> str:
    return "" if rng.random() < p else value


def write_houses(rng: random.Random, n: int = 800) -> None:
    hoods = {"downtown": 1.35, "riverside": 1.15, "suburb": 1.0, "outskirts": 0.8}
    rows = ["house_id,sqft,bedrooms,bathrooms,age_years,neighborhood,has_garage,price"]
    for i in range(1, n + 1):
        sqft = int(rng.gauss(1800, 550))
        sqft = max(600, sqft)
        beds = max(1, min(6, round(sqft / 600 + rng.gauss(0, 0.6))))
        baths = max(1, min(4, round(beds * 0.6 + rng.gauss(0, 0.4))))
        age = rng.randint(0, 70)
        hood = rng.choice(list(hoods))
        garage = rng.random() < 0.6
        price = (sqft * 140 + beds * 4000 + baths * 7000 - age * 900
                 + (12000 if garage else 0)) * hoods[hood] + rng.gauss(0, 18000)
        rows.append(",".join([
            f"H{i:04d}", str(sqft), str(beds), str(baths),
            _maybe(rng, str(age), 0.04), _maybe(rng, hood, 0.03),
            "1" if garage else "0", f"{max(price, 40000):.0f}",
        ]))
    (DATA / "houses.csv").write_text("\n".join(rows) + "\n")


def write_churn(rng: random.Random, n: int = 1500) -> None:
    contracts = ["month_to_month"] * 5 + ["one_year"] * 3 + ["two_year"] * 2
    pays = ["card", "bank_transfer", "mailed_check", "wallet"]
    regions = REGIONS
    rows = ["customer_id,tenure_months,monthly_charge,contract,payment_method,"
            "support_calls,region,churned"]
    for i in range(1, n + 1):
        contract = rng.choice(contracts)
        tenure = rng.randint(1, 72)
        charge = round(rng.uniform(20, 110), 2)
        calls = min(10, int(rng.expovariate(0.6)))
        pay = rng.choice(pays)
        # churn probability: short tenure, month-to-month, many calls, high charge
        z = (-1.2 - 0.045 * tenure + 0.012 * (charge - 60) + 0.35 * calls
             + {"month_to_month": 1.1, "one_year": 0.0, "two_year": -0.9}[contract]
             + (0.4 if pay == "mailed_check" else 0))
        p = 1 / (1 + 2.718281828 ** (-z))
        churned = 1 if rng.random() < p else 0
        rows.append(",".join([
            f"K{i:05d}", _maybe(rng, str(tenure), 0.05), _maybe(rng, str(charge), 0.03),
            _maybe(rng, contract, 0.02), pay, str(calls), rng.choice(regions), str(churned),
        ]))
    (DATA / "churn.csv").write_text("\n".join(rows) + "\n")


def write_stream_source(rng: random.Random, files: int = 5, per_file: int = 60) -> None:
    out = DATA / "stream_source"
    out.mkdir(exist_ok=True)
    pages = ["home", "search", "product", "cart", "checkout"]
    t = datetime(2026, 3, 1, 12, 0, 0, tzinfo=timezone.utc)
    for f in range(files):
        lines = []
        for _ in range(per_file):
            t += timedelta(seconds=rng.randint(1, 20))
            late = rng.random() < 0.05           # a few out-of-order events
            et = t - timedelta(minutes=rng.randint(6, 12)) if late else t
            lines.append(json.dumps({
                "event_time": et.strftime("%Y-%m-%dT%H:%M:%S"),
                "page": rng.choice(pages),
                "user_id": f"U{rng.randint(1, 30):03d}",
                "latency_ms": rng.randint(20, 900),
            }))
        (out / f"part-{f:04d}.json").write_text("\n".join(lines) + "\n")


def main() -> None:
    DATA.mkdir(exist_ok=True)
    rng = random.Random(SEED)
    write_access_log(rng)
    write_orders_and_customers(rng)
    write_sensor_readings(rng)
    write_events(rng)
    write_houses(rng)
    write_churn(rng)
    write_stream_source(rng)
    for p in sorted(DATA.rglob("*")):
        if p.is_file():
            print(f"{p.relative_to(DATA)}  {p.stat().st_size:,} bytes")


if __name__ == "__main__":
    main()
