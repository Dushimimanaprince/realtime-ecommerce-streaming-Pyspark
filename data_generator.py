"""Generates fake e-commerce events as CSV files for the streaming pipeline."""
import argparse
import csv
import os
import random
import time
import uuid
from datetime import datetime
from pathlib import Path

PRODUCTS = [
    ("P001", "Wireless Mouse", "Electronics", 19.99),
    ("P002", "Mechanical Keyboard", "Electronics", 74.50),
    ("P003", "USB-C Hub", "Electronics", 34.00),
    ("P004", "Running Shoes", "Sports", 59.90),
    ("P005", "Yoga Mat", "Sports", 24.99),
    ("P006", "Water Bottle", "Sports", 12.50),
    ("P007", "Python Cookbook", "Books", 39.00),
    ("P008", "Data Engineering Guide", "Books", 45.00),
    ("P009", "Desk Lamp", "Home", 29.99),
    ("P010", "Office Chair", "Home", 129.00),
]
EVENT_TYPES = ["view", "purchase"]
EVENT_WEIGHTS = [0.85, 0.15]
USER_COUNT = 200
FIELDS = [
    "event_id", "user_id", "event_type", "product_id", "product_name",
    "category", "price", "quantity", "event_time",
]


def make_event(bad_rate: float) -> dict:
    """Create one event. With probability bad_rate the event is deliberately corrupted."""
    product_id, product_name, category, price = random.choice(PRODUCTS)
    event_type = random.choices(EVENT_TYPES, EVENT_WEIGHTS)[0]
    event = {
        "event_id": str(uuid.uuid4()),
        "user_id": f"U{random.randint(1, USER_COUNT):04d}",
        "event_type": event_type,
        "product_id": product_id,
        "product_name": product_name,
        "category": category,
        "price": price,
        "quantity": random.randint(1, 3) if event_type == "purchase" else 0,
        "event_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    if random.random() < bad_rate:
        corrupt(event)
    return event


def corrupt(event: dict) -> None:
    """Break one field so the Spark job has something to clean."""
    problem = random.choice(["missing_user", "bad_price", "bad_event_type"])
    if problem == "missing_user":
        event["user_id"] = ""
    elif problem == "bad_price":
        event["price"] = random.choice(["N/A", "-5.0"])
    else:
        event["event_type"] = "clik"


def write_batch(output_dir: Path, events_per_file: int, bad_rate: float) -> Path:
    """Write one CSV file. It is written under a hidden name and then renamed,
    so Spark never sees a half-written file."""
    name = f"events_{datetime.now():%Y%m%d_%H%M%S_%f}.csv"
    temp_path = output_dir / f".{name}.tmp"
    final_path = output_dir / name

    with open(temp_path, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(make_event(bad_rate) for _ in range(events_per_file))

    os.replace(temp_path, final_path)
    return final_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate fake e-commerce event CSV files.")
    parser.add_argument("--output-dir", default="data/incoming", help="Folder for the CSV files")
    parser.add_argument("--events-per-file", type=int, default=20)
    parser.add_argument("--interval", type=float, default=5.0, help="Seconds between files")
    parser.add_argument("--files", type=int, default=0, help="Number of files (0 = run until Ctrl+C)")
    parser.add_argument("--bad-rate", type=float, default=0.05, help="Share of corrupted events (0 to 1)")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    written = 0
    try:
        while args.files == 0 or written < args.files:
            path = write_batch(output_dir, args.events_per_file, args.bad_rate)
            written += 1
            print(f"[{written}] wrote {args.events_per_file} events to {path.name}")
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print(f"\nStopped after {written} file(s).")


if __name__ == "__main__":
    main()