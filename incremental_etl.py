"""
Incremental ETL Pipeline
Python Specialization - Data Engineering

Run it twice in a row. The second run must change nothing.
"""

import csv
import json
import logging
import time
from datetime import datetime
from pathlib import Path

WAREHOUSE_PATH = Path("data") / "transactions.csv"
BATCH_DIR = Path("incoming")
CHECKPOINT_PATH = Path("state") / "checkpoint.json"
LOG_PATH = Path("logs") / "etl.log"

KEY = "transaction_id"

COMPARED_FIELDS = (
    "customer_id",
    "merchant",
    "amount_kes",
    "status",
    "channel",
    "updated_at"
)


def setup_logging(path: Path = LOG_PATH) -> logging.Logger:
    """Configure a logger that writes to both the console and a log file."""

    path.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("etl")
    logger.setLevel(logging.INFO)

    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s %(message)s"
    )

    file_handler = logging.FileHandler(path)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


def load_existing(path: Path = WAREHOUSE_PATH) -> dict[str, dict]:
    """Read the loaded transactions into a dict keyed by transaction_id."""

    records = {}

    if not path.exists():
        return records

    with open(path, newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        for row in reader:
            row["amount_kes"] = float(row["amount_kes"])
            records[row[KEY]] = row

    return records


def load_batch(path: Path) -> tuple[str | None, list[dict]]:
    """Read one JSON batch file and return (batch_id, transactions)."""

    try:
        with open(path, encoding="utf-8") as file:
            payload = json.load(file)

        return payload["batch_id"], payload["transactions"]

    except Exception as exc:
        logging.error(f"Unable to load batch {path}: {exc}")
        return None, []


def validate_record(record: dict) -> list:
    """Return reasons a record cannot be loaded."""

    errors = []

    if not record.get("transaction_id"):
        errors.append("missing transaction_id")

    try:
        amount = float(record["amount_kes"])
        if amount < 0:
            errors.append("negative amount_kes")
    except (ValueError, TypeError):
        errors.append("invalid amount_kes")

    required_fields = [
        "customer_id",
        "merchant",
        "status",
        "channel",
        "updated_at"
    ]

    for field in required_fields:
        if not record.get(field):
            errors.append(f"missing {field}")

    try:
        datetime.fromisoformat(record["updated_at"])
    except Exception:
        errors.append("invalid updated_at")

    return errors


def classify(existing: dict[str, dict], batch: list[dict]) -> dict[str, list[dict]]:
    """
    Split records into:
    new
    updated
    duplicate
    invalid
    """

    results = {
        "new": [],
        "updated": [],
        "duplicate": [],
        "invalid": []
    }

    seen_batch_ids = set()

    for idx, record in enumerate(batch):

        errors = validate_record(record)

        if errors:
            record["_errors"] = errors
            record["_index"] = idx
            results["invalid"].append(record)
            continue

        tx_id = record[KEY]

        if tx_id in seen_batch_ids:
            results["duplicate"].append(record)
            continue

        seen_batch_ids.add(tx_id)

        record["amount_kes"] = float(record["amount_kes"])

        if tx_id not in existing:
            results["new"].append(record)
            continue

        current = existing[tx_id]

        identical = all(
            str(record[field]) == str(current[field])
            for field in COMPARED_FIELDS
        )

        if identical:
            results["duplicate"].append(record)
            continue

        incoming_ts = datetime.fromisoformat(record["updated_at"])
        existing_ts = datetime.fromisoformat(current["updated_at"])

        if incoming_ts > existing_ts:
            results["updated"].append(record)
        else:
            # stale version, don't overwrite newer warehouse data
            results["duplicate"].append(record)

    return results


def upsert(
    existing: dict[str, dict],
    new: list[dict],
    updated: list[dict]
) -> dict[str, dict]:
    """Merge new and updated records."""

    merged = existing.copy()

    for record in new:
        merged[record[KEY]] = record

    for record in updated:
        merged[record[KEY]] = record

    return merged


def read_checkpoint(path: Path = CHECKPOINT_PATH) -> dict:
    """Return checkpoint or initialize a new one."""

    if not path.exists():
        return {
            "last_run_at": None,
            "processed_batches": [],
            "high_watermark": None,
            "row_count": 0
        }

    with open(path, encoding="utf-8") as file:
        return json.load(file)


def write_checkpoint(
    checkpoint: dict,
    path: Path = CHECKPOINT_PATH
) -> None:
    """Atomically write checkpoint."""

    path.parent.mkdir(parents=True, exist_ok=True)

    temp_path = path.with_suffix(".tmp")

    with open(temp_path, "w", encoding="utf-8") as file:
        json.dump(checkpoint, file, indent=2)

    temp_path.replace(path)


def save_warehouse(
    state: dict[str, dict],
    path: Path = WAREHOUSE_PATH
) -> None:
    """Write warehouse atomically."""

    path.parent.mkdir(parents=True, exist_ok=True)

    temp_path = path.with_suffix(".tmp")

    fieldnames = [
        "transaction_id",
        "customer_id",
        "merchant",
        "amount_kes",
        "status",
        "channel",
        "updated_at"
    ]

    rows = sorted(
        state.values(),
        key=lambda row: row["transaction_id"]
    )

    with open(temp_path, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=fieldnames
        )

        writer.writeheader()

        for row in rows:

            clean_row = {
                field: row[field]
                for field in fieldnames
            }

            writer.writerow(clean_row)

    temp_path.replace(path)


def run() -> dict:

    start_time = time.time()

    logger = setup_logging()

    logger.info("Run started")

    existing = load_existing()
    logger.info(f"Existing rows loaded: {len(existing)}")

    checkpoint = read_checkpoint()

    totals = {
        "new": 0,
        "updated": 0,
        "duplicate": 0,
        "invalid": 0
    }

    batch_files = sorted(BATCH_DIR.glob("*.json"))

    for batch_file in batch_files:

        batch_id, records = load_batch(batch_file)

        if not batch_id:
            continue

        if batch_id in checkpoint["processed_batches"]:

            logger.info(
                f"Batch {batch_id} already processed - skipping"
            )

            continue

        logger.info(
            f"Batch {batch_id}: {len(records)} records read"
        )

        buckets = classify(existing, records)

        for invalid in buckets["invalid"]:

            tx_id = invalid.get("transaction_id")

            identifier = (
                tx_id
                if tx_id
                else f"index_{invalid['_index']}"
            )

            logger.warning(
                f"Invalid record {identifier}: "
                f"{', '.join(invalid['_errors'])}"
            )

        logger.info(
            f"New: {len(buckets['new'])} | "
            f"Updated: {len(buckets['updated'])} | "
            f"Duplicates: {len(buckets['duplicate'])} | "
            f"Invalid: {len(buckets['invalid'])}"
        )

        existing = upsert(
            existing,
            buckets["new"],
            buckets["updated"]
        )

        save_warehouse(existing)

        latest_timestamp = max(
            (
                r["updated_at"]
                for r in existing.values()
            ),
            default=None
        )

        checkpoint["last_run_at"] = datetime.now().isoformat()

        checkpoint["row_count"] = len(existing)

        checkpoint["high_watermark"] = latest_timestamp

        checkpoint["processed_batches"].append(batch_id)

        write_checkpoint(checkpoint)

        totals["new"] += len(buckets["new"])
        totals["updated"] += len(buckets["updated"])
        totals["duplicate"] += len(buckets["duplicate"])
        totals["invalid"] += len(buckets["invalid"])

    logger.info(f"Rows after load: {len(existing)}")

    runtime = round(time.time() - start_time, 2)

    logger.info(f"Run finished in {runtime}s")

    return {
        "rows": len(existing),
        "new": totals["new"],
        "updated": totals["updated"],
        "duplicate": totals["duplicate"],
        "invalid": totals["invalid"]
    }


if __name__ == "__main__":
    run()