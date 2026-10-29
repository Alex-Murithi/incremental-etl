# Incremental ETL Pipeline

## Project Overview

This project implements an **incremental ETL (Extract, Transform, Load) pipeline** for Lipa Haraka, a Nairobi-based payments aggregator. The objective is to process daily transaction batches containing new, updated, duplicate, and invalid transaction records while ensuring that the warehouse remains accurate and free from duplicate loads.

A key requirement of the solution is **idempotency**. The pipeline can be executed multiple times on the same batch without creating duplicate records or altering previously processed data.

---

## Problem Statement

Lipa Haraka receives a daily JSON batch from its payment gateway. Each batch may contain:

- New transactions not previously loaded
- Existing transactions whose status or amount has changed
- Duplicate transactions already present in the warehouse
- Invalid records that should not be loaded

This project addresses that issue by implementing:

- Incremental loading
- Upsert logic
- Batch checkpointing
- Comprehensive logging
- Idempotent processing

---

## Objectives

The pipeline was designed to:

- Read existing transaction data from a CSV warehouse
- Load and process incoming JSON batches
- Compare records efficiently using dictionaries and sets
- Classify records into meaningful categories
- Apply updates only when incoming data is newer
- Prevent duplicate processing of batches
- Maintain processing checkpoints
- Generate execution logs for monitoring and troubleshooting
- Demonstrate idempotent behavior through repeated execution

---

## Directory Structure

```text
day3-incremental-etl/
│
├── incremental_etl.py
├── README.md
├── pyproject.toml
├── uv.lock
├── .gitignore
│
├── data/
│   └── transactions.csv
│
├── incoming/
│   └── batch_2026-10-21.json
│
├── state/
│   └── checkpoint.json
│
└── logs/
    └── etl.log
```

---

## Technologies Used

- Python 3
- CSV
- JSON
- Logging Module
- Dictionaries
- Sets
- File Handling
- Pathlib
- Datetime

No external data-processing libraries. 
---

## ETL Workflow

### 1. Extract

The pipeline loads:

- Existing warehouse records from `data/transactions.csv`
- Incoming transaction batches from `incoming/*.json`

The transaction warehouse is loaded into a dictionary keyed by `transaction_id` to enable fast lookups.

---

### 2. Transform

Incoming records are validated and classified.

The pipeline identifies:

- New records
- Updated records
- Duplicate records
- Invalid records

Validation checks include:

- Missing transaction IDs
- Negative transaction amounts
- Invalid amount formats
- Missing required fields
- Invalid timestamps

---

### 3. Load

After classification:

- New records are inserted
- Updated records replace older warehouse records
- Duplicate records are ignored
- Invalid records are rejected

The warehouse is then written back to disk using an atomic write operation to prevent corruption in case of failure.

---

## Classification Rules

### New

A record is classified as **New** when its `transaction_id` does not already exist in the warehouse.

---

### Updated

A record is classified as **Updated** when:

- The transaction already exists in the warehouse.
- One or more fields have changed.
- The incoming `updated_at` timestamp is newer than the stored record.

---

### Duplicate

A record is classified as **Duplicate** when:

- It is identical to an existing warehouse record.
- It appears more than once in the same batch.
- It represents an outdated version of a record already in the warehouse.

---

### Invalid

A record is classified as **Invalid** when:

- Required fields are missing.
- Data types are invalid.

Invalid records are logged and excluded from processing.

---

## Checkpointing Strategy

To prevent reprocessing completed batches, the pipeline maintains a checkpoint file:

The checkpoint is updated only after the warehouse has been successfully written.

This ensures that an interrupted run never falsely marks a batch as completed.

---

## Logging

The pipeline writes detailed logs to:

```text
logs/etl.log
```

Each run records:

- Run start and finish
- Existing row count
- Batch records read
- Classification counts
- Invalid record warnings
- Warehouse row count after processing
  
---

## Idempotency Verification

One of the primary objectives of the project was to ensure idempotent processing.

### First Run

```text
New: 6
Updated: 3
Duplicates: 5
Invalid: 3
Rows after load: 26
```

The warehouse is updated successfully.

---

### Second Run

```text
Batch BATCH-2026-10-21-01 already processed - skipping
New: 0
Updated: 0
Duplicates: 0
Invalid: 0
```

No changes are made to the warehouse on subsequent runs.

---

## Conclusion

This project demonstrates how an incremental ETL pipeline can safely process transaction data while preventing duplicate loads. By combining validation, upsert logic, checkpointing, and logging, the solution maintains data integrity and guarantees idempotent execution across multiple runs.