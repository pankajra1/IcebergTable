# IcebergTable

Sample data and scripts to generate Avro files and demo Apache Iceberg with Spark.

## Contents
- create_fire_iceberg.sql: Create and populate Iceberg table (writes Avro data files)
- run_spark_fire.sh: Runs `spark-sql` with the Iceberg runtime to execute the SQL
- fire.avsc: Avro schema (Iceberg-compatible)
- generate_fire_avro.py: Write Avro using `fastavro`
- generate_fire_json.py: Generate `.avsc` and JSON/CSV sample data

## Prerequisites
- Python 3.9+
- pip (and optionally a virtual environment)
- To run the Spark + Iceberg demo:
  - Apache Spark 3.4+ (ensure `spark-sql` is on `PATH`)
  - Internet access to download `org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.6.1`

## Setup
```bash
git clone <this-repo-url>
cd IcebergTable

# (optional but recommended)
python3 -m venv .venv
source .venv/bin/activate

pip install -r requirements.txt
```

## Generate schema and JSON/CSV
Writes `fire.avsc`, `fire.jsonl`, and `fire.csv` into this repository directory.
```bash
python3 generate_fire_json.py
```

## Generate Avro file
Writes `fire.avro` into this repository directory.
```bash
python3 generate_fire_avro.py
```

## Run Spark + Iceberg demo (optional)
This will create an Iceberg table in a repo-local warehouse directory at `warehouse/iceberg`.
```bash
./run_spark_fire.sh
```

Notes:
- The SQL config sets a local Hadoop catalog at `warehouse/iceberg` (relative path).
- If `spark-sql` is not found, install Apache Spark and ensure it is on `PATH`.

## Clean up
```bash
rm -rf warehouse
rm -f fire.avro fire.avsc fire.jsonl fire.csv
```
