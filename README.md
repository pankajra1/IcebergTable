# IcebergTable

## Contents
- create_fire_iceberg.sql: Create and populate Iceberg table (Avro data files)
- run_spark_fire.sh: Runner for spark-sql + Iceberg runtime
- fire.avsc: Avro schema (Iceberg-compatible)
- generate_fire_avro.py: Write Avro with fastavro (optional)
- generate_fire_json.py: Generate .avsc and JSON/CSV sample data

## Quick start (Spark)
```bash
./run_spark_fire.sh
```
This creates a local Iceberg table at `/home/varish/Documents/table/iceberg/fire`.
