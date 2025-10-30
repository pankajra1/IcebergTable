#!/usr/bin/env bash
set -euo pipefail
SQL_FILE="/home/varish/Documents/table/create_fire_iceberg.sql"
PKG="org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.6.1"
if ! command -v spark-sql >/dev/null 2>&1; then
  echo "spark-sql not found. Please install Apache Spark and ensure spark-sql is on PATH." >&2
  exit 1
fi
spark-sql --packages "" -S -f ""
