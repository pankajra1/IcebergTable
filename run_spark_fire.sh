#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SQL_FILE="${SCRIPT_DIR}/create_fire_iceberg.sql"
PKG="org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.6.1"

if ! command -v spark-sql >/dev/null 2>&1; then
  echo "spark-sql not found. Please install Apache Spark and ensure spark-sql is on PATH." >&2
  exit 1
fi

echo "Running Spark SQL with Iceberg package: ${PKG}"
spark-sql --packages "${PKG}" -S -f "${SQL_FILE}"
