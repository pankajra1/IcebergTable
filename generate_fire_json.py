import csv
import json
import random
from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Optional, Tuple, Iterator, Dict, Any


OUTPUT_DIR = Path(__file__).resolve().parent


SCHEMA = {
    "type": "record",
    "namespace": "com.example.hr",
    "name": "fire",
    "fields": [
        {"name": "employee_id", "type": "int"},
        {"name": "full_name", "type": "string"},
        {"name": "department", "type": "string"},
        {"name": "job_title", "type": "string"},
        {
            "name": "salary",
            "type": {
                "type": "bytes",
                "logicalType": "decimal",
                "precision": 9,
                "scale": 2,
            },
        },
        {"name": "date_of_joining", "type": {"type": "int", "logicalType": "date"}},
        {"name": "is_active", "type": "boolean"},
    ],
}


FIRST_NAMES = [
    "Ava",
    "Liam",
    "Olivia",
    "Noah",
    "Emma",
    "Elijah",
    "Sophia",
    "Lucas",
    "Mia",
    "Mason",
    "Isabella",
    "Ethan",
    "Amelia",
    "Logan",
    "Harper",
    "James",
    "Evelyn",
    "Benjamin",
    "Charlotte",
    "Henry",
]

LAST_NAMES = [
    "Johnson",
    "Smith",
    "Williams",
    "Brown",
    "Jones",
    "Garcia",
    "Miller",
    "Davis",
    "Rodriguez",
    "Martinez",
    "Hernandez",
    "Lopez",
    "Gonzalez",
    "Wilson",
    "Anderson",
    "Thomas",
    "Taylor",
    "Moore",
    "Jackson",
    "Martin",
]

DEPARTMENTS = ["Engineering", "HR", "Finance", "Sales"]

JOB_TITLES_BY_DEPT = {
    "Engineering": [
        "Software Engineer",
        "Senior Software Engineer",
        "DevOps Engineer",
        "Data Engineer",
        "QA Engineer",
        "Engineering Manager",
    ],
    "HR": [
        "HR Generalist",
        "Recruiter",
        "HR Manager",
        "Compensation Analyst",
    ],
    "Finance": [
        "Financial Analyst",
        "Senior Accountant",
        "Controller",
        "Payroll Specialist",
        "Finance Manager",
    ],
    "Sales": [
        "Sales Representative",
        "Account Executive",
        "Sales Manager",
        "Customer Success Manager",
    ],
}


def random_name(rng: random.Random) -> str:
    return f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}"


def random_department_and_title(rng: random.Random) -> Tuple[str, str]:
    dept = rng.choice(DEPARTMENTS)
    title = rng.choice(JOB_TITLES_BY_DEPT[dept])
    return dept, title


def random_salary(rng: random.Random) -> Decimal:
    raw = rng.uniform(30000, 150000)
    return Decimal(raw).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def random_join_date(rng: random.Random) -> date:
    today = date.today()
    ten_years_days = 365 * 10 + 2
    delta_days = rng.randint(0, ten_years_days)
    return today - timedelta(days=delta_days)


def build_records(num_records: int, seed: Optional[int] = 42) -> Iterator[Dict[str, Any]]:
    rng = random.Random(seed)
    for employee_id in range(1, num_records + 1):
        full_name = random_name(rng)
        department, job_title = random_department_and_title(rng)
        salary = random_salary(rng)
        joining = random_join_date(rng)
        is_active = rng.random() < 0.85
        yield {
            "employee_id": employee_id,
            "full_name": full_name,
            "department": department,
            "job_title": job_title,
            # Encode salary as string for JSON/CSV; Avro will use decimal logical type
            "salary": str(salary),
            # Encode date as ISO string for JSON/CSV; Avro will use logicalType date
            "date_of_joining": joining.isoformat(),
            "is_active": is_active,
        }


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    avsc_path = OUTPUT_DIR / "fire.avsc"
    jsonl_path = OUTPUT_DIR / "fire.jsonl"
    csv_path = OUTPUT_DIR / "fire.csv"

    # Write Avro schema (.avsc)
    with avsc_path.open("w", encoding="utf-8") as f:
        json.dump(SCHEMA, f, indent=2)

    # Write JSON Lines with 50 records (human-friendly, easy to inspect)
    records = list(build_records(50))
    with jsonl_path.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec) + "\n")

    # Write CSV (optional convenience)
    fieldnames = [
        "employee_id",
        "full_name",
        "department",
        "job_title",
        "salary",
        "date_of_joining",
        "is_active",
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

    print(f"Wrote schema to {avsc_path}")
    print(f"Wrote 50 records to {jsonl_path} and {csv_path}")


if __name__ == "__main__":
    main()


