import random
from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

from fastavro import writer


OUTPUT_PATH = Path("/home/varish/Documents/table/fire.avro")


# Avro schema compatible with Apache Iceberg export expectations
# - salary uses logicalType decimal (bytes) precision 9, scale 2
# - date_of_joining uses logicalType date
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


def random_department_and_title(rng: random.Random) -> tuple[str, str]:
    dept = rng.choice(DEPARTMENTS)
    title = rng.choice(JOB_TITLES_BY_DEPT[dept])
    return dept, title


def random_salary(rng: random.Random) -> Decimal:
    # Generate salary between 30,000 and 150,000 with cents
    raw = rng.uniform(30000, 150000)
    return Decimal(raw).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def random_join_date(rng: random.Random) -> date:
    # Date within the past 10 years
    today = date.today()
    ten_years_days = 365 * 10 + 2  # leap years buffer
    delta_days = rng.randint(0, ten_years_days)
    return today - timedelta(days=delta_days)


def build_records(num_records: int, seed: int | None = 42):
    rng = random.Random(seed)
    records = []
    for employee_id in range(1, num_records + 1):
        full_name = random_name(rng)
        department, job_title = random_department_and_title(rng)
        salary = random_salary(rng)
        joining = random_join_date(rng)
        # Make about 85% active
        is_active = rng.random() < 0.85

        record = {
            "employee_id": employee_id,
            "full_name": full_name,
            "department": department,
            "job_title": job_title,
            # fastavro supports Decimal for decimal logicalType and date for date logicalType
            "salary": salary,
            "date_of_joining": joining,
            "is_active": is_active,
        }
        records.append(record)
    return records


def main() -> None:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    records = build_records(50)
    with OUTPUT_PATH.open("wb") as out:
        writer(out, SCHEMA, records, validator=None)
    print(f"Wrote {len(records)} records to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()


