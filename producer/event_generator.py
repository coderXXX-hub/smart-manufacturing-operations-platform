import random
from datetime import datetime

PLANTS = [
    "San Jose",
    "Austin",
    "Phoenix"
]

LINES = [
    "Assembly-A",
    "Assembly-B",
    "Assembly-C"
]

WORK_CENTERS = [
    "WC-10",
    "WC-20",
    "WC-30",
    "WC-40"
]

SHIFTS = [
    "Day",
    "Evening",
    "Night"
]


def generate_event(row):

    failure_type = "NONE"

    if row["TWF"] == 1:
        failure_type = "Tool Wear"

    elif row["HDF"] == 1:
        failure_type = "Heat Dissipation"

    elif row["PWF"] == 1:
        failure_type = "Power Failure"

    elif row["OSF"] == 1:
        failure_type = "Overstrain"

    elif row["RNF"] == 1:
        failure_type = "Random Failure"

    event = {

        "event_time": datetime.utcnow().isoformat(),

        "plant": random.choice(PLANTS),

        "production_line": random.choice(LINES),

        "work_center": random.choice(WORK_CENTERS),

        "shift": random.choice(SHIFTS),

        "operator_id": f"EMP{random.randint(1000,1099)}",

        "production_order": f"PO{random.randint(100000,999999)}",

        "machine_id": row["Product ID"],

        "product_type": row["Type"],

        "air_temperature": row["Air temperature [K]"],

        "process_temperature": row["Process temperature [K]"],

        "rpm": row["Rotational speed [rpm]"],

        "torque": row["Torque [Nm]"],

        "tool_wear": row["Tool wear [min]"],

        "machine_failure": row["Machine failure"],

        "failure_type": failure_type

    }

    return event
