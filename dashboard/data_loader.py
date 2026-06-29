import pandas as pd
import glob
import os


def load(table_name):

    folder = f"data/{table_name}"

    csv_files = glob.glob(os.path.join(folder, "*.csv"))

    if not csv_files:
        raise FileNotFoundError(f"No CSV found in {folder}")

    return pd.read_csv(csv_files[0])


def load_executive():
    return load("executive_dashboard")


def load_plant_performance():
    return load("plant_performance")


def load_machine_health():
    return load("machine_health")


def load_failure_summary():
    return load("failure_summary")


def load_oee():
    return load("oee")
