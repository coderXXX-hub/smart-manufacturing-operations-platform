from dotenv import load_dotenv
import os

load_dotenv()

BOOTSTRAP_SERVER = os.getenv("BOOTSTRAP_SERVER")
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
TOPIC = os.getenv("TOPIC")

CSV_FILE = "../data/ai4i2020.csv"

SEND_INTERVAL_SECONDS = 1
