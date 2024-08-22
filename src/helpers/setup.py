import os
from dotenv import load_dotenv


def load_env():
    load_dotenv()
    os.environ["ENVIRONMENT"] = os.environ.get("ENVIRONMENT", "local")
