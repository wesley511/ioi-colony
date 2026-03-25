import os
from datetime import datetime
from threading import Lock

LOG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "LOGS",
    "unresolved_products.log"
)

_lock = Lock()

def log_unresolved_product(raw_name: str, normalized_key: str, source: str = "unknown"):
    """
    Thread-safe unresolved product logger.
    Prevents duplicates and ensures stable writes.
    """

    timestamp = datetime.utcnow().isoformat()

    entry = f"{timestamp} | source={source} | raw='{raw_name}' | normalized='{normalized_key}'\n"

    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)

    with _lock:
        # Deduplication (avoid spam)
        if os.path.exists(LOG_PATH):
            with open(LOG_PATH, "r") as f:
                if normalized_key in f.read():
                    return  # already logged

        with open(LOG_PATH, "a") as f:
            f.write(entry)
