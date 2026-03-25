from collections import Counter
from pathlib import Path

LOG_FILE = Path("LOGS/unresolved_products.log")

def report_top_unresolved(limit=20):
    if not LOG_FILE.exists():
        print("No unresolved log found.")
        return

    items = []

    with LOG_FILE.open() as f:
        for line in f:
            if "normalized='" in line:
                key = line.split("normalized='")[1].split("'")[0]
                items.append(key)

    counts = Counter(items)

    print(f"\nTop {limit} unresolved products:\n")
    for item, count in counts.most_common(limit):
        print(f"{item}: {count}")

if __name__ == "__main__":
    report_top_unresolved()
