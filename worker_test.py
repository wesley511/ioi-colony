import os
from datetime import datetime

SIGNALS_PATH = "SIGNALS/normalized"

def read_signal(file):
    with open(os.path.join(SIGNALS_PATH, file), "r") as f:
        return f.read()

def mark_processed(file):
    path = os.path.join(SIGNALS_PATH, file)
    with open(path, "r") as f:
        content = f.read()

    content = content.replace("status: new", "status: processed")

    with open(path, "w") as f:
        f.write(content)

def log(msg):
    with open("LOGS/worker_test.log", "a") as f:
        f.write(f"{datetime.now()} - {msg}\n")

def main():
    print("=== TEST RUN ===")

    for file in os.listdir(SIGNALS_PATH):
        if not file.endswith(".md"):
            continue

        content = read_signal(file)

        if "status: new" in content:
            print(f"Processing {file}")
            log(f"Processed {file}")
            mark_processed(file)

    print("=== DONE ===")

if __name__ == "__main__":
    main()
