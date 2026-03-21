import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from parse_whatsapp_sales import parse_sales_report, save_yaml


INPUT_FILE = PROJECT_ROOT / "INPUT" / "whatsapp_sales.txt"


def split_reports(text: str) -> list[str]:
    parts = text.split("TTC ")
    reports: list[str] = []

    for part in parts:
        part = part.strip()
        if not part:
            continue
        reports.append("TTC " + part)

    return reports


def main() -> None:
    path = INPUT_FILE

    if not path.exists():
        print(f"Missing input file: {path}")
        return

    text = path.read_text(encoding="utf-8")

    reports = split_reports(text)
    print(f"Found {len(reports)} reports")

    for i, report in enumerate(reports, 1):
        try:
            data = parse_sales_report(report)
            save_yaml(data)
            print(f"[OK] Report {i} processed")
        except Exception as e:
            print(f"[FAIL] Report {i}: {e}")


if __name__ == "__main__":
    main()
