from __future__ import annotations

from pathlib import Path
from datetime import datetime, timezone
from flask import Flask, jsonify, render_template, request

from scripts.colony_command import handle_query

app = Flask(__name__, template_folder="../templates")

BASE_DIR = Path(__file__).resolve().parent.parent
HEALTH_FILE = BASE_DIR / "HEALTH.md"
ALERTS_FILE = BASE_DIR / "ALERTS.md"
ANOMALIES_FILE = BASE_DIR / "ANOMALIES.md"


def read_text_file(path: Path) -> str:
    if not path.exists():
        return "not available"
    return path.read_text(encoding="utf-8", errors="ignore").strip()


def extract_bullets(markdown_text: str) -> list[str]:
    lines = []
    for raw in markdown_text.splitlines():
        line = raw.strip()
        if line.startswith("- "):
            lines.append(line[2:].strip())
    return lines


@app.get("/")
def index():
    return render_template("index.html")


@app.post("/ask")
def ask():
    payload = request.get_json(silent=True) or {}
    query = str(payload.get("query", "")).strip()
    answer = handle_query(query)
    return jsonify(
        {
            "query": query,
            "answer": answer,
            "answered_at": datetime.now(timezone.utc).isoformat(),
        }
    )


@app.get("/dashboard_snapshot")
def dashboard_snapshot():
    health_text = read_text_file(HEALTH_FILE)
    alerts_text = read_text_file(ALERTS_FILE)
    anomalies_text = read_text_file(ANOMALIES_FILE)

    snapshot = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "cards": {
            "summary": handle_query("summary"),
            "top_branch": handle_query("top branch"),
            "weakest_branch": handle_query("weakest branch"),
            "highest_sales": handle_query("highest sales"),
            "most_traffic": handle_query("most traffic"),
        },
        "health": {
            "raw": health_text,
            "bullets": extract_bullets(health_text),
        },
        "alerts": {
            "raw": alerts_text,
            "bullets": extract_bullets(alerts_text),
        },
        "anomalies": {
            "raw": anomalies_text,
            "bullets": extract_bullets(anomalies_text),
        },
        "quick_queries": [
            "summary",
            "top branch",
            "weakest branch",
            "highest sales",
            "most traffic",
            "show bena road",
            "show waigani",
            "alerts",
            "anomalies",
            "recommendations",
        ],
    }
    return jsonify(snapshot)


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8080, debug=False)
