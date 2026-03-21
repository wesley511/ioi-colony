import re
import yaml
from datetime import datetime
from pathlib import Path


def slugify(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    return value.strip("_")


def extract_value(pattern, text, default=0.0, flags=re.IGNORECASE | re.MULTILINE):
    match = re.search(pattern, text, flags)
    if not match:
        return default
    raw = match.group(1).replace(",", "").strip()
    return float(raw)


def extract_text(pattern, text, default=None, flags=re.IGNORECASE | re.MULTILINE):
    match = re.search(pattern, text, flags)
    return match.group(1).strip() if match else default


def parse_date(text: str) -> str:
    # Supports:
    # Date: 21/03/26
    # Date: Saturday 21/03/26
    match = re.search(r"Date:\s*(?:[A-Za-z]+\s+)?(\d{2}/\d{2}/\d{2})", text, re.IGNORECASE)
    if not match:
        raise ValueError("Could not find report date")
    raw_date = datetime.strptime(match.group(1), "%d/%m/%y")
    return raw_date.strftime("%Y-%m-%d")


def parse_branch(text: str) -> str:
    match = re.search(r"TTC\s+(.*?)\s+Branch", text, re.IGNORECASE)
    if not match:
        return "unknown"
    return slugify(match.group(1))


def parse_tills(text: str):
    tills = []

    till_pattern = re.compile(
        r"Till#\s*(\d+)\s*:\s*(.*?)\n"
        r"(.*?)(?=\nTill#\s*\d+\s*:|\nTOTALS\b)",
        re.IGNORECASE | re.DOTALL
    )

    for match in till_pattern.finditer(text):
        till_id = int(match.group(1))
        till_name = slugify(match.group(2))
        block = match.group(3)

        cashier = extract_text(r"Cashier:\s*(.+)", block)
        assistant = extract_text(r"Assistant:\s*(.+)", block)

        cash = extract_value(r"T/Cash:\s*K\s*([\d,\.]+)", block)
        card = extract_value(r"T/Card:\s*K\s*([\d,\.]+)", block)
        z_reading = extract_value(r"Z/Reading:\s*K\s*([\d,\.]+)", block)

        balance_text = extract_text(r"Balance:\s*(.+)", block, default="")
        variance = 0.0
        variance_reason = None

        if balance_text and balance_text.lower() not in {"ok", "none"}:
            amount_match = re.search(r"K\s*([\d,\.]+)", balance_text, re.IGNORECASE)
            if amount_match:
                variance = float(amount_match.group(1).replace(",", ""))
            reason_match = re.search(r"\((.*?)\)", balance_text)
            if reason_match:
                variance_reason = slugify(reason_match.group(1))

        till_entry = {
            "till_id": till_id,
            "name": till_name,
            "cashier": slugify(cashier) if cashier else None,
            "cash": cash,
            "card": card,
            "z_reading": z_reading,
            "variance": variance,
        }

        if assistant:
            till_entry["assistant"] = slugify(assistant)
        if variance_reason:
            till_entry["variance_reason"] = variance_reason

        tills.append(till_entry)

    return tills


def parse_totals(text: str):
    return {
        "cash": extract_value(r"Total\s+Ca(?:sh|ssh):\s*K\s*([\d,\.]+)", text),
        "card": extract_value(r"Total\s+Card:\s*K\s*([\d,\.]+)", text),
        "sales": extract_value(r"Total\s+Sales:\s*K\s*([\d,\.]+)", text),
    }


def parse_customers(text: str):
    customers = {
        "traffic": int(extract_value(r"Main\s+Door:\s*(\d+)", text)),
        "conversion_rate": extract_value(r"Conversion\s+Rate\s*:?\s*([\d\.]+)%?", text) / 100,
    }

    served = re.search(r"(?:Guest/\s*)?Customer\s+served:\s*(\d+)", text, re.IGNORECASE)
    other_entry = re.search(r"Other\s+Entry/Guests:\s*(\d+)", text, re.IGNORECASE)

    if served:
        customers["served"] = int(served.group(1))
    if other_entry:
        customers["other_entry_guests"] = int(other_entry.group(1))

    return customers


def parse_performance(text: str):
    perf = {}

    sales_per_customer = re.search(r"Sale\s+per\s+customer\s*[:=]\s*K?\s*([\d\.]+)", text, re.IGNORECASE)
    if sales_per_customer:
        perf["sales_per_customer"] = float(sales_per_customer.group(1))

    sales_per_labor_hour = re.search(r"Sales\s+Per\s+Labor\s+Hour\s*[:=]\s*K?\s*([\d\.]+)", text, re.IGNORECASE)
    if sales_per_labor_hour:
        perf["sales_per_labor_hour"] = float(sales_per_labor_hour.group(1))

    sales_per_staff = re.search(r"Sales\s+Per\s+Staff\s*:\s*K?\s*([\d\.]+)", text, re.IGNORECASE)
    if sales_per_staff:
        perf["sales_per_staff"] = float(sales_per_staff.group(1))

    return perf


def parse_supervisor_section(text: str):
    supervisor = {
        "name": None,
        "issues": {
            "staffing": None,
            "stock": None,
            "pricing": None,
            "escalations": None,
        }
    }

    name = extract_text(r"Supervisor:\s*(.+)", text)
    if name:
        supervisor["name"] = slugify(name)

    staffing = extract_text(r"Staffing\s+issues:\s*(.+)", text)
    stock = extract_text(r"Stock\s+issues\s+affecting\s+sales:\s*(.+)", text)
    pricing = extract_text(r"Pricing\s+or\s+system\s+issues:\s*(.+)", text)

    if staffing and staffing.lower() != "none":
        supervisor["issues"]["staffing"] = staffing
    if stock and stock.lower() != "none":
        supervisor["issues"]["stock"] = stock
    if pricing and pricing.lower() != "none":
        supervisor["issues"]["pricing"] = pricing

    escalations_match = re.search(
        r"Exceptions\s+escalated\s+to\s+Ops\s+Manager:\s*(.*?)(?:Supervisor\s+confirmation:|$)",
        text,
        re.IGNORECASE | re.DOTALL
    )
    if escalations_match:
        escalations = escalations_match.group(1).strip()
        if escalations:
            supervisor["issues"]["escalations"] = escalations

    return supervisor


def derive_flags(data):
    flags = {}

    total_variance = sum(t.get("variance", 0.0) for t in data.get("tills", []))
    flags["variance_flag"] = "low" if total_variance <= 50 else "high"

    total_sales = data.get("totals", {}).get("sales", 0.0)
    flags["performance_flag"] = "strong" if total_sales >= 10000 else "moderate"

    return flags


def parse_sales_report(text):
    data = {
        "type": "sales_day_end",
        "branch": parse_branch(text),
        "date": parse_date(text),
        "tills": parse_tills(text),
        "totals": parse_totals(text),
        "customers": parse_customers(text),
        "performance": parse_performance(text),
        "supervisor": parse_supervisor_section(text),
    }

    data["flags"] = derive_flags(data)
    return data


def save_yaml(data):
    filename = f"{data['branch']}_sales_{data['date']}.yaml"
    path = Path("SIGNALS/normalized") / filename

    with open(path, "w") as f:
        yaml.dump(data, f, sort_keys=False, allow_unicode=True)

    print(f"Saved: {path}")


if __name__ == "__main__":
    print("Paste WhatsApp report below. Press CTRL+D when done:\n")
    text = ""
    try:
        while True:
            text += input() + "\n"
    except EOFError:
        pass

    parsed = parse_sales_report(text)
    save_yaml(parsed)
