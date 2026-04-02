from __future__ import annotations

from typing import Any

try:
    from scripts.branch_resolution import legacy_branch_display, legacy_branch_stem
except ModuleNotFoundError:
    from branch_resolution import legacy_branch_display, legacy_branch_stem
from scripts.colony_fusion_analyzer import (
    build_branch_metrics,
    load_sales_signals,
    load_signals,
)


def load_colony_state() -> dict[str, Any]:
    staff = load_signals()
    sales = load_sales_signals()
    return build_branch_metrics(staff, sales)


def _fmt_money(value: float) -> str:
    return f"K{value:,.2f}"


def _normalize_text(value: str) -> str:
    return " ".join((value or "").strip().lower().replace("_", " ").split())


def _top_branch(metrics: dict[str, Any]) -> tuple[str, dict[str, Any]] | None:
    if not metrics:
        return None
    return max(metrics.items(), key=lambda x: x[1].get("fusion_score", 0.0))


def _weakest_branch(metrics: dict[str, Any]) -> tuple[str, dict[str, Any]] | None:
    if not metrics:
        return None
    return min(metrics.items(), key=lambda x: x[1].get("fusion_score", 0.0))


def _highest_sales(metrics: dict[str, Any]) -> tuple[str, dict[str, Any]] | None:
    if not metrics:
        return None
    return max(metrics.items(), key=lambda x: x[1].get("sales_total", 0.0))


def _most_traffic(metrics: dict[str, Any]) -> tuple[str, dict[str, Any]] | None:
    if not metrics:
        return None
    return max(metrics.items(), key=lambda x: x[1].get("transaction_total", 0))


def _find_branch_key(metrics: dict[str, Any], text: str) -> str | None:
    if not text:
        return None

    target = _normalize_text(text)
    if not target:
        return None

    normalized_map: dict[str, str] = {}
    for branch in metrics:
        normalized_map[_normalize_text(branch)] = branch
        normalized_map[_normalize_text(legacy_branch_stem(branch))] = branch
        normalized_map[_normalize_text(legacy_branch_display(branch))] = branch

    if target in normalized_map:
        return normalized_map[target]

    for normalized_branch, original_branch in normalized_map.items():
        if target in normalized_branch or normalized_branch in target:
            return original_branch

    target_tokens = set(target.split())
    best_branch: str | None = None
    best_score = 0

    for normalized_branch, original_branch in normalized_map.items():
        branch_tokens = set(normalized_branch.split())
        overlap = len(target_tokens & branch_tokens)
        if overlap > best_score:
            best_score = overlap
            best_branch = original_branch

    return best_branch if best_score > 0 else None


def _branch_detail(metrics: dict[str, Any], branch_name: str) -> str | None:
    branch_key = _find_branch_key(metrics, branch_name)
    if not branch_key:
        return None

    data = metrics[branch_key]
    lines = [
        f"Branch: {legacy_branch_display(branch_key)}",
        f"Fusion score: {data.get('fusion_score', 0.0):.2f}",
        f"Staff signals: {data.get('staff_signal_count', 0)}",
        f"Sales signals: {data.get('sales_signal_count', 0)}",
        f"Staff avg: {data.get('staff_strength_avg', 0.0):.2f}",
        f"Sales total: {_fmt_money(data.get('sales_total', 0.0))}",
        f"Cash sales: {_fmt_money(data.get('cash_sales_total', 0.0))}",
        f"EFTPOS sales: {_fmt_money(data.get('eftpos_sales_total', 0.0))}",
        f"Transactions: {data.get('transaction_total', 0)}",
    ]

    strongest_staff = list(data.get("staff_strength", {}).items())
    strongest_staff.sort(key=lambda x: (-x[1], x[0]))
    if strongest_staff:
        lines.append("")
        lines.append("Top staff:")
        for name, score in strongest_staff[:5]:
            lines.append(f"- {name}: {score:.2f}")

    strongest_sections = list(data.get("section_strength", {}).items())
    strongest_sections.sort(key=lambda x: (-x[1], x[0]))
    if strongest_sections:
        lines.append("")
        lines.append("Top sections:")
        for name, score in strongest_sections[:5]:
            lines.append(f"- {name}: {score:.2f}")

    issues = data.get("issues", [])
    if issues:
        lines.append("")
        lines.append("Issues:")
        for issue in issues[:8]:
            lines.append(f"- {issue}")

    return "\n".join(lines)


def _staff_performance_summary(metrics: dict[str, Any]) -> str:
    ranked = sorted(
        metrics.items(),
        key=lambda x: (
            -x[1].get("staff_strength_avg", 0.0),
            -x[1].get("staff_signal_count", 0),
            x[0],
        ),
    )

    lines = [
        "Staff Performance Summary:",
        "",
        "Note: this is based on staff signal/performance data, not verified clock-in attendance records.",
    ]

    for branch, data in ranked:
        lines.append(
            f"- {legacy_branch_display(branch)}: "
            f"staff avg {data.get('staff_strength_avg', 0.0):.2f}, "
            f"signals {data.get('staff_signal_count', 0)}, "
            f"fusion {data.get('fusion_score', 0.0):.2f}"
        )

    all_staff: list[tuple[str, str, float]] = []
    for branch, data in metrics.items():
        for name, score in data.get("staff_strength", {}).items():
            all_staff.append((branch, name, float(score)))

    all_staff.sort(key=lambda x: (-x[2], x[0], x[1]))
    if all_staff:
        lines.append("")
        lines.append("Top staff overall:")
        for branch, name, score in all_staff[:8]:
            lines.append(f"- {name} ({legacy_branch_display(branch)}): {score:.2f}")

    weak_sections: list[tuple[str, str, float]] = []
    for branch, data in metrics.items():
        for section, score in data.get("section_strength", {}).items():
            weak_sections.append((branch, section, float(score)))

    weak_sections.sort(key=lambda x: (x[2], x[0], x[1]))
    if weak_sections:
        lines.append("")
        lines.append("Weak sections overall:")
        for branch, section, score in weak_sections[:8]:
            lines.append(f"- {section} ({legacy_branch_display(branch)}): {score:.2f}")

    return "\n".join(lines)


def _overall_summary(metrics: dict[str, Any]) -> str:
    top = _top_branch(metrics)
    weak = _weakest_branch(metrics)
    sales = _highest_sales(metrics)
    traffic = _most_traffic(metrics)

    parts: list[str] = []
    if top:
        parts.append(f"Top branch: {legacy_branch_display(top[0])} ({top[1].get('fusion_score', 0.0):.2f})")
    if weak:
        parts.append(f"Weakest branch: {legacy_branch_display(weak[0])} ({weak[1].get('fusion_score', 0.0):.2f})")
    if sales:
        parts.append(f"Highest sales: {legacy_branch_display(sales[0])} ({_fmt_money(sales[1].get('sales_total', 0.0))})")
    if traffic:
        parts.append(f"Most traffic: {legacy_branch_display(traffic[0])} ({traffic[1].get('transaction_total', 0)} customers)")
    return "\n".join(parts)


def handle_query(query: str) -> str:
    q = _normalize_text(query)
    if not q:
        return (
            "Ask me about the colony, for example:\n"
            "- top branch\n"
            "- weakest branch\n"
            "- highest sales\n"
            "- most traffic\n"
            "- summary\n"
            "- show bena road"
        )

    metrics = load_colony_state()
    if not metrics:
        return "No colony data is available yet."

    q_words = set(q.split())
    branch_match = _find_branch_key(metrics, q)

    if branch_match and q_words.intersection({"staff", "performance", "attendance", "section", "sections"}):
        detail = _branch_detail(metrics, branch_match)
        return detail or f"I could not find a branch named '{branch_match}'."

    if (
        "attendance" in q_words
        or ("staff" in q_words and "summary" in q_words)
        or ("staff" in q_words and "performance" in q_words)
        or ("section" in q_words and "summary" in q_words)
        or ("sections" in q_words and "summary" in q_words)
    ):
        return _staff_performance_summary(metrics)

    if q.startswith("show "):
        branch_name = q[len("show "):].strip()
        detail = _branch_detail(metrics, branch_name)
        return detail or f"I could not find a branch named '{branch_name}'."

    if q.startswith("branch "):
        branch_name = q[len("branch "):].strip()
        detail = _branch_detail(metrics, branch_name)
        return detail or f"I could not find a branch named '{branch_name}'."

    if branch_match:
        detail = _branch_detail(metrics, branch_match)
        return detail or f"I could not find a branch named '{branch_match}'."

    if q in {"summary", "overview", "branch summary", "overall summary"}:
        return _overall_summary(metrics)

    if ("top" in q_words and "branch" in q_words) or ("best" in q_words and "branch" in q_words):
        result = _top_branch(metrics)
        if not result:
            return "No branch data available."
        branch, data = result
        return (
            f"Top branch is {legacy_branch_display(branch)}.\n"
            f"Fusion score: {data.get('fusion_score', 0.0):.2f}\n"
            f"Sales total: {_fmt_money(data.get('sales_total', 0.0))}\n"
            f"Transactions: {data.get('transaction_total', 0)}"
        )

    if "branch" in q_words and q_words.intersection({"weakest", "weak", "lowest", "worst"}):
        result = _weakest_branch(metrics)
        if not result:
            return "No branch data available."
        branch, data = result
        return (
            f"Weakest branch is {legacy_branch_display(branch)}.\n"
            f"Fusion score: {data.get('fusion_score', 0.0):.2f}\n"
            f"Sales total: {_fmt_money(data.get('sales_total', 0.0))}\n"
            f"Transactions: {data.get('transaction_total', 0)}"
        )

    if (
        ("highest" in q_words and "sales" in q_words)
        or ("top" in q_words and "sales" in q_words)
        or q == "sales"
    ):
        result = _highest_sales(metrics)
        if not result:
            return "No sales data available."
        branch, data = result
        return (
            f"Highest sales branch is {legacy_branch_display(branch)}.\n"
            f"Sales total: {_fmt_money(data.get('sales_total', 0.0))}\n"
            f"Cash sales: {_fmt_money(data.get('cash_sales_total', 0.0))}\n"
            f"EFTPOS sales: {_fmt_money(data.get('eftpos_sales_total', 0.0))}"
        )

    if q_words.intersection({"traffic", "customers", "transactions"}):
        result = _most_traffic(metrics)
        if not result:
            return "No traffic data available."
        branch, data = result
        return (
            f"Highest traffic branch is {legacy_branch_display(branch)}.\n"
            f"Transactions: {data.get('transaction_total', 0)}\n"
            f"Sales total: {_fmt_money(data.get('sales_total', 0.0))}"
        )

    return (
        "I understand these commands and queries:\n"
        "- top branch\n"
        "- weakest branch\n"
        "- highest sales\n"
        "- most traffic\n"
        "- summary\n"
        "- show bena road\n"
        "- show waigani\n"
        "- waigani staff performance\n"
        "- attendance summary"
    )
