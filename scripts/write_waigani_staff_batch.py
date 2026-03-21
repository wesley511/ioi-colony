#!/usr/bin/env python3

from pathlib import Path
import re
import sys

DATE = "2026-03-21"
DAY = "Saturday"
SOURCE = "TTC Waigani"
OUTDIR = Path("SIGNALS/normalized")

OUTDIR.mkdir(parents=True, exist_ok=True)

FORCE = False
START_ID = 4301

records = [
    {"staff_name": "milford", "signal_type": "productivity_signal", "staff_id": "STAFF-WAI-MILFORD", "section": "boys pants, shorts, army mix, t-shirt", "products": "boys pants, shorts, army mix, t-shirt", "items_moved": 312, "assisting_count": 18, "description": "Milford strong Saturday performance", "confidence": 0.96, "opportunity_score": 10},
    {"staff_name": "priscilla", "signal_type": "customer_engagement", "staff_id": "STAFF-WAI-PRISCILLA", "section": "mixed", "products": "mixed", "items_moved": 302, "assisting_count": 33, "description": "Priscilla strong movement and engagement", "confidence": 0.97, "opportunity_score": 10},
    {"staff_name": "grace", "signal_type": "productivity_signal", "staff_id": "STAFF-WAI-GRACE", "section": "mixed", "products": "mixed", "items_moved": 180, "assisting_count": 12, "description": "Grace active Saturday floor support", "confidence": 0.88, "opportunity_score": 8},
    {"staff_name": "kimson", "signal_type": "productivity_signal", "staff_id": "STAFF-WAI-KIMSON", "section": "mixed", "products": "mixed", "items_moved": 175, "assisting_count": 11, "description": "Kimson active Saturday floor support", "confidence": 0.88, "opportunity_score": 8},
    {"staff_name": "stanley", "signal_type": "productivity_signal", "staff_id": "STAFF-WAI-STANLEY", "section": "mixed", "products": "mixed", "items_moved": 178, "assisting_count": 12, "description": "Stanley active Saturday floor support", "confidence": 0.88, "opportunity_score": 8},
    {"staff_name": "nim", "signal_type": "productivity_signal", "staff_id": "STAFF-WAI-NIM", "section": "mixed", "products": "mixed", "items_moved": 168, "assisting_count": 10, "description": "Nim active Saturday floor support", "confidence": 0.87, "opportunity_score": 8},
    {"staff_name": "rebecca", "signal_type": "productivity_signal", "staff_id": "STAFF-WAI-REBECCA", "section": "mixed", "products": "mixed", "items_moved": 170, "assisting_count": 10, "description": "Rebecca active Saturday floor support", "confidence": 0.87, "opportunity_score": 8},
    {"staff_name": "xeena", "signal_type": "productivity_signal", "staff_id": "STAFF-WAI-XEENA", "section": "mixed", "products": "mixed", "items_moved": 165, "assisting_count": 10, "description": "Xeena active Saturday floor support", "confidence": 0.87, "opportunity_score": 8},
    {"staff_name": "gassi", "signal_type": "productivity_signal", "staff_id": "STAFF-WAI-GASSI", "section": "mixed", "products": "mixed", "items_moved": 160, "assisting_count": 9, "description": "Gassi active Saturday floor support", "confidence": 0.86, "opportunity_score": 8},
    {"staff_name": "florida", "signal_type": "productivity_signal", "staff_id": "STAFF-WAI-FLORIDA", "section": "mixed", "products": "mixed", "items_moved": 158, "assisting_count": 9, "description": "Florida active Saturday floor support", "confidence": 0.86, "opportunity_score": 8},
    {"staff_name": "debrah", "signal_type": "productivity_signal", "staff_id": "STAFF-WAI-DEBRAH", "section": "mixed", "products": "mixed", "items_moved": 155, "assisting_count": 9, "description": "Debrah active Saturday floor support", "confidence": 0.86, "opportunity_score": 8},
    {"staff_name": "epu", "signal_type": "productivity_signal", "staff_id": "STAFF-WAI-EPU", "section": "mixed", "products": "mixed", "items_moved": 152, "assisting_count": 8, "description": "Epu active Saturday floor support", "confidence": 0.86, "opportunity_score": 8},
    {"staff_name": "bethsian", "signal_type": "productivity_signal", "staff_id": "STAFF-WAI-BETHSIAN", "section": "mixed", "products": "mixed", "items_moved": 150, "assisting_count": 8, "description": "Bethsian active Saturday floor support", "confidence": 0.86, "opportunity_score": 8},
    {"staff_name": "sabilla", "signal_type": "productivity_signal", "staff_id": "STAFF-WAI-SABILLA", "section": "mixed", "products": "mixed", "items_moved": 149, "assisting_count": 8, "description": "Sabilla active Saturday floor support", "confidence": 0.86, "opportunity_score": 8},
    {"staff_name": "matthew", "signal_type": "productivity_signal", "staff_id": "STAFF-WAI-MATTHEW", "section": "mixed", "products": "mixed", "items_moved": 148, "assisting_count": 8, "description": "Matthew active Saturday floor support", "confidence": 0.86, "opportunity_score": 8},
    {"staff_name": "khay", "signal_type": "productivity_signal", "staff_id": "STAFF-WAI-KHAY", "section": "mixed", "products": "mixed", "items_moved": 145, "assisting_count": 8, "description": "Khay active Saturday floor support", "confidence": 0.85, "opportunity_score": 8},
]

def slug(text):
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")

written = []
skipped = []

for i, r in enumerate(records):
    filename = f"waigani_staff_{slug(r['staff_name'])}_{slug(r['signal_type'])}_{DATE}.md"
    path = OUTDIR / filename

    if path.exists() and not FORCE:
        skipped.append(path)
        continue

    signal_id = f"SIG-{DATE}-{START_ID + i}"

    content = f"""signal_id: {signal_id}
date: {DATE}
day: {DAY}
source_type: staff_report
source_name: {SOURCE}
category: staff
signal_type: {r['signal_type']}
staff_id: {r['staff_id']}
section: {r['section']}
products: {r['products']}
items_moved: {r['items_moved']}
assisting_count: {r['assisting_count']}
description: {r['description']}
confidence: {r['confidence']}
opportunity_score: {r['opportunity_score']}
status: new
"""

    path.write_text(content)

    if not path.exists() or path.stat().st_size == 0:
        print("ERROR writing:", path)
        sys.exit(1)

    written.append(path)

print(f"WROTE {len(written)} FILES")
for p in written:
    print(p)

print(f"SKIPPED {len(skipped)} FILES")
for p in skipped:
    print(p)
