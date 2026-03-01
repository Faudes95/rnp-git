#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

os.environ.setdefault("AUTH_ENABLED", "false")
os.environ.setdefault("ASYNC_EMBEDDINGS", "true")

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from fau_bot_core.service import SERVICE  # noqa: E402


@dataclass
class EvalCase:
    query: str
    expected_area: str
    expected_terms: List[str]
    area_filter: Optional[str] = None


CASES: List[EvalCase] = [
    EvalCase("litiasis obstructiva con sepsis y descompresion urgente", "LITIASIS", ["litiasis", "descompresion", "sepsis"]),
    EvalCase("prostatectomia riesgo oncologico ecog charlson", "ONCOLOGIA", ["prostata", "ecog", "charlson"]),
    EvalCase("nefrectomia sangrado perioperatorio seguimiento", "ONCOLOGIA", ["nefrectomia", "sangrado"]),
    EvalCase("hematuria evaluacion por riesgo", "DIAGNOSTICO", ["hematuria", "riesgo"]),
    EvalCase("infeccion urinaria complicada foco y cultivos", "INFECCIOSO", ["infeccion", "cultivos"]),
    EvalCase("vigilancia creatinina hemoglobina leucocitos plaquetas", "EPIDEMIOLOGIA", ["creatinina", "hemoglobina", "leucocitos"]),
    EvalCase("estancias prolongadas y ocupacion de camas", "OPERATIVO", ["estancia", "camas"]),
    EvalCase("gobernanza hitl auditoria para ia clinica", "GOBERNANZA", ["hitl", "auditoria"]),
]


def run_eval(limit: int = 5) -> dict:
    SERVICE.load_default_knowledge()
    hits_at_k = 0
    mrr_total = 0.0
    term_recall_total = 0.0
    details = []

    for case in CASES:
        rows = SERVICE.knowledge_search(case.query, area=case.area_filter, limit=limit)
        rank = None
        best_term_recall = 0.0
        for idx, row in enumerate(rows, start=1):
            area = str(row.get("area") or "")
            if rank is None and area == case.expected_area:
                rank = idx
            txt = f"{row.get('title','')} {row.get('preview','')}".lower()
            matched = sum(1 for t in case.expected_terms if t.lower() in txt)
            best_term_recall = max(best_term_recall, matched / float(len(case.expected_terms) or 1))

        if rank is not None:
            hits_at_k += 1
            mrr_total += 1.0 / float(rank)

        term_recall_total += best_term_recall
        details.append(
            {
                "query": case.query,
                "expected_area": case.expected_area,
                "rank_expected_area": rank,
                "hit": bool(rank is not None),
                "best_term_recall": round(best_term_recall, 4),
                "top_results": [
                    {
                        "area": r.get("area"),
                        "title": r.get("title"),
                        "score": r.get("score"),
                        "score_vector": r.get("score_vector"),
                        "score_lexical": r.get("score_lexical"),
                        "score_rerank": r.get("score_rerank"),
                    }
                    for r in rows[:3]
                ],
            }
        )

    total = len(CASES)
    metrics = {
        "total_cases": total,
        f"hit_rate_at_{limit}": round(hits_at_k / float(total or 1), 4),
        "mrr": round(mrr_total / float(total or 1), 4),
        "term_recall": round(term_recall_total / float(total or 1), 4),
        "details": details,
    }
    return metrics


def main() -> int:
    parser = argparse.ArgumentParser(description="Offline semantic retrieval evaluator")
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument("--assert-thresholds", action="store_true")
    parser.add_argument("--hit-threshold", type=float, default=0.80)
    parser.add_argument("--mrr-threshold", type=float, default=0.55)
    parser.add_argument("--term-threshold", type=float, default=0.55)
    args = parser.parse_args()

    payload = run_eval(limit=max(1, min(args.limit, 20)))
    print(json.dumps(payload, ensure_ascii=False, indent=2))

    if args.assert_thresholds:
        hit_rate = float(payload.get(f"hit_rate_at_{max(1, min(args.limit, 20))}") or 0.0)
        mrr = float(payload.get("mrr") or 0.0)
        term_recall = float(payload.get("term_recall") or 0.0)
        if hit_rate < args.hit_threshold or mrr < args.mrr_threshold or term_recall < args.term_threshold:
            return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
