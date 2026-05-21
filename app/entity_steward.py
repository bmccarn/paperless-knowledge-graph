"""Conservative entity review steward.

The steward improves the duplicate-entity queue globally. It scores candidates
with deterministic signals, asks a bounded agent for ambiguous/high-value
judgment, and records review suggestions. It does not blindly merge risky graph
entities.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any

from app.config import settings
from app.embeddings import embeddings_store
from app.graph import graph_store
from app.strands_orchestrator import strands_orchestrator

logger = logging.getLogger(__name__)

HIGH_RISK_LABELS = {
    "Person",
    "Address",
    "MedicalResult",
    "Condition",
    "Contract",
    "InsurancePolicy",
    "FinancialItem",
    "DateEvent",
    "Event",
}

LOW_RISK_LABELS = {"Organization", "Product", "System", "DocumentRef", "Location"}
TERMINAL_DECISIONS = {"ignore", "split", "merged", "never_merge", "auto_merged"}
SUGGESTION_DECISIONS = {"suggest_merge", "suggest_split", "suggest_review"}


class EntitySteward:
    def __init__(self):
        self._running = False

    async def run_once(
        self,
        reason: str = "manual",
        limit: int | None = None,
        focus_uuid: str | None = None,
    ) -> dict[str, Any]:
        if self._running:
            return {"status": "skipped", "reason": "entity steward already running"}

        self._running = True
        try:
            decisions = await embeddings_store.get_entity_review_decisions()
            ignored_pairs = {
                tuple(sorted([d["left_uuid"], d["right_uuid"]]))
                for d in decisions
                if d["decision"] in TERMINAL_DECISIONS
            }
            candidates = await graph_store.get_entity_review_candidates(
                ignored_pairs,
                limit=limit or settings.entity_steward_candidate_limit,
            )
            if focus_uuid:
                candidates = [
                    c for c in candidates
                    if focus_uuid in {str(c.get("left", {}).get("uuid")), str(c.get("right", {}).get("uuid"))}
                ] or candidates[: min(10, len(candidates))]

            reviewed = []
            for candidate in candidates:
                deterministic = score_candidate(candidate)
                agent_review = None
                if should_ask_agent(deterministic):
                    agent_review = await strands_orchestrator.review_entity_candidate(candidate, deterministic)
                recommendation = choose_recommendation(deterministic, agent_review)
                decision = recommendation_to_decision(recommendation)
                note = json.dumps(
                    {
                        "reason": reason,
                        "deterministic": deterministic,
                        "agent": agent_review or {},
                    },
                    default=str,
                    separators=(",", ":"),
                )
                await embeddings_store.add_entity_review_decision(
                    candidate["left"]["uuid"],
                    candidate["right"]["uuid"],
                    decision,
                    note[:6000],
                )
                reviewed.append({
                    "pair": [candidate["left"]["uuid"], candidate["right"]["uuid"]],
                    "names": [candidate["left"]["name"], candidate["right"]["name"]],
                    "label": candidate.get("label"),
                    "decision": decision,
                    "recommendation": recommendation,
                    "deterministic": deterministic,
                    "agent": agent_review,
                })

            report = {
                "status": "completed",
                "reason": reason,
                "candidate_count": len(candidates),
                "reviewed_count": len(reviewed),
                "suggest_merge": sum(1 for r in reviewed if r["decision"] == "suggest_merge"),
                "suggest_split": sum(1 for r in reviewed if r["decision"] == "suggest_split"),
                "suggest_review": sum(1 for r in reviewed if r["decision"] == "suggest_review"),
                "reviewed": reviewed[:20],
            }
            logger.info(
                "Entity steward completed: %s candidates, %s merge suggestions, %s split suggestions",
                report["reviewed_count"],
                report["suggest_merge"],
                report["suggest_split"],
            )
            return report
        finally:
            self._running = False


def score_candidate(candidate: dict[str, Any]) -> dict[str, Any]:
    left = candidate.get("left") or {}
    right = candidate.get("right") or {}
    label = str(candidate.get("label") or "")
    left_name = str(left.get("name") or "")
    right_name = str(right.get("name") or "")
    left_norm = normalize_name(left_name)
    right_norm = normalize_name(right_name)
    fuzzy = float(candidate.get("score") or 0) / 100.0
    exact_name = left_norm == right_norm and bool(left_norm)
    compatible_type = bool(label)
    left_ids = extract_identifiers(left.get("properties") or {})
    right_ids = extract_identifiers(right.get("properties") or {})
    shared_identifiers = sorted(set(left_ids) & set(right_ids))
    risk = "high" if label in HIGH_RISK_LABELS else "low" if label in LOW_RISK_LABELS else "medium"

    score = 0.25 + 0.35 * fuzzy
    reasons = [f"name similarity {round(fuzzy * 100)}%"]
    if exact_name:
        score += 0.22
        reasons.append("normalized names match exactly")
    if compatible_type:
        score += 0.08
        reasons.append("entity labels are compatible")
    if shared_identifiers:
        score += 0.28
        reasons.append("shared identifier(s): " + ", ".join(shared_identifiers[:3]))
    if risk == "high":
        score -= 0.12
        reasons.append("high-risk entity type requires review")
    score = max(0.0, min(1.0, score))

    if score >= 0.86 and risk == "low":
        recommendation = "merge"
    elif score <= 0.5 or (risk == "high" and not shared_identifiers and not exact_name):
        recommendation = "split"
    else:
        recommendation = "review"

    return {
        "score": round(score, 3),
        "risk": risk,
        "label": label,
        "exact_name": exact_name,
        "name_similarity": round(fuzzy, 3),
        "shared_identifiers": shared_identifiers[:10],
        "recommendation": recommendation,
        "reasons": reasons,
    }


def should_ask_agent(deterministic: dict[str, Any]) -> bool:
    if deterministic["risk"] == "high":
        return True
    score = float(deterministic.get("score") or 0)
    return 0.55 <= score <= 0.94


def choose_recommendation(deterministic: dict[str, Any], agent_review: dict[str, Any] | None) -> str:
    det_rec = deterministic.get("recommendation") or "review"
    if not agent_review:
        return det_rec
    agent_rec = str(agent_review.get("recommendation") or "review").lower()
    confidence = float(agent_review.get("confidence") or 0)
    risk = str(agent_review.get("risk") or deterministic.get("risk") or "medium")
    if agent_rec == "merge" and confidence >= 0.82 and risk == "low":
        return "merge"
    if agent_rec == "split" and confidence >= 0.65:
        return "split"
    if det_rec == "merge" and confidence < 0.82:
        return "review"
    return "review" if agent_rec not in {"merge", "split"} else agent_rec


def recommendation_to_decision(recommendation: str) -> str:
    return {
        "merge": "suggest_merge",
        "split": "suggest_split",
        "review": "suggest_review",
    }.get(recommendation, "suggest_review")


def normalize_name(value: str) -> str:
    text = value.lower()
    text = re.sub(r"\b(inc|llc|l\.l\.c|co|company|corp|corporation|the)\b", " ", text)
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return " ".join(text.split())


def extract_identifiers(properties: dict[str, Any]) -> list[str]:
    identifiers = []
    for key, value in properties.items():
        key_l = str(key).lower()
        if key_l in {"uuid", "name", "title", "description"}:
            continue
        values = value if isinstance(value, list) else [value]
        for item in values:
            text = str(item)
            if not text or len(text) < 4:
                continue
            if any(signal in key_l for signal in ("policy", "account", "vin", "email", "phone", "ssn", "number", "id")):
                identifiers.append(f"{key_l}:{normalize_identifier(text)}")
    return [i for i in dict.fromkeys(identifiers) if len(i.split(":", 1)[-1]) >= 4]


def normalize_identifier(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9@.-]+", "", value).lower()


entity_steward = EntitySteward()
