"""Conservative entity review steward.

The steward improves the duplicate-entity queue globally. It scores candidates
with deterministic signals, asks a bounded agent for ambiguous/high-value
judgment, and records review suggestions. It does not blindly merge risky graph
entities.
"""

from __future__ import annotations

import json
import math
import logging
from collections import defaultdict
from typing import Any

from app.config import settings
from app.embeddings import embeddings_store
from app.entity_decisions import EntityMergeProhibited
from app.entity_policy import (AUTO_MERGE_REVIEW_METHOD, LOW_RISK_LABELS, TERMINAL_DECISIONS, auto_merge_blocker,
                               extract_identifiers, name_key)
from app.entity_resolver import AutoMergeBlocked, entity_resolver
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

SUGGESTION_DECISIONS = {"suggest_merge", "suggest_split", "suggest_review", "suggest_auto_merge"}


class EntitySteward:
    def __init__(self):
        self._running = False

    @property
    def running(self) -> bool:
        return self._running

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
                # Legacy suggestions have unknown actors and immutable history.
                # Reconsideration requires explicit review, not background upsert.
                or (d.get("provenance") or "legacy_unknown") == "legacy_unknown"
            }
            auto = {}
            if settings.entity_steward_auto_merge != "off":
                auto = await self._auto_merge_pass(reason, decisions)
            candidates = await graph_store.get_entity_review_candidates(
                # The pass's own rows stay the newest for its pairs on the review page.
                ignored_pairs | auto.pop("handled", set()),
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
                    provenance="automated_suggestion", review_method="entity_steward",
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
                **auto,
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

    async def _auto_merge_pass(self, reason: str, decisions: list[dict]) -> dict[str, Any]:
        """Exact-duplicate low-risk pairs: shadow records them, "on" merges them.

        The agent can only block. At most `max_per_run` pairs reach the agent,
        least recently reviewed first, so repeated runs move through the backlog.
        """
        mode = settings.entity_steward_auto_merge
        cap = settings.entity_steward_auto_merge_max_per_run
        report: dict[str, Any] = {"auto_merge_mode": mode, "auto_merged": 0, "would_auto_merge": 0,
                                  "auto_merge_blocked": defaultdict(int), "auto_merge_groups_over_two": 0}
        handled: set[tuple[str, str]] = set()
        last_reviewed = {}
        for row in decisions:
            if row.get("provenance") == "automated_suggestion":
                pair = tuple(sorted([row["left_uuid"], row["right_uuid"]]))
                last_reviewed[pair] = max(last_reviewed.get(pair, ""), str(row.get("created_at") or ""))
        pairs = []
        for label in sorted(LOW_RISK_LABELS):
            groups = defaultdict(list)
            for node in await graph_store.get_entities_by_type(label):
                if isinstance(node.get("name"), str) and node["name"].strip() and isinstance(node.get("uuid"), str):
                    groups[name_key(node["name"], label)].append(node)
            for group in groups.values():
                if len(group) > 2:
                    report["auto_merge_groups_over_two"] += 1
                elif len(group) == 2:
                    pairs.append((label, *sorted(group, key=lambda node: node["uuid"])))
        pairs.sort(key=lambda item: last_reviewed.get((item[1]["uuid"], item[2]["uuid"]), ""))

        async def suggest(pair, decision, payload):
            note = json.dumps({"reason": reason, **payload}, default=str, separators=(",", ":"))
            await embeddings_store.add_entity_review_decision(
                *pair, decision, note[:6000], provenance="automated_suggestion", review_method="entity_steward")
            handled.add(pair)

        agent_calls = 0
        for label, left, right in pairs:
            pair = (left["uuid"], right["uuid"])
            # Snapshot pre-filter; the resolver checks every rule again on fresh reads.
            blocker = auto_merge_blocker({"labels": [label], "properties": left}, {"labels": [label], "properties": right},
                                         label, 2)
            if not blocker:
                check = await entity_resolver.auto_merge_check(*pair)
                blocker = check["blocker"]
            if blocker:
                report["auto_merge_blocked"][blocker] += 1
                continue
            if agent_calls >= cap:
                report["auto_merge_blocked"]["cap"] += 1
                handled.add(pair)  # a later run reviews it; the regular pass must not refresh its timestamp
                continue
            agent_calls += 1
            nodes = {left["uuid"]: left, right["uuid"]: right}
            candidate = {"label": label,
                         "left": {"uuid": check["primary_uuid"], "name": nodes[check["primary_uuid"]]["name"],
                                  "properties": nodes[check["primary_uuid"]]},
                         "right": {"uuid": check["duplicate_uuid"], "name": nodes[check["duplicate_uuid"]]["name"],
                                   "properties": nodes[check["duplicate_uuid"]]}}
            facts = {"mode": mode, "label": label, "primary_uuid": check["primary_uuid"],
                     "duplicate_uuid": check["duplicate_uuid"], "gate": "exact name_key pair passed every rule"}
            try:
                agent_review = await strands_orchestrator.review_entity_candidate(
                    candidate, {"label": label, "risk": "low", "auto_merge_gate": facts["gate"]})
            except Exception as e:
                logger.warning("Auto-merge agent review failed for %s: %s", pair, e)
                agent_review = None
            payload = {"deterministic": {"risk": "low", "label": label}, "auto_merge": facts, "agent": agent_review or {}}
            if not agent_approves_auto_merge(agent_review):
                report["auto_merge_blocked"]["agent"] += 1
                await suggest(pair, "suggest_review", payload)
                continue
            if mode != "on":
                report["would_auto_merge"] += 1
                await suggest(pair, "suggest_auto_merge", payload)
                continue
            note = json.dumps({"reason": reason, **payload}, default=str, separators=(",", ":"))[:6000]
            try:
                await entity_resolver.merge_entities(check["primary_uuid"], check["duplicate_uuid"],
                                                     review_method=AUTO_MERGE_REVIEW_METHOD, note=note)
            except EntityMergeProhibited as e:
                report["auto_merge_blocked"]["prohibited"] += 1
                await suggest(pair, "suggest_review", {**payload, "auto_merge_error": str(e)})
                continue
            except AutoMergeBlocked as e:
                report["auto_merge_blocked"][str(e)] += 1
                continue
            report["auto_merged"] += 1
            handled.add(pair)
        report["auto_merge_blocked"] = dict(report["auto_merge_blocked"])
        logger.info("Entity steward auto-merge (%s): %s merged, %s would merge, blocked %s",
                    mode, report["auto_merged"], report["would_auto_merge"], report["auto_merge_blocked"])
        return {**report, "handled": handled}


def score_candidate(candidate: dict[str, Any]) -> dict[str, Any]:
    left = candidate.get("left") or {}
    right = candidate.get("right") or {}
    label = str(candidate.get("label") or "")
    left_name = str(left.get("name") or "")
    right_name = str(right.get("name") or "")
    left_norm = name_key(left_name, label)
    right_norm = name_key(right_name, label)
    fuzzy = float(candidate.get("score") or 0) / 100.0
    exact_name = left_norm == right_norm and bool(left_norm)
    compatible_type = bool(label)
    left_ids = extract_identifiers(left.get("properties") or {})
    right_ids = extract_identifiers(right.get("properties") or {})
    shared_identifiers = sorted(
        f"{key}:{value}" for key in left_ids.keys() & right_ids.keys()
        for value in set(left_ids[key]) & set(right_ids[key])
    )
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
    if not isinstance(agent_review, dict) or not agent_review:
        return "review" if should_ask_agent(deterministic) else det_rec
    agent_rec = str(agent_review.get("recommendation") or "review").lower()
    if isinstance(agent_review.get("confidence"), bool):
        return "review"
    try:
        confidence = float(agent_review.get("confidence") or 0)
    except (TypeError, ValueError):
        return "review"
    if not math.isfinite(confidence) or not 0 <= confidence <= 1:
        return "review"
    risk = str(agent_review.get("risk") or "unknown").lower()
    if agent_rec == "merge" and confidence >= 0.82 and risk == "low" and deterministic.get("risk") == "low":
        return "merge"
    if agent_rec == "split" and confidence >= 0.65:
        return "split"
    return "review"


def agent_approves_auto_merge(review: Any) -> bool:
    """Only a confident, low-risk merge verdict passes; anything else blocks."""
    if not isinstance(review, dict):
        return False
    confidence = review.get("confidence")
    return (str(review.get("recommendation")).lower() == "merge" and str(review.get("risk")).lower() == "low"
            and type(confidence) in (int, float) and math.isfinite(confidence) and 0.82 <= confidence <= 1)


def recommendation_to_decision(recommendation: str) -> str:
    return {
        "merge": "suggest_merge",
        "split": "suggest_split",
        "review": "suggest_review",
    }.get(recommendation, "suggest_review")


entity_steward = EntitySteward()
