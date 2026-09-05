#!/usr/bin/env python3
"""Offline reproductions of audit findings at caaaa5a.

Runs selected, unmodified function bodies from the checkout with in-memory
dependencies. No app imports, credentials, databases, or model calls are used.
These assert the observed defects; they are audit evidence, not regression tests.
After fixes, replace them with tests asserting the desired behavior.
"""

import ast
import asyncio
import hashlib
import logging
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

ROOT = Path(__file__).resolve().parents[2]


def load_bodies(filename, names, namespace, class_name=None):
    tree = ast.parse((ROOT / filename).read_text(), filename=filename)
    bodies = tree.body
    if class_name:
        bodies = next(n for n in bodies if isinstance(n, ast.ClassDef) and n.name == class_name).body
    selected = [n for n in bodies if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name in names]
    assert {n.name for n in selected} == set(names)
    exec(compile(ast.Module(body=selected, type_ignores=[]), filename, "exec"), namespace)
    return {n.name: namespace[n.name] for n in selected}


class CaptureCache:
    def __init__(self, response=None):
        self.keys = []
        self.response = response

    def get(self, key):
        self.keys.append(key)
        return self.response


def query_probe(cache):
    namespace = {
        "hashlib": hashlib,
        "Any": Any,
        "QUERY_CACHE_VERSION": "evidence-v15",
        "query_cache": cache,
    }
    load_bodies("app/cache.py", ["normalize_query_key"], namespace)
    methods = load_bodies("app/query.py", ["query_stream", "_active_model", "_llm_generate"], namespace, "QueryEngine")
    probe = type("QueryProbe", (), methods)()
    probe.model = "default-model"
    probe._model_override = None
    probe._normalize_mode = lambda mode: mode
    probe._is_broad_query = lambda question: False
    return probe


async def reproduce_query_model_race():
    probe = query_probe(CaptureCache())
    used_models = []

    async def fake_completion(**kwargs):
        used_models.append(kwargs["model"])
        return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content="{}"))])

    class StopProbe(Exception):
        pass

    async def fake_plan(*args):
        await probe._llm_generate("plan")
        raise StopProbe

    probe.client = SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(create=fake_completion)))
    probe._build_query_plan = fake_plan
    first = probe.query_stream("question A", model_override="model-A")
    second = probe.query_stream("question B", model_override="model-B")
    await anext(first)
    await anext(second)
    try:
        await anext(first)
    except StopProbe:
        pass
    await first.aclose()
    await second.aclose()
    assert used_models == ["model-B"], used_models
    print("REPRODUCED: query A requested model-A but its next model call used model-B")


async def reproduce_cache_collisions():
    cache = CaptureCache({"answer": "answer from model-A", "sources": []})
    probe = query_probe(cache)
    first = probe.query_stream("same question", model_override="model-A")
    second = probe.query_stream("same question", model_override="model-B")
    first_event, second_event = await anext(first), await anext(second)
    await first.aclose()
    await second.aclose()
    assert cache.keys[0] == cache.keys[1]
    assert first_event == second_event
    print("REPRODUCED: changing model reuses the same cached answer")

    cache = CaptureCache()
    probe = query_probe(cache)
    prefix = "x" * 50
    first = probe.query_stream("which one?", conversation_history=[{"role": "user", "content": prefix + " choose red"}])
    second = probe.query_stream("which one?", conversation_history=[{"role": "user", "content": prefix + " choose blue"}])
    await anext(first)
    await anext(second)
    await first.aclose()
    await second.aclose()
    assert cache.keys[0] == cache.keys[1]
    print("REPRODUCED: distinct conversation context after character 50 has an identical answer-cache key")


async def reproduce_sync_checkpoint(failed):
    stamps = [datetime(2026, 9, 4, hour, tzinfo=timezone.utc) for hour in range(4)]
    docs = [{"id": 1, "title": "first", "modified": stamps[1]}]
    processed_ids = []
    state = {"checkpoint": stamps[0]}

    class Clock:
        @staticmethod
        def now(tz):
            return stamps[3]

    async def get_last_sync():
        return state["checkpoint"]

    async def set_last_sync(value):
        state["checkpoint"] = value

    async def get_all_documents(modified_after=None):
        return [d.copy() for d in docs if modified_after is None or d["modified"] > modified_after]

    async def get_skip_tag_ids():
        return set()

    async def get_graph_ids():
        return set()

    async def process_document(doc):
        processed_ids.append(doc["id"])
        if failed:
            return {"doc_id": doc["id"], "status": "error", "error": "transient model failure"}
        docs.append({"id": 2, "title": "arrived during sync", "modified": stamps[2]})
        return {"doc_id": doc["id"], "status": "processed"}

    namespace = {
        "asyncio": asyncio, "time": time, "datetime": Clock, "timezone": timezone,
        "logger": logging.getLogger("audit"),
        "settings": SimpleNamespace(max_concurrent_docs=1),
        "paperless_client": SimpleNamespace(get_all_documents=get_all_documents, get_skip_tag_ids=get_skip_tag_ids,
            partition_indexable_documents=lambda docs, tags: (docs, [])),
        "embeddings_store": SimpleNamespace(get_last_sync=get_last_sync, set_last_sync=set_last_sync),
        "graph_store": SimpleNamespace(get_all_document_ids=get_graph_ids),
        "process_document": process_document,
    }
    sync = load_bodies("app/pipeline.py", ["sync_documents"], namespace)["sync_documents"]
    first = await sync()
    second = await sync()
    assert first["errors"] == int(failed)
    assert second["total"] == 0 and processed_ids == [1]
    if failed:
        print("REPRODUCED: failed document is omitted from the next incremental sync")
    else:
        print("REPRODUCED: document arriving during sync is omitted from the next incremental sync")


async def reproduce_stale_entity_uuid():
    nodes = {}
    creates = []

    class Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def run(self, query, **kwargs):
            async def single():
                found = nodes.get(kwargs["name"])
                return {"uuid": found} if found else None
            return SimpleNamespace(single=single)

    async def create_node(label, props):
        new_uuid = f"entity-{len(creates) + 1}"
        creates.append(new_uuid)
        nodes[props["name"]] = new_uuid
        return new_uuid

    namespace = {
        "_coerce_text": lambda value: value,
        "logger": logging.getLogger("audit"),
        "graph_store": SimpleNamespace(driver=SimpleNamespace(session=Session), create_node=create_node),
    }
    methods = load_bodies("app/entity_resolver.py", ["__init__", "resolve_generic"], namespace, "EntityResolver")
    resolver = type("ResolverProbe", (), methods)()
    resolver._neo4j_label = lambda entity_type: entity_type
    first = await resolver.resolve_generic("Test location", "Location", 1)
    nodes.clear()
    second = await resolver.resolve_generic("Test location", "Location", 1)
    assert first == second and not nodes and len(creates) == 1
    print("REPRODUCED: resolver returns a nonexistent cached UUID after graph clearing")


async def reproduce_cancel_overlap():
    gate = asyncio.Event()
    first_started = asyncio.Event()
    second_started = asyncio.Event()
    active = 0
    maximum = 0
    launched = []

    async def work(**kwargs):
        nonlocal active, maximum
        active += 1
        maximum = max(active, maximum)
        (first_started if active == 1 else second_started).set()
        await gate.wait()
        active -= 1
        return {"processed": 1, "errors": 0}

    async def steward(**kwargs):
        pass

    def launch(coro):
        task = asyncio.create_task(coro)
        launched.append(task)
        return task

    class HTTPException(Exception):
        def __init__(self, **kwargs):
            super().__init__(kwargs)

    namespace = {
        "asyncio": SimpleNamespace(Event=asyncio.Event, create_task=launch),
        "uuid": uuid, "datetime": datetime, "timezone": timezone, "time": time,
        "_tasks": {}, "_cancel_events": {}, "HTTPException": HTTPException,
        "app": SimpleNamespace(post=lambda *args, **kwargs: lambda fn: fn),
        "_make_progress_callback": lambda task_id: lambda *args: None,
        "invalidate_on_sync": lambda: None,
        "_clear_freshness_cache": lambda: None,
        "_schedule_task_cleanup": lambda task_id: None,
        "sync_documents": work,
        "entity_steward": SimpleNamespace(run_once=steward),
        "logger": logging.getLogger("audit"),
    }
    functions = load_bodies("app/main.py", ["_run_sync_task", "cancel_task"], namespace)
    first_id = await functions["_run_sync_task"]()
    await first_started.wait()
    await functions["cancel_task"](first_id)
    await functions["_run_sync_task"]()
    await second_started.wait()
    gate.set()
    await asyncio.gather(*launched)
    await asyncio.gather(*launched)
    assert maximum == 2 and namespace["_tasks"][first_id]["status"] == "completed"
    print("REPRODUCED: cancellation admits overlapping ingestion and is overwritten by completed")


async def main():
    await reproduce_query_model_race()
    await reproduce_cache_collisions()
    await reproduce_sync_checkpoint(failed=True)
    await reproduce_sync_checkpoint(failed=False)
    await reproduce_stale_entity_uuid()
    await reproduce_cancel_overlap()


if __name__ == "__main__":
    asyncio.run(main())
