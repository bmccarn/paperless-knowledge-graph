"""Capture native stage semantics and their actual model calls in isolation."""
from contextlib import asynccontextmanager, contextmanager
import logging
import time

from scripts.eval_source_audit import CURRENT_ATTEMPT, AttemptLog, configure_proxy_cache, write_private
from scripts.live_query_capture import CapturedClient
from scripts.live_query_evaluation import sha256


@contextmanager
def capture_stages(orchestrator, capture, directory):
    """Wrap the existing orchestrator; do not replace its planning or verdicts.

    The caller owns the isolated process and must join all query tasks before exit.
    Stage attempts and transport attempts are separate ledgers, never added
    together when reporting call count or tokens.
    """
    from app import strands_orchestrator as native_module

    native_text, native_model, native_agent = orchestrator._text_agent, orchestrator._model, native_module.Agent
    attempts, hashes = [], {}

    def write(name, value):
        write_private(directory / name, value)
        hashes[name] = sha256((directory / name).read_bytes())

    async def text(name, system_prompt, prompt, *, response_format=None):
        attempt = {'index': len(attempts), 'name': name, 'system_prompt': system_prompt,
                   'prompt': prompt, 'response_format': response_format, 'diagnostics': []}
        attempts.append(attempt)
        write(f"stage-{attempt['index']:03d}-input.json", attempt)
        token = CURRENT_ATTEMPT.set(attempt)
        started = time.monotonic()
        try:
            attempt['response'] = await native_text(name, system_prompt, prompt, response_format=response_format)
            return attempt['response']
        except BaseException as exc:
            attempt['exception_type'] = type(exc).__name__
            raise
        finally:
            attempt['elapsed_seconds'] = time.monotonic() - started
            try:
                write(f"stage-{attempt['index']:03d}-output.json", attempt)
            finally:
                CURRENT_ATTEMPT.reset(token)

    class CapturedAgent(native_agent):
        def __init__(self, *args, **kwargs):
            kwargs['retry_strategy'] = None
            super().__init__(*args, **kwargs)

        async def invoke_async(self, *args, **kwargs):
            result = await super().invoke_async(*args, **kwargs)
            attempt = CURRENT_ATTEMPT.get()
            if attempt is None:
                raise ValueError('Native agent escaped stage capture')
            attempt['native_result'] = {'text': str(result), 'message': result.message,
                                        'stop_reason': result.stop_reason}
            return result

    def model(**kwargs):
        instance = configure_proxy_cache(native_model(**kwargs), 'bypass')
        native_client = instance._get_client

        @asynccontextmanager
        async def client():
            attempt = CURRENT_ATTEMPT.get()
            if attempt is None:
                raise ValueError('Native model escaped stage capture')
            # Preserve the pinned adapter's per-invocation client lifecycle.
            async with native_client() as configured:
                yield CapturedClient(configured, capture, label=f"stage-{attempt['index']:03d}")

        instance._get_client = client
        return instance

    handler = AttemptLog()
    logger = logging.getLogger('app.strands_orchestrator')
    prior_level = logger.level
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    native_module.Agent = CapturedAgent
    orchestrator._text_agent, orchestrator._model = text, model
    try:
        yield {'attempts': attempts, 'hashes': hashes}
    finally:
        native_module.Agent = native_agent
        orchestrator._text_agent, orchestrator._model = native_text, native_model
        logger.removeHandler(handler)
        logger.setLevel(prior_level)
