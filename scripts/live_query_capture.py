"""Private application-model call capture, including streaming and embeddings."""
import asyncio
import json
import math
import os
import time
from types import SimpleNamespace

from scripts.live_query_evaluation import sha256


class ModelCallBudgetExceeded(RuntimeError):
    pass


def serialized(value):
    if hasattr(value, 'model_dump'):
        return value.model_dump(mode='json')
    if isinstance(value, dict):
        return {key: serialized(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [serialized(item) for item in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise TypeError('Unrecognized model artifact type')


class ModelCapture:
    """One create call per artifact pair; SDK retries must already be disabled.

    Application retries/fallbacks become separate calls. Upstream proxy attempts
    remain unknown. Transport headers, credentials and environment values are never copied.
    """
    INPUT_FIELDS = frozenset({
        'model', 'messages', 'input', 'response_format', 'stream', 'stream_options',
        'temperature', 'top_p', 'tools', 'tool_choice', 'parallel_tool_calls',
        'max_tokens', 'max_completion_tokens', 'reasoning_effort', 'extra_body',
        'encoding_format', 'dimensions', 'stop', 'seed', 'frequency_penalty',
        'presence_penalty', 'n', 'logprobs', 'top_logprobs',
    })

    def __init__(self, directory, *, max_calls, seconds):
        if (type(max_calls) is not int or max_calls < 1
                or type(seconds) not in (int, float) or not math.isfinite(seconds) or seconds <= 0):
            raise ValueError('Positive call and elapsed budgets required')
        self.directory = directory
        self.max_calls = max_calls
        self.deadline = time.monotonic() + seconds
        self.attempts = []
        self.hashes = {}
        self.exhausted = False
        self.pending = {}
        self.stream_chunks = {}
        self.failures = {}

    def write(self, name, payload):
        encoded = json.dumps(serialized(payload), ensure_ascii=False, allow_nan=False).encode()
        path = self.directory / name
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, 'wb') as output:
            output.write(encoded)
        self.hashes[name] = sha256(encoded)

    def start(self, kind, kwargs):
        if len(self.attempts) >= self.max_calls or time.monotonic() >= self.deadline:
            self.exhausted = True
            raise ModelCallBudgetExceeded('Evaluation model-call budget exhausted')
        # Fail unknown body options rather than silently omit material controls.
        unknown = set(kwargs) - self.INPUT_FIELDS - {'extra_headers'}
        if unknown:
            raise ValueError('Unreviewed model request options')
        request = serialized({key: value for key, value in kwargs.items() if key in self.INPUT_FIELDS})
        attempt = {'index': len(self.attempts), 'kind': kind, 'request': request}
        self.write(f"model-{attempt['index']:03d}-input.json", attempt)
        self.attempts.append(attempt)
        self.pending[attempt['index']] = time.monotonic()
        return attempt

    def finish(self, attempt, *, response=None, chunks=None, error=None):
        index = attempt['index']
        if index not in self.pending:
            return
        output = {**attempt, 'elapsed_seconds': time.monotonic() - self.pending[index],
                  'status': 'failed' if error else 'completed'}
        if error:
            output['exception_type'] = error
        if response is not None:
            output['response'] = serialized(response)
        if chunks is not None:
            output['chunks'] = chunks
        self.write(f'model-{index:03d}-output.json', output)
        if error:
            self.failures[index] = error
        self.pending.pop(index)
        self.stream_chunks.pop(index, None)

    async def create(self, kind, operation, kwargs):
        attempt = self.start(kind, kwargs)
        try:
            async with asyncio.timeout_at(self.deadline):
                result = await operation(**kwargs)
            if kwargs.get('stream'):
                return CapturedStream(self, attempt, result)
            error = (stream_completion_error(attempt, [serialized(result)])
                     if kind.rsplit(':', 1)[-1] == 'chat' else None)
            self.finish(attempt, response=result, error=error)
            return result
        except BaseException as exc:
            self.finish(attempt, error=type(exc).__name__)
            raise

    def close_pending(self):
        # Called after the owned query task is cancelled/joined. Retain incomplete
        # streams instead of declaring a missing output to be successful.
        for attempt in self.attempts:
            if attempt['index'] in self.pending:
                self.finish(attempt, chunks=self.stream_chunks.get(attempt['index']), error='IncompleteStream')

    def require_complete(self):
        if not self.attempts or self.pending or self.failures or self.exhausted:
            raise ValueError('Model execution is incomplete or contains a failed attempt')


def stream_completion_error(attempt, chunks):
    """Iterator EOF is not a provider finish signal (the SDK may invent end_turn)."""
    count = attempt['request'].get('n', 1)
    if type(count) is not int or count < 1:
        return 'InvalidChoiceCount'
    terminal = {}
    for chunk in chunks:
        if not isinstance(chunk, dict) or not isinstance(chunk.get('choices', []), list):
            return 'InvalidProviderChoices'
        for choice in chunk.get('choices', []):
            if not isinstance(choice, dict):
                return 'InvalidProviderChoices'
            index, reason = choice.get('index'), choice.get('finish_reason')
            if type(index) is not int or not 0 <= index < count:
                return 'UnexpectedProviderChoice'
            if reason is not None:
                if index in terminal and terminal[index] != reason:
                    return 'ConflictingFinishReasons'
                terminal[index] = reason
    if set(terminal) != set(range(count)):
        return 'MissingProviderTermination'
    if any(reason not in {'stop', 'tool_calls', 'function_call'} for reason in terminal.values()):
        return 'IncompleteProviderTermination'
    return None


class CapturedStream:
    def __init__(self, capture, attempt, stream):
        self.capture, self.attempt, self.stream = capture, attempt, stream
        self.iterator = stream.__aiter__()
        self.chunks = []
        capture.stream_chunks[attempt['index']] = self.chunks

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            async with asyncio.timeout_at(self.capture.deadline):
                item = await self.iterator.__anext__()
            self.chunks.append(serialized(item))
            return item
        except StopAsyncIteration:
            error = stream_completion_error(self.attempt, self.chunks)
            self.capture.finish(self.attempt, chunks=self.chunks, error=error)
            raise
        except BaseException as exc:
            self.capture.finish(self.attempt, chunks=self.chunks, error=type(exc).__name__)
            raise

    async def close(self):
        try:
            await self.stream.close()
        finally:
            self.capture.finish(self.attempt, chunks=self.chunks, error='IncompleteStream')


class CapturedClient:
    """Proxy only request creation; preserve the configured client's ownership."""
    def __init__(self, client, capture, *, label, bypass_cache=False):
        if client.max_retries != 0:
            raise ValueError('Disable OpenAI SDK retries before evaluation capture')
        self.client = client

        def request_options(kwargs):
            if not bypass_cache:
                return kwargs
            extra = dict(kwargs.get('extra_body') or {})
            if 'cache' in extra:
                raise ValueError('Existing request cache settings require separate review')
            return {**kwargs, 'extra_body': {**extra, 'cache': {'no-cache': True, 'no-store': True}}}

        async def chat_create(**kwargs):
            return await capture.create(label + ':chat', client.chat.completions.create, request_options(kwargs))

        async def embedding_create(**kwargs):
            return await capture.create(label + ':embedding', client.embeddings.create, request_options(kwargs))

        self.chat = SimpleNamespace(completions=SimpleNamespace(create=chat_create))
        self.embeddings = SimpleNamespace(create=embedding_create)

    async def close(self):
        await self.client.close()
