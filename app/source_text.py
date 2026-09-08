"""Separate certifying OCR from generated retrieval hints and index metadata."""
import hashlib
import re


def certifying_text(item: dict) -> str | None:
    # Legacy generated summaries used a reserved slot in the OCR table. The
    # guard remains necessary until old indexes have been reconciled.
    if item.get("chunk_index") == 9999 or item.get("doc_type") == "no_content":
        return None
    kind = item.get("source_kind") or "legacy"
    if kind not in {"legacy", "ocr"}:
        return None
    text = item.get("source_content")
    if text is None:
        text = item.get("content") or item.get("excerpt") or ""
    if not isinstance(text, str):
        raise ValueError("OCR source content must be text")
    if kind == "legacy":
        # Previous versions prepended derived date/type metadata to each OCR
        # chunk. Remove only the known generated envelope, never a text prefix
        # selected by a model. New writes store the raw OCR separately.
        header = f"Document: {item.get('title') or ''}\nType: {item.get('doc_type') or ''}\nDate: "
        if text.startswith(header):
            text = re.sub(r"^[^\n]*\n\n", "", text[len(header):], count=1)
    return text


def bind_document_context(item: dict, document_text: str) -> None:
    """Bind a chunk to a uniquely located part of already fetched original OCR."""
    content = certifying_text(item) or ""
    start = document_text.find(content) if content else -1
    unique = start >= 0 and document_text.find(content, start + 1) < 0
    item['_source_document_content'] = document_text
    item['source_context'] = {
        'document_id': item.get('document_id'),
        'digest': hashlib.sha256(document_text.encode()).hexdigest(),
        'start': start if unique else None,
        'end': start + len(content) if unique else None,
    }


def certified_document_context(item: dict, content: str) -> tuple[str, int] | None:
    """Recheck the source binding; persisted/model metadata cannot grant authority."""
    text, context = item.get('_source_document_content'), item.get('source_context')
    if not isinstance(text, str) or not isinstance(context, dict) or not content:
        return None
    start, end = context.get('start'), context.get('end')
    if (context.get('document_id') != item.get('document_id')
            or type(start) is not int or type(end) is not int
            or not 0 <= start < end <= len(text) or text[start:end] != content
            or text.find(content) != start or text.find(content, start + 1) >= 0
            or context.get('digest') != hashlib.sha256(text.encode()).hexdigest()):
        return None
    return text, start
