"""Separate certifying OCR from generated retrieval hints and index metadata."""
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
