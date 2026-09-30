"""Explicit route scopes and staged API-key enforcement."""
import hmac
import logging

from fastapi import HTTPException, Request, Security
from fastapi.security import APIKeyHeader

from app.config import settings

logger = logging.getLogger(__name__)
api_key_header = APIKeyHeader(name="X-KG-API-Key", auto_error=False)


def _matches(provided: str, configured: str) -> bool:
    matched = False
    for key in configured.split(","):
        key = key.strip()
        if key:
            matched |= hmac.compare_digest(provided.encode("utf-8"), key.encode("utf-8"))
    return matched


def _authorize(request: Request, scope: str) -> None:
    if settings.kg_auth_mode == "off":
        return
    values = request.headers.getlist("x-kg-api-key")
    provided = values[0] if len(values) == 1 else ""
    admin = _matches(provided, settings.kg_admin_api_keys)
    read = _matches(provided, settings.kg_read_api_keys)
    status = 200 if admin or (read and scope == "read") else (403 if read else 401)
    if settings.kg_auth_mode == "warn":
        if status != 200:
            logger.warning("route=%s scope=%s status=%s", request.scope["route"].path, scope, status)
        return
    if status != 200:
        raise HTTPException(status_code=status, detail="Forbidden" if status == 403 else "Missing or invalid API key")


async def require_read(request: Request, _key: str | None = Security(api_key_header)) -> None:
    _authorize(request, "read")


async def require_admin(request: Request, _key: str | None = Security(api_key_header)) -> None:
    _authorize(request, "admin")


async def public_route() -> None:
    """Explicit marker for the two public health routes."""
