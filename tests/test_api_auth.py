import unittest
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import Depends, FastAPI
from fastapi.routing import APIRoute
from starlette.responses import StreamingResponse

from tests.runtime import configure_test_environment
configure_test_environment()
from app import auth
from app.config import Settings
from app.main import app


class ApiAuthTests(unittest.IsolatedAsyncioTestCase):
    def test_route_completeness(self):
        public = set()
        paths = set()
        for route in app.routes:
            self.assertIsInstance(route, APIRoute, f"Unreviewed route: {route}")
            paths.add(route.path)
            calls = [dependency.call for dependency in route.dependant.dependencies]
            for method in route.methods:
                expected = (auth.public_route if route.path in {"/health", "/readyz"}
                            else auth.require_read if (
                                method == "GET" and route.path not in {"/logs", "/logs/stream"}
                                or method == "POST" and route.path in {"/query", "/query/stream"})
                            else auth.require_admin)
                self.assertEqual([call for call in calls if call in {
                    auth.public_route, auth.require_read, auth.require_admin}], [expected],
                    f"{method} {route.path}")
                if expected is auth.public_route:
                    public.add((method, route.path))
        self.assertEqual(public, {("GET", "/health"), ("GET", "/readyz")})
        self.assertTrue({"/docs", "/redoc", "/openapi.json", "/docs/oauth2-redirect"} <= paths)

    def test_settings(self):
        with patch.dict("os.environ", {"KG_AUTH_MODE": "enforce", "KG_READ_API_KEYS": "a, b", "KG_ADMIN_API_KEYS": "c"}):
            settings = Settings(_env_file=None)
            self.assertEqual(settings.kg_auth_mode, "enforce")
            self.assertEqual(settings.kg_read_api_keys, "a, b")
            self.assertEqual(settings.kg_admin_api_keys, "c")
        self.assertEqual(Settings.model_fields["kg_auth_mode"].default, "off")
        with self.assertRaises(ValueError):
            Settings(kg_auth_mode="typo", _env_file=None)

    async def test_modes_keys_status_matrix_and_streams(self):
        fixture = FastAPI()
        async def result():
            return {"ok": True}
        async def stream():
            async def events():
                yield "data: ok\n\n"
            return StreamingResponse(events(), media_type="text/event-stream")
        for scope, dependency in [("read", auth.require_read), ("admin", auth.require_admin)]:
            fixture.add_api_route(f"/{scope}/{{item}}", result, dependencies=[Depends(dependency)])
            fixture.add_api_route(f"/{scope}/{{item}}/stream", stream, methods=["POST"], dependencies=[Depends(dependency)])
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=fixture), base_url="http://test") as client:
            for mode in ["off", "warn", "enforce"]:
                with patch.multiple(auth.settings, kg_auth_mode=mode, kg_read_api_keys=" read, ,read2,shared ", kg_admin_api_keys=" admin,admin2,shared "):
                    for scope in ["read", "admin"]:
                        for key in [None, "", "invalid", "read", "read2", "admin", "admin2", "shared"]:
                            predicted = 401 if key in [None, "", "invalid"] else 403 if scope == "admin" and key.startswith("read") else 200
                            for suffix in ["", "/stream"]:
                                with self.subTest(mode=mode, scope=scope, key=key, stream=suffix), patch.object(auth.logger, "warning") as log:
                                    response = await client.request("POST" if suffix else "GET", f"/{scope}/private-id{suffix}?secret=query", headers={} if key is None else {"X-KG-API-Key": key})
                                    self.assertEqual(response.status_code, predicted if mode == "enforce" else 200)
                                    if response.status_code == 200 and suffix:
                                        self.assertEqual(response.text, "data: ok\n\n")
                                    if mode == "warn" and predicted != 200:
                                        log.assert_called_once_with("route=%s scope=%s status=%s", f"/{scope}/{{item}}{suffix}", scope, predicted)
                                    else:
                                        log.assert_not_called()
            with patch.multiple(auth.settings, kg_auth_mode="enforce", kg_read_api_keys="", kg_admin_api_keys=""):
                self.assertEqual((await client.get("/read/x")).status_code, 401)
            with patch.multiple(auth.settings, kg_auth_mode="enforce", kg_read_api_keys="read", kg_admin_api_keys="admin"):
                self.assertEqual((await client.get("/read/x", headers=[("X-KG-API-Key", "read"), ("X-KG-API-Key", "admin")])).status_code, 401)

    async def test_real_docs_and_streams_reject_before_execution(self):
        with patch.multiple(auth.settings, kg_auth_mode="enforce", kg_read_api_keys="read", kg_admin_api_keys="admin"):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
                for path in ["/docs", "/redoc", "/openapi.json", "/docs/oauth2-redirect", "/logs/stream"]:
                    self.assertEqual((await client.get(path)).status_code, 401)
                self.assertEqual((await client.get("/logs/stream", headers={"X-KG-API-Key": "read"})).status_code, 403)
                self.assertEqual((await client.post("/query/stream", json={"question": "test"})).status_code, 401)
                for path in ["/docs", "/redoc", "/openapi.json", "/docs/oauth2-redirect"]:
                    self.assertEqual((await client.get(path, headers={"X-KG-API-Key": "read"})).status_code, 200)

    async def test_conversation_queries_require_admin_before_any_work(self):
        from app import main
        async def events(*args, **kwargs):
            yield {"type": "complete", "answer": "ok", "finalization": {"disposition": "answered"}}
        with (
            patch.multiple(auth.settings, kg_auth_mode="enforce", kg_read_api_keys="read", kg_admin_api_keys="admin"),
            patch.object(main.conversations, "get_conversation_history", new_callable=AsyncMock, return_value=[]) as history,
            patch.object(main.conversations, "add_message", new_callable=AsyncMock) as save,
            patch.object(main.query_engine, "query", new_callable=AsyncMock, return_value={"answer": "ok"}) as query,
            patch.object(main.query_engine, "query_stream", side_effect=events),
        ):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
                for path in ["/query", "/query/stream"]:
                    response = await client.post(path, headers={"X-KG-API-Key": "read"}, json={"question": "test", "conversation_id": "saved"})
                    self.assertEqual(response.status_code, 403)
                    history.assert_not_called()
                    save.assert_not_called()
                    query.assert_not_called()
                for path in ["/query", "/query/stream"]:
                    self.assertEqual((await client.post(path, headers={"X-KG-API-Key": "read"}, json={"question": "test"})).status_code, 200)
                    save.assert_not_called()
                for path in ["/query", "/query/stream"]:
                    self.assertEqual((await client.post(path, headers={"X-KG-API-Key": "admin"}, json={"question": "test", "conversation_id": "saved"})).status_code, 200)
                self.assertEqual(save.await_count, 4)

    def test_comparison_uses_constant_time_for_every_key(self):
        with patch.object(auth.hmac, "compare_digest", wraps=auth.hmac.compare_digest) as compare:
            self.assertTrue(auth._matches("one", "one,two,three"))
            self.assertEqual(compare.call_count, 3)
            self.assertTrue(all(isinstance(arg, bytes) for call in compare.call_args_list for arg in call.args))
