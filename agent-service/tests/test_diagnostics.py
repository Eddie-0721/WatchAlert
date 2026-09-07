import unittest
from types import SimpleNamespace
from unittest.mock import patch
import httpx
from fastapi import HTTPException
from app.diagnostics import probe_model, probe_gateway
from app.main import DiagnosticsRequest, diagnostics


class DiagnosticTests(unittest.IsolatedAsyncioTestCase):
    async def test_deepseek_models_probe_no_inference(self):
        calls = []
        def handler(request):
            calls.append(request)
            return httpx.Response(200, json={"data": [{"id": "chosen-model"}]})
        model = SimpleNamespace(provider="deepseek", baseURL="https://api.deepseek.com/v1", model="chosen-model", apiKey="test-key")
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            self.assertEqual(await probe_model(client, model), "ok")
            model.model = "missing"
            self.assertEqual(await probe_model(client, model), "missing_model")
            model.baseURL = "https://evil.example"
            self.assertEqual(await probe_model(client, model), "invalid")
        self.assertEqual(len(calls), 2)
        self.assertTrue(all(r.method == "GET" and str(r.url) == "https://api.deepseek.com/models" for r in calls))

    async def test_gateway_requires_real_authenticated_response(self):
        def handler(request):
            self.assertEqual(request.headers["X-WatchAlert-Agent-Token"], "internal")
            return httpx.Response(200, json={"code": 200, "data": {"status": "ok"}})
        with patch.dict("os.environ", {"WATCHALERT_TOOL_GATEWAY_URL": "http://gateway", "WATCHALERT_AGENT_INTERNAL_TOKEN": "internal"}):
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                self.assertEqual(await probe_gateway(client), "ok")
            with self.assertRaises(HTTPException):
                await diagnostics(DiagnosticsRequest(), "wrong-token")

    async def test_provider_errors_not_returned(self):
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(401,text="sensitive provider message"))) as client:
            model = SimpleNamespace(provider="deepseek", baseURL="https://api.deepseek.com", model="any", apiKey="test-key")
            self.assertEqual(await probe_model(client, model), "unauthorized")
