"""Explicit, bounded connectivity probes. Never send a chat/inference request."""
import asyncio
import json
import os

import httpx


async def read_json(client, url, headers):
    async with client.stream("GET", url, headers=headers) as response:
        if response.status_code in (401, 403):
            return "unauthorized", None
        if response.status_code != 200:
            return "unavailable", None
        body = bytearray()
        async for chunk in response.aiter_bytes():
            body.extend(chunk)
            if len(body) > 65536:
                return "invalid", None
        try:
            return "ok", json.loads(body)
        except (ValueError, UnicodeError):
            return "invalid", None


async def probe_gateway(client):
    base = os.getenv("WATCHALERT_TOOL_GATEWAY_URL", "").strip()
    token = os.getenv("WATCHALERT_AGENT_INTERNAL_TOKEN", "").strip()
    if not base or not token:
        return "unconfigured"
    status, data = await read_json(client, base.rstrip("/") + "/api/w8t/internal/agent/health", {"X-WatchAlert-Agent-Token": token})
    if status == "ok" and not (isinstance(data, dict) and data.get("code") == 200 and data.get("data") == {"status": "ok"}):
        return "invalid"
    return status


async def probe_model(client, model):
    if not model.apiKey:
        # Environment fallback may target another provider: do not forward its
        # secret to DeepSeek, and do not confuse configured with verified.
        return "unchecked" if os.getenv("OPENAI_API_KEY") else "unconfigured"
    if model.provider != "deepseek" or model.baseURL.rstrip("/") not in ("https://api.deepseek.com", "https://api.deepseek.com/v1"):
        return "invalid"
    status, data = await read_json(client, "https://api.deepseek.com/models", {"Authorization": "Bearer " + model.apiKey})
    if status != "ok":
        return status
    if not isinstance(data, dict) or not isinstance(data.get("data"), list):
        return "invalid"
    return "ok" if any(isinstance(item, dict) and item.get("id") == model.model for item in data["data"]) else "missing_model"


async def bounded(probe):
    try:
        return await asyncio.wait_for(probe, timeout=7)
    except Exception:
        # Never leak URLs, auth headers or raw provider error text.
        return "unavailable"


async def diagnose(model, sdk_available):
    async with httpx.AsyncClient(timeout=5, follow_redirects=False) as client:
        gateway, provider = await asyncio.gather(bounded(probe_gateway(client)), bounded(probe_model(client, model)))
    return {"checks": [{"id": "sdk", "status": "ok" if sdk_available else "unavailable"},
                       {"id": "gateway", "status": gateway}, {"id": "model", "status": provider}]}
