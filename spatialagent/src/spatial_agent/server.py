import json
import logging

from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from .config import settings
from .executor.mcp_client import MCPClient
from .executor.retry import MaxRetriesExceeded, retry_loop
from .executor.tool_picker import pick_tool
from .models.llm import LLMClient
from .models.registry import detect_available_models, select_model
from .notify.lakehouse import notify_lakehouse
from .planner.prompts import build_error_prompt, build_spatial_prompt, build_analytics_prompt
from .planner.schema import SchemaBuilder
from .planner.sql_gen import extract_sql, validate_sql, generate_sql
from .router.intent import classify
from .router.llm_search import fuzzy_column_search, fuzzy_table_search
from .router.tool_router import (
    match as match_tool,
    format_result as format_tool_result,
    _format_geometry_types_multi,
)
from .session import SessionManager

logger = logging.getLogger(__name__)

app = FastAPI(title="Spatial Lakehouse Agent", version="0.1.0")

session_manager = SessionManager()
mcp_client = MCPClient(settings.mcp_endpoint)
schema_builder = SchemaBuilder(mcp_client)
llm_client = LLMClient(
    backend=settings.llm_backend,
    vllm_url=settings.vllm_base_url,
    ollama_url=settings.ollama_base_url,
    timeout=settings.query_timeout,
)


class ChatRequest(BaseModel):
    session_id: str
    message: str
    active_layers: list[str] = []


def _sse(event: dict) -> str:
    return f"data: {json.dumps(event)}\n\n"


@app.post("/api/agent/chat")
async def chat(req: ChatRequest):
    async def generate():
        session = session_manager.get_or_create(req.session_id)

        try:
            # Classify intent
            yield _sse({"type": "status", "content": "Classifying intent..."})
            intent = classify(req.message)

            # Conversational: respond directly
            if intent == "conversational":
                yield _sse({
                    "type": "result",
                    "content": "Hello! I can help you query spatial and tabular data "
                    "from the lakehouse. Try asking about tables, locations, or spatial "
                    "relationships like 'show buildings near the river'.",
                })
                yield _sse({"type": "done"})
                return

            # Discover schema
            yield _sse({"type": "status", "content": "Discovering schema..."})
            # Extract active namespaces from webmap layer keys ("ns/table" → "ns")
            active_namespaces = list({
                layer.split("/")[0]
                for layer in req.active_layers
                if "/" in layer
            })
            print(f"[DEBUG] active_layers={req.active_layers}, active_namespaces={active_namespaces}", flush=True)
            schema_context = await schema_builder.build_context(
                req.message, session, active_namespaces=active_namespaces
            )

            # Meta: route to specific MCP tool if possible, else schema context
            if intent == "meta":
                known_tables = session.schema_cache.get("_tables", [])
                route = match_tool(req.message, known_tables)

                if route and route.tool_name == "search_tables":
                    # Use LLM fuzzy search for better semantic matching
                    yield _sse({
                        "type": "status",
                        "content": "Searching catalog...",
                    })
                    try:
                        base_url = (
                            settings.vllm_base_url
                            if settings.llm_backend == "vllm"
                            else settings.ollama_base_url
                        )
                        available = await detect_available_models(
                            settings.llm_backend, base_url
                        )
                        model = available[0] if available else settings.primary_model

                        if "column_pattern" in route.arguments:
                            # Column search — need table descriptions
                            # Ensure all tables are described in cache
                            for t in known_tables:
                                cache_key = f"_desc_{t['full_name']}"
                                if cache_key not in session.schema_cache:
                                    desc = await mcp_client.call_tool(
                                        "describe_table",
                                        {"table": f"{t['namespace']}.{t['name']}"},
                                    )
                                    session.schema_cache[cache_key] = desc

                            content = await fuzzy_column_search(
                                req.message,
                                known_tables,
                                session.schema_cache,
                                llm_client,
                                model,
                            )
                        else:
                            # Table name search
                            content = await fuzzy_table_search(
                                req.message,
                                known_tables,
                                llm_client,
                                model,
                            )
                        yield _sse({"type": "result", "content": content})
                    except Exception as e:
                        # Fall back to MCP search_tables on LLM failure
                        logger.warning("LLM search failed, falling back: %s", e)
                        result = await mcp_client.call_tool(
                            route.tool_name, route.arguments
                        )
                        yield _sse({
                            "type": "result",
                            "content": format_tool_result(
                                route.tool_name, result
                            ),
                        })
                elif route and route.tool_name == "table_stats_multi":
                    # Multi-table aggregation (e.g. geometry types across namespace)
                    tables = route.arguments.get("tables", [])
                    yield _sse({
                        "type": "status",
                        "content": f"Querying {len(tables)} tables...",
                    })
                    results = []
                    for tbl in tables:
                        r = await mcp_client.call_tool("table_stats", {"table": tbl})
                        results.append((tbl, r))
                    yield _sse({
                        "type": "result",
                        "content": _format_geometry_types_multi(results),
                    })
                elif route:
                    yield _sse({
                        "type": "status",
                        "content": f"Calling {route.tool_name}...",
                    })
                    result = await mcp_client.call_tool(
                        route.tool_name, route.arguments
                    )
                    yield _sse({
                        "type": "result",
                        "content": format_tool_result(
                            route.tool_name, result, route.format_hint
                        ),
                    })
                else:
                    # No tool match — use LLM fuzzy search as fallback
                    yield _sse({
                        "type": "status",
                        "content": "Searching catalog...",
                    })
                    try:
                        base_url = (
                            settings.vllm_base_url
                            if settings.llm_backend == "vllm"
                            else settings.ollama_base_url
                        )
                        available = await detect_available_models(
                            settings.llm_backend, base_url
                        )
                        model = available[0] if available else settings.primary_model

                        # Ensure all tables are described in cache
                        for t in known_tables:
                            cache_key = f"_desc_{t['full_name']}"
                            if cache_key not in session.schema_cache:
                                desc = await mcp_client.call_tool(
                                    "describe_table",
                                    {"table": f"{t['namespace']}.{t['name']}"},
                                )
                                session.schema_cache[cache_key] = desc

                        content = await fuzzy_column_search(
                            req.message,
                            known_tables,
                            session.schema_cache,
                            llm_client,
                            model,
                        )
                        yield _sse({"type": "result", "content": content})
                    except Exception as e:
                        logger.warning("LLM fallback search failed: %s", e)
                        yield _sse({
                            "type": "result",
                            "content": schema_context,
                        })

                yield _sse({"type": "done"})
                return

            # Detect models
            base_url = (
                settings.vllm_base_url if settings.llm_backend == "vllm"
                else settings.ollama_base_url
            )
            available = await detect_available_models(settings.llm_backend, base_url)
            model = select_model(intent, available, settings)

            # Generate + execute with retry
            yield _sse({"type": "status", "content": "Generating SQL..."})

            should_materialize = intent == "spatial"
            print(f"[DEBUG] Schema context for {intent}:\n{schema_context[:2000]}", flush=True)

            async def gen_fn(msg, ctx, error=None, failed_sql=None):
                if error and failed_sql:
                    msgs = build_error_prompt(error, failed_sql, msg, ctx)
                elif intent == "spatial":
                    msgs = build_spatial_prompt(ctx, msg)
                else:
                    msgs = build_analytics_prompt(ctx, msg)
                response = await llm_client.generate(msgs, model)
                sql = extract_sql(response)
                validate_sql(sql)
                print(f"[DEBUG] Generated SQL: {sql}", flush=True)
                return sql

            async def exec_fn(sql):
                tool_name, tool_args = pick_tool(sql, should_materialize, req.session_id)
                return await mcp_client.call_tool(tool_name, tool_args)

            async for event in retry_loop(
                gen_fn, exec_fn, req.message, schema_context, settings.max_retry
            ):
                if event["type"] == "result_data":
                    result = event["data"]
                    sql = event["sql"]
                    row_count = result.get("row_count", 0)

                    if should_materialize:
                        _, tool_args = pick_tool(sql, True, req.session_id)
                        await notify_lakehouse(
                            settings.lakehouse_api,
                            req.session_id,
                            tool_args["namespace"],
                            tool_args["result_name"],
                            row_count,
                            req.message,
                        )
                        yield _sse({
                            "type": "result",
                            "content": f"Found {row_count} features. Layer added to map.",
                        })
                    else:
                        rows = result.get("rows", [])
                        if rows and row_count <= 20:
                            # Format small result sets as a readable table
                            lines = []
                            keys = list(rows[0].keys())
                            lines.append("| " + " | ".join(keys) + " |")
                            lines.append("| " + " | ".join("---" for _ in keys) + " |")
                            for row in rows:
                                vals = [str(row.get(k, "")) for k in keys]
                                lines.append("| " + " | ".join(vals) + " |")
                            content = "\n".join(lines)
                        else:
                            content = f"Query returned {row_count} rows."
                        yield _sse({
                            "type": "result",
                            "content": content,
                        })
                else:
                    yield _sse(event)

        except MaxRetriesExceeded as e:
            yield _sse({"type": "error", "content": str(e)})
        except RuntimeError as e:
            yield _sse({"type": "error", "content": str(e)})
        except Exception as e:
            logger.exception("Unexpected error in chat")
            yield _sse({"type": "error", "content": f"Internal error: {e}"})

        yield _sse({"type": "done"})

        # Store in history
        session.history.append({"role": "user", "content": req.message})

    return StreamingResponse(generate(), media_type="text/event-stream")


@app.get("/api/agent/health")
async def health():
    base_url = (
        settings.vllm_base_url if settings.llm_backend == "vllm"
        else settings.ollama_base_url
    )
    available = await detect_available_models(settings.llm_backend, base_url)
    active = settings.active_model or (available[0] if available else "none")
    return {"status": "ok", "model": active}


@app.get("/api/agent/models")
async def models():
    base_url = (
        settings.vllm_base_url if settings.llm_backend == "vllm"
        else settings.ollama_base_url
    )
    available = await detect_available_models(settings.llm_backend, base_url)
    active = settings.active_model or (available[0] if available else "none")
    return {"available": available, "active": active}
