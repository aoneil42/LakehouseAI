# CLAUDE.md — spatial-lakehouse-agent

Execute these tasks in order. Each task has verification steps.
Do NOT modify any files in the lakehouse or MCP repos.
Reference SPATIAL-AGENT-ARCHITECTURE.md for full specs and code blocks.

---

## Task 1: Scaffold the project

**1a.** Initialize repo structure — create all directories per project structure:

```
src/spatial_agent/
src/spatial_agent/router/
src/spatial_agent/planner/
src/spatial_agent/executor/
src/spatial_agent/models/
src/spatial_agent/notify/
prompts/
tests/
scripts/
```

Create all `__init__.py` files (empty) in every package directory.

**1b.** Create `pyproject.toml`:

```toml
[project]
name = "spatial-lakehouse-agent"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = [
    "fastapi>=0.115",
    "uvicorn[standard]>=0.34",
    "httpx>=0.27",
    "pydantic>=2.0",
    "pydantic-settings>=2.0",
    "mcp>=1.0",
    "openai>=1.0",
    "sse-starlette>=2.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0",
    "pytest-asyncio>=0.23",
    "respx>=0.21",
]
```

**1c.** Create `.env.example` with all `SA_` variables and defaults.

**1d.** Create `src/spatial_agent/config.py` — Pydantic settings with `SA_` prefix.
See architecture doc Section 1.

**1e.** Create `src/spatial_agent/session.py` — `SessionState` dataclass +
`SessionManager` class. See architecture doc Section 10.

**Verify:**

```bash
pip install -e "."
python -c "from spatial_agent.config import settings; print(settings.port)"
# Should print: 8090
python -c "from spatial_agent.session import SessionManager; sm = SessionManager(); s = sm.get_or_create('test-1234'); print(s.scratch_namespace)"
# Should print: _scratch_test1234
```

---

## Task 2: MCP Client

**2a.** Create `src/spatial_agent/executor/mcp_client.py`:

- `MCPClient` class with `endpoint` parameter
- `async call_tool(tool_name, arguments) -> dict` using `streamablehttp_client`
- `async list_tools() -> list` for tool discovery
- Parse `result.content` text into JSON
- Handle connection errors gracefully (return JSON error object)

See architecture doc Section 6 for full implementation.

**2b.** Create `tests/test_mcp_client.py`:

- Mock `streamablehttp_client` context manager
- Test `call_tool` returns parsed JSON on success
- Test `call_tool` handles connection refused
- Test `call_tool` handles malformed JSON response
- Test `list_tools` returns tool list

**Verify:**

```bash
pytest tests/test_mcp_client.py -v
```

---

## Task 3: Intent Router

**3a.** Create `src/spatial_agent/router/intent.py`:

- `SPATIAL_KEYWORDS` set: `near`, `within`, `buffer`, `intersect`, `distance`,
  `polygon`, `boundary`, `adjacent`, `surrounding`, `closest`, `overlap`,
  `contains`, `crosses`, `touches`, `lat`, `lon`, `coordinate`, `radius`,
  `bbox`, `envelope`, `geometry`, `spatial`, `geom`, `proximity`, `meters`,
  `kilometers`, `miles`, `feet`
- `CONVERSATIONAL_PATTERNS`: short messages (<8 words) with no data terms,
  greetings, help requests
- `classify(message: str) -> Literal["spatial", "analytics", "conversational"]`
- Return `"spatial"` if any spatial keyword matches (case-insensitive, word boundary)
- Return `"conversational"` if message matches conversational patterns
- Default to `"analytics"`

**3b.** Create `tests/test_intent.py`:

```python
@pytest.mark.parametrize("msg,expected", [
    ("buildings near the river", "spatial"),
    ("buffer roads by 100m", "spatial"),
    ("show me parcels within the city boundary", "spatial"),
    ("what is the distance between these points", "spatial"),
    ("count rows in census table", "analytics"),
    ("average population by tract", "analytics"),
    ("what tables have a height column", "analytics"),
    ("hello, what can you do?", "conversational"),
    ("hi", "conversational"),
    ("help", "conversational"),
])
def test_classify(msg, expected):
    assert classify(msg) == expected
```

**Verify:**

```bash
pytest tests/test_intent.py -v
```

---

## Task 4: Schema Context Builder

**4a.** Create `src/spatial_agent/planner/schema.py`:

- `SchemaBuilder` class, takes `MCPClient` instance
- `async build_context(message: str, session: SessionState) -> str`
- Flow:
  1. Check `session.schema_cache` freshness (TTL 5 min)
  2. If stale, call MCP `list_tables()` → cache full table list
  3. Extract keywords from `message`, match against table names
  4. For matched tables, call `describe_table(table)` and `get_bbox(table)`
  5. Format into context string (see architecture doc Section 4)
  6. Cache results in `session.schema_cache`
- Handle MCP errors gracefully (return partial context with available info)

**4b.** Create `tests/test_schema.py` (add to tests/):

- Mock MCP client with fixture returning sample table/column data
- Test context string includes matched tables
- Test context string excludes unrelated tables
- Test cache hit (no MCP calls on second invocation within TTL)
- Test cache miss after TTL expiry

**Verify:**

```bash
pytest tests/ -k schema -v
```

---

## Task 5: SQL Generation + Validation

**5a.** Create prompt template files in `prompts/`:

- `system_spatial.txt` — full spatial SQL expert prompt (see architecture doc Section 5)
- `system_analytics.txt` — simpler version without spatial function references
- `schema_context.txt` — `{schema_context}` placeholder template
- `error_correction.txt` — retry template with `{error_message}`, `{failed_sql}`,
  `{user_message}`, `{schema_context}` placeholders

**5b.** Create `src/spatial_agent/planner/prompts.py`:

- Load templates from `prompts/` directory
- `build_spatial_prompt(schema_context, user_message) -> list[dict]`
- `build_analytics_prompt(schema_context, user_message) -> list[dict]`
- `build_error_prompt(error, failed_sql, user_message, schema_context) -> list[dict]`
- Each returns messages list: `[{"role": "system", ...}, {"role": "user", ...}]`

**5c.** Create `src/spatial_agent/planner/sql_gen.py`:

- `extract_sql(llm_response: str) -> str`
  - Check for ```sql fences first
  - Fall back to lines starting with SELECT/WITH
  - Strip non-SQL text
  - Raise `ExtractionError` if no SQL found
- `validate_sql(sql: str, known_tables: list[str]) -> None`
  - Reject if not SELECT or WITH...SELECT
  - Reject INSERT/UPDATE/DELETE/DROP/ALTER/CREATE (except CREATE in CTE context)
  - Check table references against known_tables (warn on unknown, don't block)
  - Check balanced parentheses
  - Raise `ValidationError` with descriptive message
- `async generate_sql(message, schema_context, llm_client, model) -> str`
  - Build prompt, call LLM, extract SQL, validate
  - Return validated SQL string

**5d.** Create `tests/test_sql_gen.py`:

- Test `extract_sql` from fenced markdown: ` ```sql\nSELECT ...\n``` `
- Test `extract_sql` from unfenced response: `"Here's the query:\nSELECT ..."`
- Test `extract_sql` with mixed explanation text
- Test `extract_sql` raises on no SQL content
- Test `validate_sql` accepts valid SELECT
- Test `validate_sql` accepts WITH...SELECT (CTE)
- Test `validate_sql` rejects INSERT
- Test `validate_sql` rejects DROP TABLE
- Test `validate_sql` rejects DELETE
- Test `validate_sql` detects unmatched parentheses

**Verify:**

```bash
pytest tests/test_sql_gen.py -v
```

---

## Task 6: Tool Picker + Retry

**6a.** Create `src/spatial_agent/executor/tool_picker.py`:

- `generate_result_name(sql: str) -> str` — derive a short snake_case name
  from the SQL (e.g., extract main table name + operation hint)
- `pick_tool(sql, should_materialize, session_id) -> tuple[str, dict]`
  - v1: materialize_result for map display, query for text-only results
  - See architecture doc Section 7

**6b.** Create `src/spatial_agent/executor/retry.py`:

- `class MaxRetriesExceeded(Exception): pass`
- `async retry_loop(generate_fn, execute_fn, user_message, schema_context, max_retry) -> dict`
  - Call `generate_fn` → get SQL
  - Call `execute_fn` → get result
  - On error: build error correction prompt, re-generate, re-execute
  - Yield SSE-compatible status dicts for each attempt
  - Raise `MaxRetriesExceeded` after all retries exhausted

**6c.** Create `tests/test_retry.py`:

- Test success on first try → no retry, returns result
- Test success on second try → one retry, returns result
- Test all retries exhausted → raises `MaxRetriesExceeded`
- Test error message passed to correction prompt

**Verify:**

```bash
pytest tests/test_retry.py -v
```

---

## Task 7: LLM Interface

**7a.** Create `src/spatial_agent/models/llm.py`:

- `class LLMClient`:
  - `__init__(self, backend, vllm_url, ollama_url)`
  - `async generate(messages: list[dict], model: str) -> str`
  - vLLM backend: use `openai.AsyncOpenAI(base_url=vllm_url)` →
    `client.chat.completions.create(model=model, messages=messages)`
  - Ollama backend: `httpx.AsyncClient` POST to
    `{ollama_url}/api/chat` with `{"model": model, "messages": messages, "stream": false}`
  - Return the text content of the first choice/message
  - Handle timeouts (configurable, default 60s)

**7b.** Create `src/spatial_agent/models/registry.py`:

- `async detect_available_models(backend, base_url) -> list[str]`
  - vLLM: GET `{base_url}/models` → parse model IDs
  - Ollama: GET `{base_url}/api/tags` → parse model names
  - Return empty list on connection failure (log warning)
- `select_model(intent: str, available: list[str], settings) -> str`
  - `spatial` → primary_model if available, else mid_model, else first available
  - `analytics` → fast_model if available, else primary_model
  - `conversational` → primary_model
  - Raise if no models available

**7c.** Create tests with mocked HTTP responses for both backends.

**Verify:**

```bash
pytest tests/ -k "llm or registry" -v
```

---

## Task 8: Notification

**8a.** Create `src/spatial_agent/notify/lakehouse.py`:

- `async notify_lakehouse(lakehouse_api, session_id, namespace, table, row_count, description="")`
- POST to `{lakehouse_api}/api/agent/notify/{session_id}`
- Timeout 10s, log warning on failure (don't raise — the query still succeeded)

See architecture doc Section 9.

**8b.** Create test with mocked httpx verifying correct URL and payload.

**Verify:**

```bash
pytest tests/ -k notify -v
```

---

## Task 9: FastAPI Server

**9a.** Create `src/spatial_agent/server.py`:

```python
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from .config import settings

app = FastAPI(title="Spatial Lakehouse Agent", version="0.1.0")

class ChatRequest(BaseModel):
    session_id: str
    message: str
```

Endpoints:

- **`POST /api/agent/chat`** — SSE streaming response:
  1. Get/create session via SessionManager
  2. Classify intent → yield `{"type": "status", "content": "Classifying..."}`
  3. Build schema context → yield status
  4. Generate SQL → yield `{"type": "sql", "content": sql}`
  5. Pick tool, execute via MCP → yield status
  6. On success: notify lakehouse → yield `{"type": "result", "content": summary}`
  7. On conversational: skip SQL, respond directly → yield result
  8. Always yield `{"type": "done"}`
  9. On any error: yield `{"type": "error", "content": error_message}` then done
  - Use `StreamingResponse(generator(), media_type="text/event-stream")`
  - Each event: `f"data: {json.dumps(event)}\n\n"`

- **`GET /api/agent/health`** → `{"status": "ok", "model": active_model_name}`

- **`GET /api/agent/models`** → `{"available": [...], "active": "..."}`

**9b.** Create `tests/test_server.py`:

- Use `httpx.AsyncClient` with `ASGITransport` (FastAPI test pattern)
- Mock MCP client, LLM client
- Test health endpoint returns 200
- Test chat with spatial query → SSE stream includes sql + result events
- Test chat with conversational query → no sql event
- Test chat with MCP error → error event + done

**Verify:**

```bash
pytest tests/ -v
# ALL tests pass
```

---

## Task 10: Docker

**10a.** Create `Dockerfile` — see architecture doc.

**10b.** Create `Dockerfile.dev` — see architecture doc.

**10c.** Create `docker-compose.agent.yml` — see architecture doc.

**10d.** Create `scripts/health_check.py`:

```python
#!/usr/bin/env python3
import sys
import urllib.request
try:
    urllib.request.urlopen("http://localhost:8090/api/agent/health", timeout=5)
    sys.exit(0)
except Exception:
    sys.exit(1)
```

**10e.** Create `scripts/download_models.sh` — see architecture doc. Make executable.

**Verify:**

```bash
docker build -t spatial-agent .
docker build -f Dockerfile.dev -t spatial-agent-dev .
# Both build without errors
```

---

## Task 11: README.md

Create README with these sections:

1. **What this does** (1 paragraph)
2. **Architecture diagram** (copy from architecture doc)
3. **Quick start** — docker compose overlay command
4. **Dev setup** — Ollama on host + Dockerfile.dev
5. **Configuration** — table of all SA_ env vars with defaults and descriptions
6. **MCP Tools** — brief list of 19 available tools by category
7. **Testing** — pytest commands
8. **Air-gapped deployment** — download_models.sh usage

---

## Task 12: Integration Verification

Run against the full lakehouse stack:

```bash
# Terminal 1: Start lakehouse
cd Iceberg-Geospatial-API-Server
docker compose up -d

# Terminal 2: Start Ollama (dev)
ollama serve
ollama pull devstral-small:latest

# Terminal 3: Start agent
cd spatial-lakehouse-agent
SA_LLM_BACKEND=ollama SA_OLLAMA_BASE_URL=http://localhost:11434 \
  uvicorn spatial_agent.server:app --port 8090

# Terminal 4: Test endpoints
curl http://localhost:8090/api/agent/health
# → {"status":"ok",...}

curl http://localhost:8090/api/agent/models
# → {"available":[...],"active":"..."}

curl -N -X POST http://localhost:8090/api/agent/chat \
  -H "Content-Type: application/json" \
  -d '{"session_id":"test-001","message":"list all tables"}'
# → SSE stream with status + result events

curl -N -X POST http://localhost:8090/api/agent/chat \
  -H "Content-Type: application/json" \
  -d '{"session_id":"test-001","message":"hello"}'
# → SSE stream with conversational result (no sql event)
```

---

## Files Created Summary

| Category | Count | Files |
|----------|-------|-------|
| Config | 4 | pyproject.toml, .env.example, Dockerfile, Dockerfile.dev |
| Compose | 1 | docker-compose.agent.yml |
| Source | 14 | config, server, session, router/intent, planner/{schema,prompts,sql_gen}, executor/{mcp_client,tool_picker,retry}, models/{registry,llm}, notify/lakehouse |
| Prompts | 4 | system_spatial.txt, system_analytics.txt, schema_context.txt, error_correction.txt |
| Tests | 7 | conftest + 6 test files |
| Scripts | 2 | health_check.py, download_models.sh |
| Docs | 2 | README.md, CLAUDE.md |
| Init files | 7 | __init__.py in each package |
| **Total** | **~41** | |
