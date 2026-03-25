# Running LakehouseAI in GitHub Codespaces

## Quick Start

1. Go to your GitHub repo → **Code** → **Codespaces** → **Create codespace on main**
2. Select **8-core / 32GB** machine type (required — the stack uses ~12GB RAM)
3. Wait ~5 minutes for first-time setup (Docker image builds)
4. The webmap opens automatically at the forwarded port 80 URL

That's it. The devcontainer config handles everything: Docker-in-Docker, image builds, `docker compose up`, and `bootstrap.sh`.

## What Happens Automatically

| Phase | What runs | When |
|---|---|---|
| **Create** | `setup.sh` — builds all 6 custom Docker images | First creation only |
| **Start** | `docker compose up -d && bootstrap.sh` — starts containers, provisions Garage + LakeKeeper | Every start |
| **Ports** | 8 ports forwarded with Codespace URLs | Automatic |

## Accessing Services

Once running, click the **Ports** tab in the VS Code terminal panel. Each service gets a `*.app.github.dev` URL:

| Port | Service | Notes |
|---|---|---|
| **80** | Webmap | Opens automatically — your main interface |
| **8181** | LakeKeeper UI | Iceberg catalog browser |
| **8000** | Feature API | FastAPI docs at `/api/docs` |
| **5050** | OGC API Features | pygeoapi landing page |
| **8001** | Esri GeoServices | REST endpoint at `/esri/rest/info` |
| **8082** | MCP Server | Connect Claude Code: `claude mcp add spatial-lakehouse --transport http --url <codespace-url>:8082/mcp` |
| **3900** | Garage S3 | Object storage API |
| **8090** | Spatial Agent | Only if started with `--profile agent` |

### Codespace URL Caveat

The webmap's nginx config proxies API calls using Docker container hostnames (e.g., `http://lakehouse-api:8000`). This works for requests **from the webmap container to other containers** on the Docker network. But if you open the Feature API directly from your browser via the Codespace URL, that's a direct connection to port 8000 — which also works fine.

The webmap at port 80 is the primary entry point and handles all the proxying internally.

## Starting the Spatial Agent

The agent is optional and requires Ollama. Since Codespaces don't have GPUs, you'd point it at an external Ollama/vLLM endpoint:

```bash
# Option A: Remote Ollama (e.g., on your Lightsail box)
export SA_OLLAMA_BASE_URL="http://your-server:11434"

# Option B: Install Ollama in the Codespace (CPU-only, slow but works for testing)
curl -fsSL https://ollama.ai/install.sh | sh
ollama pull devstral-small:latest &

# Start the agent
cd lakehouse
docker compose --profile agent up -d
```

## Machine Size Recommendations

| Machine | RAM | Works? |
|---|---|---|
| 4-core / 16GB | 16GB | Core stack only, tight on memory |
| **8-core / 32GB** | 32GB | **Recommended** — core stack + agent + headroom |
| 16-core / 64GB | 64GB | Overkill unless running Sedona |

## Cost

Codespaces bill per hour while running. The 8-core machine is ~$0.36/hr. **Stop your Codespace** when you're not using it (it auto-stops after 30 min of inactivity by default).

```bash
# Check running containers
docker compose -f lakehouse/docker-compose.yml ps

# View logs
docker compose -f lakehouse/docker-compose.yml logs -f api

# Rebuild a single service after code changes
docker compose -f lakehouse/docker-compose.yml build api
docker compose -f lakehouse/docker-compose.yml up -d api
```

## Developing in the Codespace

The Codespace has Python 3.12, Node 20, and Docker available. For faster iteration on a single service:

```bash
# Run the Feature API outside Docker (no rebuild needed on code changes)
cd lakehouse/api
source .env  # picks up GARAGE_KEY_ID, GARAGE_SECRET_KEY
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8000 --reload

# Run webmap in Vite dev mode (hot reload)
cd lakehouse/webmap
npm install
npm run dev  # port 5173
```

## Files to Add to Your Repo

Copy these into your GitHub repo:

```
.devcontainer/
├── devcontainer.json
└── setup.sh          # make sure this is executable: chmod +x
```