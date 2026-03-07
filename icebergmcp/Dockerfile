FROM python:3.12-slim

# System deps for DuckDB spatial
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgeos-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Non-root user
RUN groupadd -r mcp && useradd -r -g mcp -m mcp

WORKDIR /app

COPY pyproject.toml .
COPY src/ src/
RUN pip install --no-cache-dir .

RUN chown -R mcp:mcp /app

USER mcp

EXPOSE 8082

HEALTHCHECK --interval=30s --timeout=5s --retries=3 --start-period=15s \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8082/health')"]

CMD ["python", "-m", "spatial_lakehouse_mcp.server"]
