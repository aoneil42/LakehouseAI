FROM python:3.12-slim
RUN groupadd -r agent && useradd -r -g agent agent
WORKDIR /app
COPY pyproject.toml .
COPY src/ src/
RUN pip install --no-cache-dir .
COPY prompts/ prompts/
COPY scripts/ scripts/
RUN chown -R agent:agent /app
USER agent
EXPOSE 8090
HEALTHCHECK --interval=30s --timeout=5s --retries=3 --start-period=15s \
    CMD ["python", "scripts/health_check.py"]
CMD ["uvicorn", "spatial_agent.server:app", "--host", "0.0.0.0", "--port", "8090"]
