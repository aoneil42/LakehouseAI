import os
from pathlib import Path


def _find_prompts_dir() -> Path:
    # Docker: /app/prompts
    app_dir = Path("/app/prompts")
    if app_dir.is_dir():
        return app_dir
    # Dev: relative to source tree
    src_dir = Path(__file__).resolve().parent.parent.parent.parent / "prompts"
    if src_dir.is_dir():
        return src_dir
    # Env override
    env_dir = os.environ.get("SA_PROMPTS_DIR")
    if env_dir:
        return Path(env_dir)
    raise FileNotFoundError("Cannot find prompts directory")


_PROMPTS_DIR = _find_prompts_dir()


def _load(name: str) -> str:
    return (_PROMPTS_DIR / name).read_text()


def build_spatial_prompt(schema_context: str, user_message: str) -> list[dict]:
    system = _load("system_spatial.txt").replace("{schema_context}", schema_context)
    return [
        {"role": "system", "content": system},
        {"role": "user", "content": user_message},
    ]


def build_analytics_prompt(schema_context: str, user_message: str) -> list[dict]:
    system = _load("system_analytics.txt").replace("{schema_context}", schema_context)
    return [
        {"role": "system", "content": system},
        {"role": "user", "content": user_message},
    ]


def build_error_prompt(
    error: str, failed_sql: str, user_message: str, schema_context: str
) -> list[dict]:
    template = _load("error_correction.txt")
    content = (
        template.replace("{error_message}", error)
        .replace("{failed_sql}", failed_sql)
        .replace("{user_message}", user_message)
        .replace("{schema_context}", schema_context)
    )
    return [
        {"role": "system", "content": content},
        {"role": "user", "content": "Fix the SQL. Return ONLY the corrected query."},
    ]
