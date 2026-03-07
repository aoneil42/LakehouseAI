from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List


def _utcnow():
    return datetime.now(timezone.utc)


@dataclass
class SessionState:
    session_id: str
    created_at: datetime = field(default_factory=_utcnow)
    schema_cache: dict = field(default_factory=dict)
    schema_cache_ts: float = 0.0
    history: List[dict] = field(default_factory=list)
    scratch_namespace: str = ""

    def __post_init__(self):
        self.scratch_namespace = f"_scratch_{self.session_id.replace('-', '')[:8]}"


class SessionManager:
    def __init__(self):
        self._sessions: Dict[str, SessionState] = {}

    def get_or_create(self, session_id: str) -> SessionState:
        if session_id not in self._sessions:
            self._sessions[session_id] = SessionState(session_id=session_id)
        return self._sessions[session_id]

    def remove(self, session_id: str):
        self._sessions.pop(session_id, None)
