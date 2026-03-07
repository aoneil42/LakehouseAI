from spatial_agent.session import SessionManager, SessionState


def test_get_or_create_new():
    sm = SessionManager()
    s = sm.get_or_create("abc-1234-def")
    assert isinstance(s, SessionState)
    assert s.session_id == "abc-1234-def"
    assert s.scratch_namespace == "_scratch_abc1234d"


def test_get_or_create_existing():
    sm = SessionManager()
    s1 = sm.get_or_create("test-id")
    s1.history.append({"role": "user", "content": "hi"})
    s2 = sm.get_or_create("test-id")
    assert s2 is s1
    assert len(s2.history) == 1


def test_remove():
    sm = SessionManager()
    sm.get_or_create("test-id")
    sm.remove("test-id")
    s = sm.get_or_create("test-id")
    assert len(s.history) == 0


def test_scratch_namespace():
    s = SessionState(session_id="a1b2c3d4-e5f6-7890-abcd-ef1234567890")
    assert s.scratch_namespace == "_scratch_a1b2c3d4"
