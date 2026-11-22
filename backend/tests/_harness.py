"""Stub external dependencies so the real src.evidence / src.worker can run on stdlib only."""
import sys
import types
from contextlib import contextmanager
from datetime import datetime, timezone

BACKEND = '/workspace/backend'
if BACKEND not in sys.path:
    sys.path.insert(0, BACKEND)


def utcnow():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def install_common_stubs():
    # sqlalchemy 只需要 select 与 with_for_update 语义，测试中以假对象承载
    sa = types.ModuleType('sqlalchemy')

    class _Query:
        def where(self, *args, **kwargs):
            return self

        def order_by(self, *args, **kwargs):
            return self

    sa.select = lambda *args, **kwargs: _Query()
    sys.modules.setdefault('sqlalchemy', sa)

    fastapi = types.ModuleType('fastapi')

    class HTTPException(Exception):
        def __init__(self, status_code, detail):
            super().__init__(detail)
            self.status_code = status_code
            self.detail = detail

    fastapi.HTTPException = HTTPException
    sys.modules.setdefault('fastapi', fastapi)

    src = types.ModuleType('src')
    src.__path__ = [f'{BACKEND}/src']
    sys.modules.setdefault('src', src)

    db = types.ModuleType('src.db')

    class FakeSession:
        registry = None  # 由测试装载一个 FakeRegistry
        commits = 0

        def __init__(self):
            self.registry = FakeSession.registry

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            if exc == (None, None, None):
                FakeSession.commits += 1
            return False

        def get(self, model, key, with_for_update=False):
            return self.registry.get(key, lock=with_for_update)

        def scalars(self, query):
            return self.registry.all_settled()

    FakeSession.begin = classmethod(lambda cls: cls())
    db.Session = FakeSession
    db.redis = types.SimpleNamespace()
    db.QUEUE = 'freeze:fit'
    db.utcnow = utcnow
    sys.modules['src.db'] = db

    ledger = types.ModuleType('src.ledger')

    def checksum(data):
        import hashlib
        import json
        normalized = json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
        return hashlib.sha256(normalized.encode()).hexdigest()

    ledger.checksum = checksum
    ledger.record = lambda *args, **kwargs: FakeSession.registry.events.append(args)
    sys.modules['src.ledger'] = ledger

    models = types.ModuleType('src.models')

    class _State:
        def __init__(self, value):
            self.value = value

    class ReleaseState:
        QUEUED = _State('queued')
        RUNNING = _State('running')
        FAILED = _State('failed')
        AWAITING = _State('awaiting_verification')
        RETEST = _State('retest_required')
        VERIFIED = _State('verified')
        HELD = _State('held')
        RELEASED = _State('released')

    class CalibrationRelease:
        state = types.SimpleNamespace(in_=lambda values: ('in', values))
        id = 'id'

    models.ReleaseState = ReleaseState
    models.CalibrationRelease = CalibrationRelease
    sys.modules['src.models'] = models


class FakeRegistry:
    def __init__(self):
        self.rows = {}
        self.events = []

    def put(self, entry):
        self.rows[entry.id] = entry

    def get(self, key, lock=False):
        return self.rows.get(key)

    def all_settled(self):
        class Result:
            def __init__(self, rows):
                self._rows = rows

            def all(self_inner):
                return self_inner._rows

        from src.models import ReleaseState
        settled = {ReleaseState.AWAITING, ReleaseState.RETEST, ReleaseState.VERIFIED,
                   ReleaseState.HELD, ReleaseState.RELEASED}
        return Result([row for row in self.rows.values() if row.state in settled])


class FakeEntry:
    def __init__(self, release_id, state, input_hash, snapshot, result):
        self.id = release_id
        self.state = state
        self.input_hash = input_hash
        self.snapshot = snapshot
        self.result = result


@contextmanager
def temp_evidence_dir():
    import tempfile
    from pathlib import Path
    tmp = tempfile.mkdtemp(prefix='evidence-test-')
    yield Path(tmp)
