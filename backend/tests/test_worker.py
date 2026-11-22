"""worker 试算落定与中断恢复测试：迟到试算不盖证据、卷不可写回退失败、恢复不重新拟合。"""
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from _harness import FakeRegistry, install_common_stubs, temp_evidence_dir, utcnow  # noqa: E402

install_common_stubs()
from src.db import Session as FakeSessionModule  # noqa: E402
FakeSession = FakeSessionModule

# --- 在导入 worker 前补齐它依赖的桩 ---
import types  # noqa: E402

calc = types.ModuleType('src.calculation')
FIT_CALLS = []


def estimate(snapshot):
    FIT_CALLS.append(snapshot['batch'])
    return {'prediction_pct': 12.5, 'loo_rmse': 0.4, 'n_references': 3, 'alpha': 0.65,
            'seed': 1062, 'solver': 'svd', 'lambdas': {}, 'coefficients': {}, 'feature_names': []}


calc.estimate = estimate
sys.modules['src.calculation'] = calc

redis_mod = types.ModuleType('redis')
redis_exc = types.ModuleType('redis.exceptions')


class RedisError(Exception):
    pass


redis_exc.RedisError = RedisError
redis_mod.exceptions = redis_exc
sys.modules['redis'] = redis_mod
sys.modules['redis.exceptions'] = redis_exc

sa = sys.modules['sqlalchemy']


class _Expr:
    def __init__(self, *parts):
        self.parts = parts

    def __or__(self, other):
        return _Expr('or', self, other)

    def __and__(self, other):
        return _Expr('and', self, other)

    def __eq__(self, other):
        return _Expr('eq', self, other)

    def where(self, *args, **kwargs):
        return self

    def order_by(self, *args, **kwargs):
        return self

    def limit(self, *args, **kwargs):
        return self

    def with_for_update(self, *args, **kwargs):
        return self


sa.or_ = lambda *a: _Expr('or', *a)


class _Column:
    def __eq__(self, other):
        return _Expr('eq', self, other)

    def __lt__(self, other):
        return _Expr('lt', self, other)

    def __in__(self, other):
        return _Expr('in', self, other)


class _Model:
    state = _Column()
    touched_at = _Column()
    id = _Column()


class _DryBatch:
    code = _Column()


class _ChamberRun:
    pass


sa.select = lambda *a: _Expr('select', *a)
sys.modules['sqlalchemy'] = sa

models = sys.modules['src.models']


class _StateValue:
    def __init__(self, value):
        self.value = value


class BatchState:
    COLLECTING = _StateValue('collecting')
    CALIBRATING = _StateValue('calibrating')
    REVIEW = _StateValue('review')
    RELEASED = _StateValue('released')


models.BatchState = BatchState
models.CalibrationRelease.state = types.SimpleNamespace(in_=lambda values: ('in', values))
models.CalibrationRelease.id = 'id'
models.DryBatch = _DryBatch
models.ChamberRun = _ChamberRun

ledger = sys.modules['src.ledger']
ledger.inside = lambda value, window: window[0] <= value <= window[1]

from src import worker  # noqa: E402
from src import evidence  # noqa: E402
from src.db import Session  # noqa: E402
from src.ledger import checksum  # noqa: E402
from src.models import ReleaseState  # noqa: E402

PASS = 0
FAIL = 0


def check(name, condition, detail=''):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f'  ok  {name}')
    else:
        FAIL += 1
        print(f'FAIL  {name} {detail}')


class WorkerRegistry(FakeRegistry):
    def __init__(self):
        super().__init__()
        self.batches = {}
        self.runs = {}

    def get(self, key, lock=False):
        return self.rows.get(key)

    def all_settled(self):
        class Result:
            def __init__(self, rows_inner):
                self._rows = rows_inner

            def all(self_inner):
                return self_inner._rows

        settled = {ReleaseState.AWAITING, ReleaseState.RETEST, ReleaseState.VERIFIED,
                   ReleaseState.HELD, ReleaseState.RELEASED}
        return Result([row for row in self.rows.values() if row.state in settled])


class WorkerSession:
    registry = None

    def __init__(self):
        self.registry = WorkerSession.registry

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def get(self, model, key, with_for_update=False):
        if model is _ChamberRun:
            return self.registry.runs.get(key)
        return self.registry.rows.get(key)

    def scalar(self, query):
        # settle 中只按批次码取 DryBatch
        return next(iter(self.registry.batches.values()), None)

    def scalars(self, query):
        return self.registry.all_settled()


class Entry:
    def __init__(self, release_id, state, attempt, snapshot, digest, run_id=1):
        self.id = release_id
        self.state = state
        self.attempts = attempt
        self.snapshot = snapshot
        self.input_hash = digest
        self.run_id = run_id
        self.result = None
        self.prediction_pct = None
        self.error = None
        self.touched_at = utcnow()


class Batch:
    def __init__(self, code):
        self.code = code
        self.state = BatchState.CALIBRATING


class Run:
    def __init__(self, run_id, source_name):
        self.id = run_id
        self.source_name = source_name


def make_snapshot(batch='FD-9101'):
    return {'batch': batch, 'recipe': 'AP-26R3', 'curve_hash': 'c' * 64,
            'features': [14.2, 1.0, 243, 30.0, -20.0], 'sample_pairs': {},
            'window': [9.0, 16.0],
            'training': [{'batch': f'FD-{i}', 'features': [1.0 * i], 'observed': 10.0 + i} for i in range(3)]}


def main():
    registry = WorkerRegistry()
    WorkerSession.registry = registry
    FakeSession.registry = registry
    Session.begin = classmethod(lambda cls: WorkerSession())
    with temp_evidence_dir() as tmp:
        evidence.EVIDENCE_DIR = tmp
        snapshot = make_snapshot()
        digest = checksum(snapshot)
        registry.batches['FD-9101'] = Batch('FD-9101')
        registry.runs[1] = Run(1, 'FD-9101_chamber_r1.csv')

        # 1. 正常落定：状态推进、证据文件与台账结论一致
        entry = Entry(8101, ReleaseState.RUNNING, 1, snapshot, digest)
        registry.put(entry)
        job = (entry.id, 1, snapshot, digest)
        worker.settle(job, estimate(snapshot), None)
        check('窗内落定为待复测确认', entry.state == ReleaseState.AWAITING, entry.state)
        check('台账写入预测值', entry.prediction_pct == 12.5)
        view = evidence.inspect_evidence(entry)
        check('落定同时生成可用证据', view['status'] == evidence.EVIDENCE_AVAILABLE, str(view))
        check('批次进入待复核', registry.batches['FD-9101'].state == BatchState.REVIEW)

        # 2. 较早一次试算迟到：台账已被后一次确认，旧 attempt 不得盖掉文件与结论
        original_file = evidence.evidence_path(entry.id).read_bytes()
        stale = Entry(8101, ReleaseState.RUNNING, 1, snapshot, digest)
        # 模拟后一次试算已先落定（attempts=2，状态 AWAITING）
        entry.attempts = 2
        worker.settle((entry.id, 1, snapshot, digest), {'prediction_pct': 55.5}, None)
        check('迟到旧试算不改变结论', entry.prediction_pct == 12.5 and entry.state == ReleaseState.AWAITING)
        check('迟到旧试算不盖掉证据文件', evidence.evidence_path(entry.id).read_bytes() == original_file)

        # 另一种迟到：单据已被补证据流程推进离开 RUNNING
        entry.state = ReleaseState.HELD
        worker.settle((entry.id, 2, snapshot, digest), estimate(snapshot), None)
        check('非 RUNNING 状态拒绝落定', entry.state == ReleaseState.HELD and entry.prediction_pct == 12.5)
        entry.state = ReleaseState.AWAITING

        # 3. 证据卷不可写：本次试算回退失败，结论不半落定
        bad_entry = Entry(8102, ReleaseState.RUNNING, 1, make_snapshot('FD-9102'),
                          checksum(make_snapshot('FD-9102')))
        registry.put(bad_entry)
        registry.batches['FD-9102'] = Batch('FD-9102')
        original_dir = evidence.EVIDENCE_DIR
        evidence.EVIDENCE_DIR = Path(tmp) / 'missing-parent'
        # 让父目录创建也失败：把 EVIDENCE_DIR 指到一个已存在的普通文件路径下
        blocker = Path(tmp) / 'blocker'
        blocker.write_text('x', encoding='utf-8')
        evidence.EVIDENCE_DIR = blocker  # mkdir(parents=True) 对文件路径会抛 OSError
        try:
            worker.settle((bad_entry.id, 1, bad_entry.snapshot, bad_entry.input_hash),
                          estimate(bad_entry.snapshot), None)
        finally:
            evidence.EVIDENCE_DIR = original_dir
        check('卷不可写时回退为计算失败', bad_entry.state == ReleaseState.FAILED, bad_entry.state)
        check('失败时结论不半落定', bad_entry.result is None and bad_entry.prediction_pct is None)
        check('失败原因可按原快照重试', bad_entry.error is not None and '重试' in bad_entry.error)

        # 4. 拟合异常：进入失败状态，不产生证据
        boom_entry = Entry(8103, ReleaseState.RUNNING, 1, make_snapshot('FD-9103'),
                           checksum(make_snapshot('FD-9103')))
        registry.put(boom_entry)
        registry.batches['FD-9103'] = Batch('FD-9103')

        def boom(snapshot_inner):
            raise RuntimeError('参考批次的工况特征缺少变化')

        old_estimate = worker.estimate
        worker.estimate = boom
        try:
            worker.process((boom_entry.id, 1, boom_entry.snapshot, boom_entry.input_hash))
        finally:
            worker.estimate = old_estimate
        check('拟合异常落为失败', boom_entry.state == ReleaseState.FAILED and '工况' in (boom_entry.error or ''))
        check('失败单据没有证据文件', not evidence.evidence_path(boom_entry.id).exists())

        # 5. 中断恢复：已落定但缺证据的单据只补齐不重算
        orphan = Entry(8104, ReleaseState.AWAITING, 1, make_snapshot('FD-9104'),
                       checksum(make_snapshot('FD-9104')))
        orphan.result = estimate(orphan.snapshot)
        orphan.prediction_pct = orphan.result['prediction_pct']
        registry.put(orphan)
        fit_before = len(FIT_CALLS)
        check('恢复前缺证据为待核', evidence.inspect_evidence(orphan)['status'] == evidence.EVIDENCE_PENDING)
        recovered = worker.recover_interrupted()
        check('恢复补齐缺失证据', orphan.id in recovered
              and evidence.inspect_evidence(orphan)['status'] == evidence.EVIDENCE_AVAILABLE)
        payload = json.loads(evidence.evidence_path(orphan.id).read_text(encoding='utf-8'))
        check('恢复内容来自原输入与已落定结论',
              payload['input_hash'] == orphan.input_hash and payload['result'] == orphan.result)
        check('恢复没有重新拟合', len(FIT_CALLS) == fit_before, f'FIT_CALLS={FIT_CALLS}')

        # 6. 超窗预测落 RETEST，且证据仍可用
        over_entry = Entry(8105, ReleaseState.RUNNING, 1, make_snapshot('FD-9105'),
                           checksum(make_snapshot('FD-9105')))
        registry.put(over_entry)
        registry.batches['FD-9105'] = Batch('FD-9105')
        worker.settle((over_entry.id, 1, over_entry.snapshot, over_entry.input_hash),
                      {'prediction_pct': 22.0}, None)
        check('超窗落为超窗待复测', over_entry.state == ReleaseState.RETEST, over_entry.state)
        check('超窗单据证据同样可用',
              evidence.inspect_evidence(over_entry)['status'] == evidence.EVIDENCE_AVAILABLE)

        # 7. 主循环冒烟：redis 轮询、周期性恢复与心跳都能跑通（验证 redis 接线与恢复节拍）
        class FakeRedis:
            def __init__(self):
                self.calls = 0

            def blpop(self, queue, timeout=0):
                self.calls += 1
                return None

        class FakeHeartbeat:
            def __init__(self):
                self.touches = 0

            def touch(self_inner):
                self_inner.touches += 1
                if self_inner.touches >= 3:
                    raise KeyboardInterrupt

        fake_redis = FakeRedis()
        worker.redis = fake_redis
        worker.HEARTBEAT = FakeHeartbeat()
        worker.claim = lambda wakeup_id=None: None
        recover_calls = []
        worker.recover_interrupted = lambda: recover_calls.append(1)
        worker.time.sleep = lambda seconds: None
        try:
            worker.main()
        except KeyboardInterrupt:
            pass
        check('主循环轮询 redis 队列', fake_redis.calls == 3)
        check('主循环启动即跑一次恢复，且按节拍再恢复', len(recover_calls) >= 2)
        check('主循环每轮写心跳', worker.HEARTBEAT.touches == 3)

    print(f'\n{FAIL} failed, {PASS} passed')
    sys.exit(1 if FAIL else 0)


if __name__ == '__main__':
    main()
