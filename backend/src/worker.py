import logging
import time
from datetime import timedelta
from pathlib import Path

from redis.exceptions import RedisError
from sqlalchemy import or_, select

from .calculation import estimate
from .db import QUEUE, Session, redis, utcnow
from . import evidence
from .ledger import checksum, inside, record
from .models import BatchState, CalibrationRelease, ChamberRun, DryBatch, ReleaseState

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)
HEARTBEAT = Path('/tmp/worker-heartbeat')
RECOVERY_INTERVAL_SECONDS = 30


def claim(wakeup_id):
    with Session.begin() as db:
        eligible = or_(CalibrationRelease.state == ReleaseState.QUEUED,
                       (CalibrationRelease.state == ReleaseState.RUNNING) &
                       (CalibrationRelease.touched_at < utcnow() - timedelta(minutes=5)))
        query = select(CalibrationRelease).where(eligible)
        if wakeup_id is not None:
            query = query.where(CalibrationRelease.id == wakeup_id)
        release = db.scalar(query.order_by(CalibrationRelease.id).limit(1).with_for_update(skip_locked=True))
        if release is None:
            return None
        release.state = ReleaseState.RUNNING
        release.attempts += 1
        release.touched_at = utcnow()
        return release.id, release.attempts, release.snapshot, release.input_hash


def settle(job, result, error):
    release_id, attempt, snapshot, digest = job
    with Session.begin() as db:
        batch = db.scalar(select(DryBatch).where(DryBatch.code == snapshot['batch']).with_for_update())
        entry = db.get(CalibrationRelease, release_id, with_for_update=True)
        # 版本守卫：补证据或较早一次迟到的试算先落定时，过期尝试不得盖掉已确认的文件与结论
        if entry.attempts != attempt or entry.state != ReleaseState.RUNNING:
            log.info('calibration %s stale attempt %s skipped, current state %s',
                     release_id, attempt, entry.state.value)
            return
        entry.touched_at = utcnow()
        if error is not None:
            entry.state = ReleaseState.FAILED
            entry.error = error[:240]
            record(db, 'worker', 'fit.failed', entry.error, batch.code)
            return
        entry.result = result
        entry.prediction_pct = result['prediction_pct']
        within_window = inside(entry.prediction_pct, snapshot['window'])
        entry.state = ReleaseState.AWAITING if within_window else ReleaseState.RETEST
        # 证据文件按原输入与本次结论原子落盘；卷不可写时本次试算回退为失败，可按原快照重试
        try:
            verification = evidence.write_evidence(
                release_id, digest, snapshot, result, actor='worker',
                action='fit.evidence', batch_code=batch.code)
        except OSError:
            entry.state = ReleaseState.FAILED
            entry.error = '证据卷暂不可写，可按原快照重试'
            entry.result = None
            entry.prediction_pct = None
            record(db, 'worker', 'fit.failed', entry.error, batch.code)
            log.exception('calibration evidence could not be written')
            return
        entry.error = None
        batch.state = BatchState.REVIEW
        run = db.get(ChamberRun, entry.run_id)
        record(db, 'worker', 'fit.completed' if within_window else 'retest.opened',
               f'{run.source_name} · {entry.prediction_pct:.3f}% · 工艺窗 {snapshot["window"]} · 证据 {verification["label"]}',
               batch.code)
        log.info('calibration %s %s evidence=%s', release_id, entry.state.value, verification['status'])


def process(job):
    _, _, snapshot, digest = job
    try:
        if checksum(snapshot) != digest:
            raise ValueError('试算输入快照校验失败')
        result = estimate(snapshot)
    except Exception as exc:
        log.exception('calibration fit failed')
        settle(job, None, str(exc)[:240] or '拟合计算失败，可按原快照重试')
    else:
        settle(job, result, None)


def recover_interrupted():
    """试算中断恢复：台账已落定而证据卷缺失/不一致的单据，只用原输入与结论补齐，不再拟合。"""
    recovered = evidence.recover_pending_evidence()
    if recovered:
        log.info('recovered evidence without refitting: %s', recovered)
    return recovered


def main():
    last_recovery = 0.0
    recover_interrupted()
    while True:
        try:
            try:
                entry = redis.blpop(QUEUE, timeout=2)
            except RedisError:
                log.warning('Redis unavailable; checking committed calibration queue')
                time.sleep(2)
                entry = None
            job = claim(int(entry[1])) if entry else None
            job = job or claim(None)
            if job:
                process(job)
            if time.monotonic() - last_recovery > RECOVERY_INTERVAL_SECONDS:
                recover_interrupted()
                last_recovery = time.monotonic()
            HEARTBEAT.touch()
        except Exception:
            log.exception('worker iteration failed')
            time.sleep(3)


if __name__ == '__main__':
    main()
