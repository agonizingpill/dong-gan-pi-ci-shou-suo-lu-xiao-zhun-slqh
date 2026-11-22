"""拟合证据卷与台账的对账规则。

证据文件 /evidence/fit-<id>.json 只是台账落定结论的落盘副本：
- 台账未落定（排队、计算中、计算失败）时，卷上的文件一律视为迟到的游离文件，不能当证据；
- 台账已落定但文件缺失、指纹对不上或结论对不上时，校准单标记为待核，由主管补证据；
- 补证据只用校准单冻结的原输入快照和已落定结论重建，绝不重新拟合；
- 已签发的结论照旧，补证据不改变任何台账结论。
"""
import hashlib
import json
import logging
import os
import tempfile
from pathlib import Path

from fastapi import HTTPException
from sqlalchemy import select

from .db import Session
from .ledger import checksum, record
from .models import CalibrationRelease, ReleaseState

log = logging.getLogger(__name__)

EVIDENCE_DIR = Path(os.getenv('EVIDENCE_DIR', '/evidence'))

# 台账尚未落定结论的状态：此时卷上即便有文件也不算数
UNSETTLED_STATES = frozenset({ReleaseState.QUEUED, ReleaseState.RUNNING, ReleaseState.FAILED})
# 已落定结论、应当持有证据文件的状态
SETTLED_STATES = frozenset({
    ReleaseState.AWAITING, ReleaseState.RETEST, ReleaseState.VERIFIED,
    ReleaseState.HELD, ReleaseState.RELEASED,
})

# 证据核验状态（返回给前端）
EVIDENCE_AVAILABLE = 'available'           # 文件、指纹、结论全部一致
EVIDENCE_PENDING = 'pending_verification'  # 待核：缺文件/缺指纹/结论对不上
EVIDENCE_UNSETTLED = 'unsettled'           # 台账尚未落定，谈不上证据

STATUS_TEXT = {
    EVIDENCE_AVAILABLE: '证据可用',
    EVIDENCE_PENDING: '待核',
    EVIDENCE_UNSETTLED: '未落定',
}

REASON_MISSING_FILE = '缺少拟合证据文件'
REASON_MISSING_FINGERPRINT = '证据文件缺少输入指纹'
REASON_MISSING_RESULT = '证据文件缺少拟合结论'
REASON_FINGERPRINT_MISMATCH = '证据指纹与校准单冻结输入不一致'
REASON_RESULT_MISMATCH = '证据结论与台账已落定结论不一致'
REASON_UNREADABLE = '证据文件无法读取或解析'


def evidence_path(release_id):
    return EVIDENCE_DIR / f'fit-{release_id}.json'


def _payload(release_id, input_hash, snapshot, result):
    return {'input_hash': input_hash, 'snapshot': snapshot, 'result': result}


def inspect_values(release_id, state, input_hash, snapshot, result):
    """只读核验证据卷文件与给定台账数据是否一致。"""
    if state in UNSETTLED_STATES or result is None:
        return {'status': EVIDENCE_UNSETTLED, 'reason': None,
                'label': STATUS_TEXT[EVIDENCE_UNSETTLED], 'file_path': None,
                'file_fingerprint': None}
    path = evidence_path(release_id)
    if not path.is_file():
        return {'status': EVIDENCE_PENDING, 'reason': REASON_MISSING_FILE,
                'label': STATUS_TEXT[EVIDENCE_PENDING], 'file_path': str(path),
                'file_fingerprint': None}
    try:
        raw = path.read_bytes()
        payload = json.loads(raw.decode('utf-8'))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return {'status': EVIDENCE_PENDING, 'reason': REASON_UNREADABLE,
                'label': STATUS_TEXT[EVIDENCE_PENDING], 'file_path': str(path),
                'file_fingerprint': None}
    if not isinstance(payload, dict) or not payload.get('input_hash'):
        reason = REASON_MISSING_FINGERPRINT
    elif not payload.get('result'):
        reason = REASON_MISSING_RESULT
    elif payload['input_hash'] != input_hash:
        reason = REASON_FINGERPRINT_MISMATCH
    elif payload.get('snapshot') != snapshot or payload['result'] != result:
        reason = REASON_RESULT_MISMATCH
    else:
        reason = None
    status = EVIDENCE_AVAILABLE if reason is None else EVIDENCE_PENDING
    return {'status': status, 'reason': reason, 'label': STATUS_TEXT[status],
            'file_path': str(path),
            'file_fingerprint': hashlib.sha256(raw).hexdigest() if reason is None else None}


def inspect_evidence(entry):
    return inspect_values(entry.id, entry.state, entry.input_hash, entry.snapshot, entry.result)


def write_evidence(release_id, input_hash, snapshot, result, *, actor, action, batch_code=None):
    """按原输入快照与已落定结论原子落盘，返回核验结果。整个过程不调用拟合。"""
    path = evidence_path(release_id)
    current = inspect_values(release_id, ReleaseState.AWAITING, input_hash, snapshot, result)
    # 已存在且内容一致时幂等返回，不制造多余的落盘事件
    if current['status'] == EVIDENCE_AVAILABLE:
        return current
    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f'.fit-{release_id}.', suffix='.tmp', dir=EVIDENCE_DIR)
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as handle:
            json.dump(_payload(release_id, input_hash, snapshot, result), handle, ensure_ascii=False)
        os.replace(tmp_path, path)
    finally:
        tmp_path.unlink(missing_ok=True)
    verification = inspect_values(release_id, ReleaseState.AWAITING, input_hash, snapshot, result)
    if verification['status'] != EVIDENCE_AVAILABLE:
        raise OSError(f'证据落盘后核验未通过：{verification["reason"]}')
    with Session.begin() as db:
        record(db, actor, action,
               f'按原输入 {input_hash} 与已落定结论重建证据文件 {path.name}', batch_code)
    return verification


def backfill_evidence(release_id, actor):
    """主管补证据：仅允许对已落定且证据待核的校准单操作，不触碰台账结论。"""
    with Session.begin() as db:
        entry = db.get(CalibrationRelease, release_id, with_for_update=True)
        if entry is None:
            raise HTTPException(404, '没有找到该校准单')
        if entry.state not in SETTLED_STATES or entry.result is None:
            raise HTTPException(409, '台账尚未落定结论的校准单不能补证据')
        # 快照校验仍只核对冻结输入，避免被篡改的快照落盘成证据
        if checksum(entry.snapshot) != entry.input_hash:
            raise HTTPException(409, '校准单冻结输入校验失败，不能据此补证据')
        if inspect_evidence(entry)['status'] == EVIDENCE_AVAILABLE:
            return inspect_evidence(entry)
        values = (entry.id, entry.input_hash, entry.snapshot, entry.result, entry.snapshot.get('batch'))
    release_id_, input_hash, snapshot, result, batch_code = values
    return write_evidence(release_id_, input_hash, snapshot, result,
                          actor=actor, action='evidence.backfill', batch_code=batch_code)


def recover_pending_evidence(limit=20):
    """扫描已落定但证据待核的校准单（worker 中断恢复等场景），用原输入与结论补齐。

    已可用的文件保持原样；队列中或计算中的游离文件不在此处处理，仍由 settle 的
    版本守卫决定，迟到的旧试算不会盖掉已确认的文件。
    """
    recovered = []
    with Session() as db:
        entries = db.scalars(select(CalibrationRelease).where(
            CalibrationRelease.state.in_([state.value for state in SETTLED_STATES]))
            .order_by(CalibrationRelease.id)).all()
        pending = []
        for entry in entries:
            if inspect_evidence(entry)['status'] == EVIDENCE_PENDING:
                pending.append((entry.id, entry.input_hash, entry.snapshot, entry.result,
                                entry.snapshot.get('batch')))
    for release_id, input_hash, snapshot, result, batch_code in pending[:limit]:
        try:
            write_evidence(release_id, input_hash, snapshot, result,
                           actor='worker', action='evidence.recovered', batch_code=batch_code)
        except OSError:
            log.exception('evidence recovery for calibration %s blocked by storage', release_id)
            continue
        recovered.append(release_id)
    return recovered
