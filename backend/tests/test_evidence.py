"""证据对账规则测试：游离文件、待核判定、主管补证据、中断恢复。"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from _harness import FakeEntry, FakeRegistry, install_common_stubs, temp_evidence_dir  # noqa: E402

install_common_stubs()
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


def make_snapshot():
    return {'batch': 'FD-9001', 'recipe': 'AP-26R3', 'curve_hash': 'c' * 64,
            'features': [14.2, 1.0, 243, 30.0, -20.0], 'sample_pairs': {},
            'window': [9.0, 16.0],
            'training': [{'batch': f'FD-{i}', 'features': [1.0 * i], 'observed': 10.0 + i} for i in range(3)]}


def make_result():
    return {'prediction_pct': 12.34, 'loo_rmse': 0.5, 'n_references': 3, 'alpha': 0.65,
            'seed': 1062, 'solver': 'svd', 'lambdas': {}, 'coefficients': {}, 'feature_names': []}


def main():
    registry = FakeRegistry()
    Session.registry = registry
    snapshot = make_snapshot()
    digest = checksum(snapshot)
    result = make_result()

    with temp_evidence_dir() as tmp:
        evidence.EVIDENCE_DIR = tmp

        entry_id = 7001
        path = evidence.evidence_path(entry_id)

        # 1. 台账未落定时，卷上先有文件也不能当证据（游离文件）
        unsettled = FakeEntry(entry_id, ReleaseState.QUEUED, digest, snapshot, None)
        path.write_text(json.dumps({'input_hash': digest, 'snapshot': snapshot, 'result': result}), encoding='utf-8')
        view = evidence.inspect_evidence(unsettled)
        check('未落定台账的游离文件不算证据', view['status'] == evidence.EVIDENCE_UNSETTLED, str(view))

        # 台账落定后，游离文件恰好与原输入/结论一致 → 核验可用
        settled = FakeEntry(entry_id, ReleaseState.AWAITING, digest, snapshot, result)
        registry.put(settled)
        view = evidence.inspect_evidence(settled)
        check('落定后同内容文件核验可用', view['status'] == evidence.EVIDENCE_AVAILABLE, str(view))

        # 2. 缺文件 → 待核
        missing_id = 7002
        missing = FakeEntry(missing_id, ReleaseState.HELD, digest, snapshot, result)
        registry.put(missing)
        view = evidence.inspect_evidence(missing)
        check('缺文件标为待核', view['status'] == evidence.EVIDENCE_PENDING
              and view['reason'] == evidence.REASON_MISSING_FILE, str(view))

        # 3. 缺指纹 → 待核
        fp_id = 7003
        fp_entry = FakeEntry(fp_id, ReleaseState.AWAITING, digest, snapshot, result)
        registry.put(fp_entry)
        evidence.evidence_path(fp_id).write_text(json.dumps({'snapshot': snapshot, 'result': result}), encoding='utf-8')
        view = evidence.inspect_evidence(fp_entry)
        check('缺指纹标为待核', view['status'] == evidence.EVIDENCE_PENDING
              and view['reason'] == evidence.REASON_MISSING_FINGERPRINT, str(view))

        # 4. 指纹对不上 → 待核
        bad_fp_id = 7004
        bad_fp = FakeEntry(bad_fp_id, ReleaseState.AWAITING, digest, snapshot, result)
        registry.put(bad_fp)
        evidence.evidence_path(bad_fp_id).write_text(
            json.dumps({'input_hash': 'deadbeef', 'snapshot': snapshot, 'result': result}), encoding='utf-8')
        view = evidence.inspect_evidence(bad_fp)
        check('指纹不一致标为待核', view['status'] == evidence.EVIDENCE_PENDING
              and view['reason'] == evidence.REASON_FINGERPRINT_MISMATCH, str(view))

        # 5. 结论对不上 → 待核
        bad_result_id = 7005
        bad_result = FakeEntry(bad_result_id, ReleaseState.AWAITING, digest, snapshot, result)
        registry.put(bad_result)
        tampered = dict(result, prediction_pct=99.9)
        evidence.evidence_path(bad_result_id).write_text(
            json.dumps({'input_hash': digest, 'snapshot': snapshot, 'result': tampered}), encoding='utf-8')
        view = evidence.inspect_evidence(bad_result)
        check('结论不一致标为待核', view['status'] == evidence.EVIDENCE_PENDING
              and view['reason'] == evidence.REASON_RESULT_MISMATCH, str(view))

        # 6. 文件损坏无法解析 → 待核
        broken_id = 7006
        broken = FakeEntry(broken_id, ReleaseState.RETEST, digest, snapshot, result)
        registry.put(broken)
        evidence.evidence_path(broken_id).write_bytes(b'{not valid json')
        view = evidence.inspect_evidence(broken)
        check('损坏文件标为待核', view['status'] == evidence.EVIDENCE_PENDING
              and view['reason'] == evidence.REASON_UNREADABLE, str(view))

        # 7. 主管补证据：用原输入与结论重建，不拟合
        before_events = len(registry.events)
        verification = evidence.backfill_evidence(missing_id, 'supervisor')
        check('补证据后可用', verification['status'] == evidence.EVIDENCE_AVAILABLE, str(verification))
        payload = json.loads(evidence.evidence_path(missing_id).read_text(encoding='utf-8'))
        check('补证据内容来自原输入与已落定结论',
              payload['input_hash'] == digest and payload['snapshot'] == snapshot and payload['result'] == result)
        check('补证据写入审计事件', len(registry.events) > before_events)

        # 8. 已可用时补证据幂等，不重复落盘事件
        events_before = len(registry.events)
        verification = evidence.backfill_evidence(missing_id, 'supervisor')
        check('证据可用时补证据幂等', verification['status'] == evidence.EVIDENCE_AVAILABLE
              and len(registry.events) == events_before)

        # 9. 未落定单据拒绝补证据
        queued_id = 7007
        queued = FakeEntry(queued_id, ReleaseState.RUNNING, digest, snapshot, None)
        registry.put(queued)
        try:
            evidence.backfill_evidence(queued_id, 'supervisor')
            check('未落定单据拒绝补证据', False, '未抛出异常')
        except Exception as exc:
            check('未落定单据拒绝补证据', getattr(exc, 'status_code', None) == 409, str(exc))

        # 10. 中断恢复：所有待核单据按原输入/结论补齐，已可用文件不动
        recovered = evidence.recover_pending_evidence()
        for rid in (fp_id, bad_fp_id, bad_result_id, broken_id):
            check(f'恢复后 {rid} 证据可用',
                  evidence.inspect_evidence(registry.get(rid))['status'] == evidence.EVIDENCE_AVAILABLE)
        check('恢复覆盖全部待核单据', set(recovered) == {fp_id, bad_fp_id, bad_result_id, broken_id}, str(recovered))
        check('恢复不触碰已可用的单据', entry_id not in recovered and missing_id not in recovered)

        # 11. 已签发单据补证据后结论照旧
        released_id = 7008
        released = FakeEntry(released_id, ReleaseState.RELEASED, digest, snapshot, result)
        registry.put(released)
        verification = evidence.backfill_evidence(released_id, 'supervisor')
        check('已签发单据可补证据', verification['status'] == evidence.EVIDENCE_AVAILABLE)
        check('签发结论保持不变', released.result == result and released.state == ReleaseState.RELEASED)

    print(f'\n{FAIL} failed, {PASS} passed')
    sys.exit(1 if FAIL else 0)


if __name__ == '__main__':
    main()
