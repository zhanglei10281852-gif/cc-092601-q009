from __future__ import annotations

from datetime import UTC, datetime

from app.compute.service import ComputeOperationsService
from app.core.clock import FrozenClock
from app.database import get_connection, init_db


TEMPLATE = {
    "code": "solver-budget",
    "name": "预算测试模板",
    "algorithm": "solver-budget",
    "parameter_schema": {
        "size": {"type": "integer", "required": True, "minimum": 1, "maximum": 1000},
    },
    "default_parameters": {},
    "max_runtime_seconds": 120,
    "max_attempts": 2,
    "estimated_cpu_seconds": 100,
    "estimated_memory_hours": 2.0,
}


def submit_payload(key: str, *, project: str = "project-a", user: str = "researcher-1") -> dict:
    return {
        "template_code": "solver-budget",
        "project_code": project,
        "requested_by": user,
        "parameters": {"size": 10},
        "priority": 50,
        "idempotency_key": key,
    }


def create_template(client, **overrides) -> dict:
    payload = {**TEMPLATE, **overrides}
    response = client.post("/api/compute/templates?actor=administrator", json=payload)
    assert response.status_code == 201, response.text
    return response.json()


def adjust(client, project: str, **payload) -> dict:
    response = client.post(f"/api/compute/budgets/adjust?actor=administrator", json={"project_code": project, **payload})
    assert response.status_code == 200, response.text
    return response.json()


def summary(client, project: str, period: str | None = None) -> dict:
    params = {"project_code": project}
    if period:
        params["period"] = period
    response = client.get("/api/compute/budgets/summary", params=params)
    assert response.status_code == 200, response.text
    return response.json()


def test_template_estimate_defaults_and_untracked_precheck(client):
    minimal = {
        "code": "solver-default",
        "name": "默认估算模板",
        "algorithm": "solver-default",
        "parameter_schema": {"size": {"type": "integer", "required": True}},
        "max_runtime_seconds": 120,
    }
    created = client.post("/api/compute/templates?actor=administrator", json=minimal)
    assert created.status_code == 201, created.text
    assert created.json()["estimated_cpu_seconds"] == 120  # 缺省按最大运行时长保守估计
    assert created.json()["estimated_memory_hours"] == 0.0
    create_template(client)
    submitted = client.post("/api/compute/tasks", json=submit_payload("precheck-untracked"))
    assert submitted.status_code == 202
    precheck = submitted.json()["budget_precheck"]
    assert precheck["tracked"] is False
    assert precheck["fits"] is None
    assert precheck["required"] == {"cpu_seconds": 100, "memory_hours": 2.0, "tasks": 1}


def test_claim_reserves_and_completion_settles_actuals(client):
    create_template(client)
    adjust(client, "project-a", cpu_seconds=1000, memory_hours=10.0, tasks=5, reason="九月初始额度")
    submitted = client.post("/api/compute/tasks", json=submit_payload("reserve-000001")).json()
    assert submitted["budget_precheck"]["fits"] is True
    assert submitted["budget_precheck"]["available"] == {"cpu_seconds": 1000, "memory_hours": 10.0, "tasks": 5}

    claimed = client.post("/api/compute/tasks/claim", json={"worker_id": "w1", "capabilities": ["solver-budget"], "lease_seconds": 60})
    assert claimed.json()["task"]["id"] == submitted["id"]
    state = summary(client, "project-a")
    assert state["reserved"] == {"cpu_seconds": 100, "memory_hours": 2.0, "tasks": 1}
    assert state["available"] == {"cpu_seconds": 900, "memory_hours": 8.0, "tasks": 4}
    assert state["used"] == {"cpu_seconds": 0, "memory_hours": 0.0, "tasks": 0}

    completed = client.post(
        f"/api/compute/tasks/{submitted['id']}/complete",
        json={"worker_id": "w1", "result": {"value": 1}, "metrics": {"cpu_seconds": 40, "memory_hours": 1.5}},
    )
    assert completed.status_code == 200
    state = summary(client, "project-a")
    assert state["reserved"] == {"cpu_seconds": 0, "memory_hours": 0.0, "tasks": 0}
    assert state["used"] == {"cpu_seconds": 40, "memory_hours": 1.5, "tasks": 1}
    assert state["available"] == {"cpu_seconds": 960, "memory_hours": 8.5, "tasks": 4}

    events = client.get("/api/compute/budgets/events", params={"project_code": "project-a"}).json()["items"]
    assert [event["event_type"] for event in events] == ["grant", "reserve", "release", "settle"]
    assert events[0]["reason"] == "九月初始额度" and events[0]["actor"] == "administrator"
    assert events[-1]["cpu_seconds"] == 40 and events[-1]["actor"] == "w1"


def test_insufficient_budget_keeps_task_queued_with_reason(client):
    create_template(client)
    adjust(client, "project-b", cpu_seconds=50, memory_hours=10.0, tasks=5, reason="小额试用额度")
    submitted = client.post("/api/compute/tasks", json=submit_payload("deny-000001", project="project-b")).json()
    assert submitted["budget_precheck"]["fits"] is False  # 预检查可见但不拦截提交

    claim = client.post("/api/compute/tasks/claim", json={"worker_id": "w1", "capabilities": ["solver-budget"], "lease_seconds": 60})
    assert claim.status_code == 200 and claim.json()["task"] is None
    details = client.get(f"/api/compute/task-details/{submitted['id']}").json()
    assert details["status"] == "queued"
    assert details["last_error_code"] == "budget_exceeded"
    assert "project-b" in details["last_error_message"]
    assert summary(client, "project-b")["rejected_tasks"] == 1

    again = client.post("/api/compute/tasks/claim", json={"worker_id": "w2", "capabilities": ["solver-budget"], "lease_seconds": 60})
    assert again.json()["task"] is None
    assert summary(client, "project-b")["rejected_tasks"] == 1  # 同一周期同一任务不重复计数

    adjust(client, "project-b", cpu_seconds=200, reason="主管审批追加")
    claimed = client.post("/api/compute/tasks/claim", json={"worker_id": "w3", "capabilities": ["solver-budget"], "lease_seconds": 60})
    assert claimed.json()["task"]["id"] == submitted["id"]
    state = summary(client, "project-b")
    assert state["reserved"] == {"cpu_seconds": 100, "memory_hours": 2.0, "tasks": 1}
    assert state["rejected_tasks"] == 1


def test_budget_adjustments_require_reason_and_nonzero_delta(client):
    create_template(client)
    missing_reason = client.post("/api/compute/budgets/adjust?actor=administrator", json={"project_code": "project-c", "cpu_seconds": 100})
    assert missing_reason.status_code == 422
    zero_delta = client.post("/api/compute/budgets/adjust?actor=administrator", json={"project_code": "project-c", "reason": "零调整"})
    assert zero_delta.status_code == 422
    adjust(client, "project-c", cpu_seconds=100, tasks=2, reason="初始额度")
    negative = client.post("/api/compute/budgets/adjust?actor=administrator", json={"project_code": "project-c", "cpu_seconds": -500, "reason": "超额核减"})
    assert negative.status_code == 422
    events = client.get("/api/compute/budgets/events", params={"project_code": "project-c"}).json()["items"]
    assert len(events) == 1 and events[0]["event_type"] == "grant"
    assert events[0]["reason"] == "初始额度" and events[0]["actor"] == "administrator"


def test_failure_settles_actuals_and_retry_reserves_again(client):
    init_db()
    clock = FrozenClock(datetime(2026, 9, 26, 2, 0, tzinfo=UTC))
    service = ComputeOperationsService(get_connection(), clock)
    service.create_template(TEMPLATE, "administrator")
    service.adjust_budget({"project_code": "project-d", "period": None, "cpu_seconds": 1000, "memory_hours": 10.0, "tasks": 5, "reason": "初始额度"}, "administrator")
    submitted = service.submit(submit_payload("fail-000001", project="project-d"))
    service.claim("w1", ["solver-budget"], 60)
    failed = service.fail(submitted["id"], "w1", "numeric_error", "不收敛", True, {"cpu_seconds": 10, "memory_hours": 0.5})
    assert failed["status"] == "queued"
    state = service.budget_summary("project-d")
    assert state["reserved"] == {"cpu_seconds": 0, "memory_hours": 0.0, "tasks": 0}
    assert state["used"] == {"cpu_seconds": 10, "memory_hours": 0.5, "tasks": 1}

    clock.advance(seconds=3)  # 越过失败退避
    claimed = service.claim("w2", ["solver-budget"], 60)
    assert claimed["id"] == submitted["id"]
    assert service.budget_summary("project-d")["reserved"] == {"cpu_seconds": 100, "memory_hours": 2.0, "tasks": 1}

    # 未上报指标时按预留量结算，避免预算泄漏
    final = service.fail(submitted["id"], "w2", "hardware", "节点故障", False)
    assert final["status"] == "failed"
    state = service.budget_summary("project-d")
    assert state["used"] == {"cpu_seconds": 110, "memory_hours": 2.5, "tasks": 2}
    assert state["reserved"] == {"cpu_seconds": 0, "memory_hours": 0.0, "tasks": 0}


def test_invalid_settlement_metrics_rejected(client):
    create_template(client)
    adjust(client, "project-e", cpu_seconds=1000, memory_hours=10.0, tasks=5, reason="初始额度")
    submitted = client.post("/api/compute/tasks", json=submit_payload("metrics-000001", project="project-e")).json()
    client.post("/api/compute/tasks/claim", json={"worker_id": "w1", "capabilities": ["solver-budget"], "lease_seconds": 60})
    bad = client.post(
        f"/api/compute/tasks/{submitted['id']}/complete",
        json={"worker_id": "w1", "result": {}, "metrics": {"cpu_seconds": -5}},
    )
    assert bad.status_code == 422
    # 结算失败回滚，预留仍然保留
    assert summary(client, "project-e")["reserved"] == {"cpu_seconds": 100, "memory_hours": 2.0, "tasks": 1}


def test_untracked_project_runs_without_budget(client):
    create_template(client)
    submitted = client.post("/api/compute/tasks", json=submit_payload("untracked-000001", project="project-z")).json()
    assert submitted["budget_precheck"]["tracked"] is False
    claimed = client.post("/api/compute/tasks/claim", json={"worker_id": "w1", "capabilities": ["solver-budget"], "lease_seconds": 60})
    assert claimed.json()["task"]["id"] == submitted["id"]
    completed = client.post(f"/api/compute/tasks/{submitted['id']}/complete", json={"worker_id": "w1", "result": {}, "metrics": {}})
    assert completed.status_code == 200
    assert summary(client, "project-z")["tracked"] is False


def test_settlement_crosses_period_without_mixing(client):
    init_db()
    clock = FrozenClock(datetime(2026, 9, 26, 2, 0, tzinfo=UTC))
    service = ComputeOperationsService(get_connection(), clock)
    service.create_template(TEMPLATE, "administrator")
    service.adjust_budget({"project_code": "project-f", "period": None, "cpu_seconds": 1000, "memory_hours": 10.0, "tasks": 5, "reason": "九月额度"}, "administrator")
    submitted = service.submit(submit_payload("period-000001", project="project-f"))
    claimed = service.claim("worker-a", ["solver-budget"], 10)
    assert claimed["id"] == submitted["id"] and claimed["budget_period"] == "2026-09"

    clock.advance(days=10)  # 进入 2026-10 周期
    service.adjust_budget({"project_code": "project-f", "period": None, "cpu_seconds": 500, "memory_hours": 5.0, "tasks": 3, "reason": "十月额度"}, "administrator")
    service.complete(submitted["id"], "worker-a", {"value": 1}, {"cpu_seconds": 30, "memory_hours": 1.0})

    september = service.budget_summary("project-f", "2026-09")
    assert september["used"] == {"cpu_seconds": 30, "memory_hours": 1.0, "tasks": 1}
    assert september["reserved"] == {"cpu_seconds": 0, "memory_hours": 0.0, "tasks": 0}
    october = service.budget_summary("project-f", "2026-10")
    assert october["granted"] == {"cpu_seconds": 500, "memory_hours": 5.0, "tasks": 3}
    assert october["used"] == {"cpu_seconds": 0, "memory_hours": 0.0, "tasks": 0}
    assert october["reserved"] == {"cpu_seconds": 0, "memory_hours": 0.0, "tasks": 0}

    # 新周期领取的任务预留计入新周期，与旧周期互不影响
    followup = service.submit(submit_payload("period-000002", project="project-f"))
    claimed = service.claim("worker-b", ["solver-budget"], 10)
    assert claimed["id"] == followup["id"] and claimed["budget_period"] == "2026-10"
    assert service.budget_summary("project-f", "2026-10")["reserved"] == {"cpu_seconds": 100, "memory_hours": 2.0, "tasks": 1}
    assert service.budget_summary("project-f", "2026-09")["reserved"] == {"cpu_seconds": 0, "memory_hours": 0.0, "tasks": 0}


def test_lease_recovery_releases_reservation(client):
    init_db()
    clock = FrozenClock(datetime(2026, 9, 26, 2, 0, tzinfo=UTC))
    service = ComputeOperationsService(get_connection(), clock)
    service.create_template(TEMPLATE, "administrator")
    service.adjust_budget({"project_code": "project-g", "period": None, "cpu_seconds": 1000, "memory_hours": 10.0, "tasks": 5, "reason": "初始额度"}, "administrator")
    submitted = service.submit(submit_payload("lease-000001", project="project-g"))
    claimed = service.claim("worker-a", ["solver-budget"], 10)
    assert claimed["budget_reserved"] == 1
    assert service.budget_summary("project-g")["reserved"] == {"cpu_seconds": 100, "memory_hours": 2.0, "tasks": 1}

    clock.advance(seconds=11)
    recovered = service.recover_expired()
    assert recovered["recovered"] == [submitted["id"]]
    state = service.budget_summary("project-g")
    assert state["reserved"] == {"cpu_seconds": 0, "memory_hours": 0.0, "tasks": 0}
    assert state["used"] == {"cpu_seconds": 0, "memory_hours": 0.0, "tasks": 0}
    task = service.get_task(submitted["id"])
    assert task["budget_reserved"] == 0 and task["status"] == "queued"
    events = service.budget_events("project-g")
    assert [event["event_type"] for event in events] == ["grant", "reserve", "release"]
    assert "租约过期" in events[-1]["reason"]

    # 取消中的任务租约失联同样释放预留并转为取消
    reclaimed = service.claim("worker-b", ["solver-budget"], 10)
    assert reclaimed["id"] == submitted["id"]
    service.cancel(submitted["id"], "administrator", "项目方向调整")
    clock.advance(seconds=11)
    result = service.recover_expired()
    assert result["cancelled"] == [submitted["id"]]
    assert service.budget_summary("project-g")["reserved"] == {"cpu_seconds": 0, "memory_hours": 0.0, "tasks": 0}
