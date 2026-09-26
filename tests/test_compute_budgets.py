from __future__ import annotations

from datetime import UTC, datetime

from app.compute.service import ComputeOperationsService
from app.core.clock import FrozenClock
from app.database import get_connection


TEMPLATE = {
    "code": "solver-a",
    "name": "方程求解模板",
    "algorithm": "solver-a",
    "parameter_schema": {
        "iterations": {"type": "integer", "required": True, "minimum": 1, "maximum": 10000},
    },
    "default_parameters": {},
    "max_runtime_seconds": 300,
    "max_attempts": 2,
    "est_cpu_seconds": 60,
    "est_memory_gb_hours": 1.5,
}


def submit_payload(key: str, *, project: str = "project-a", user: str = "researcher-1", priority: int = 50) -> dict:
    return {
        "template_code": "solver-a",
        "project_code": project,
        "requested_by": user,
        "parameters": {"iterations": 100},
        "priority": priority,
        "idempotency_key": key,
    }


def create_template(client) -> None:
    response = client.post("/api/compute/templates?actor=administrator", json=TEMPLATE)
    assert response.status_code == 201, response.text


def set_budget(client, project: str, cpu: int, memory: float, tasks: int) -> dict:
    response = client.put(
        "/api/compute/budgets?actor=administrator",
        json={
            "project_code": project,
            "cpu_seconds_limit": cpu,
            "memory_gb_hours_limit": memory,
            "task_count_limit": tasks,
            "reason": "周期预算核定",
        },
    )
    assert response.status_code == 200, response.text
    return response.json()


def test_submit_precheck_is_visible_but_not_blocking(client):
    create_template(client)
    set_budget(client, "project-a", 1000, 10.0, 5)
    fits = client.post("/api/compute/tasks", json=submit_payload("precheck-fits"))
    assert fits.status_code == 202
    precheck = fits.json()["budget_precheck"]
    assert precheck["enforced"] is True
    assert precheck["fits"] is True
    assert precheck["estimate"] == {"cpu_seconds": 60, "memory_gb_hours": 1.5, "task_count": 1}
    assert precheck["available"]["cpu_seconds"] == 1000

    set_budget(client, "project-tiny", 10, 0.5, 0)
    overflow = client.post("/api/compute/tasks", json=submit_payload("precheck-overflow", project="project-tiny"))
    assert overflow.status_code == 202
    precheck = overflow.json()["budget_precheck"]
    assert precheck["fits"] is False
    assert precheck["shortages"] == ["cpu_seconds", "memory_gb_hours", "task_count"]

    free = client.post("/api/compute/tasks", json=submit_payload("precheck-free", project="project-none"))
    assert free.status_code == 202
    assert free.json()["budget_precheck"]["enforced"] is False


def test_claim_reserves_atomically_and_skips_over_budget(client):
    create_template(client)
    set_budget(client, "project-a", 100, 10.0, 5)
    first = client.post("/api/compute/tasks", json=submit_payload("claim-first", priority=90)).json()
    second = client.post("/api/compute/tasks", json=submit_payload("claim-second", priority=80)).json()
    other = client.post("/api/compute/tasks", json=submit_payload("claim-other", project="project-b", priority=70)).json()

    claimed = client.post("/api/compute/tasks/claim", json={"worker_id": "w1", "capabilities": ["solver-a"], "lease_seconds": 60})
    assert claimed.json()["task"]["id"] == first["id"]
    budget = client.get("/api/compute/budgets/project-a").json()
    assert budget["reserved"] == {"cpu_seconds": 60, "memory_gb_hours": 1.5, "task_count": 1}
    assert budget["available"]["cpu_seconds"] == 40

    skipped = client.post("/api/compute/tasks/claim", json={"worker_id": "w2", "capabilities": ["solver-a"], "lease_seconds": 60})
    assert skipped.json()["task"]["id"] == other["id"]
    blocked = client.get(f"/api/compute/task-details/{second['id']}").json()
    assert blocked["status"] == "queued"
    assert blocked["queue_reason_code"] == "budget_exceeded"
    assert "预算不足" in blocked["queue_reason_message"]

    budget = client.get("/api/compute/budgets/project-a").json()
    assert budget["rejected_count"] == 1
    empty = client.post("/api/compute/tasks/claim", json={"worker_id": "w3", "capabilities": ["solver-a"], "lease_seconds": 60})
    assert empty.json()["task"] is None
    assert client.get("/api/compute/budgets/project-a").json()["rejected_count"] == 1


def test_complete_and_fail_settle_actual_metrics(client):
    create_template(client)
    set_budget(client, "project-a", 100, 10.0, 5)
    task = client.post("/api/compute/tasks", json=submit_payload("settle-one")).json()
    client.post("/api/compute/tasks/claim", json={"worker_id": "w1", "capabilities": ["solver-a"], "lease_seconds": 60})
    completed = client.post(
        f"/api/compute/tasks/{task['id']}/complete",
        json={"worker_id": "w1", "result": {"value": 1}, "metrics": {"cpu_seconds": 40, "memory_gb_hours": 1.0}},
    )
    assert completed.status_code == 200
    budget = client.get("/api/compute/budgets/project-a").json()
    assert budget["reserved"] == {"cpu_seconds": 0, "memory_gb_hours": 0.0, "task_count": 0}
    assert budget["used"] == {"cpu_seconds": 40, "memory_gb_hours": 1.0, "task_count": 1}
    assert budget["available"]["cpu_seconds"] == 60

    second = client.post("/api/compute/tasks", json=submit_payload("settle-two")).json()
    client.post("/api/compute/tasks/claim", json={"worker_id": "w1", "capabilities": ["solver-a"], "lease_seconds": 60})
    failed = client.post(
        f"/api/compute/tasks/{second['id']}/fail",
        json={"worker_id": "w1", "error_code": "numeric_error", "message": "数值不收敛", "retryable": True, "metrics": {"cpu_seconds": 25}},
    )
    assert failed.status_code == 200
    assert failed.json()["status"] == "queued"
    budget = client.get("/api/compute/budgets/project-a").json()
    assert budget["reserved"]["cpu_seconds"] == 0
    assert budget["used"]["cpu_seconds"] == 65
    assert budget["used"]["task_count"] == 2


def test_lease_recovery_releases_reservation(client):
    from app.database import init_db

    init_db()
    clock = FrozenClock(datetime(2026, 9, 26, 2, 0, tzinfo=UTC))
    service = ComputeOperationsService(get_connection(), clock)
    service.create_template(TEMPLATE, "administrator")
    service.set_budget(
        {"project_code": "project-a", "period": None, "cpu_seconds_limit": 100, "memory_gb_hours_limit": 10.0, "task_count_limit": 5, "reason": "周期预算核定"},
        "administrator",
    )
    task = service.submit(submit_payload("recovery-one"))
    claimed = service.claim("worker-a", ["solver-a"], 10)
    assert claimed and claimed["id"] == task["id"]
    assert service.budget_summary("project-a")["reserved"]["cpu_seconds"] == 60

    clock.advance(seconds=11)
    recovered = service.recover_expired()
    assert recovered["recovered"] == [task["id"]]
    budget = service.budget_summary("project-a")
    assert budget["reserved"] == {"cpu_seconds": 0, "memory_gb_hours": 0.0, "task_count": 0}
    assert budget["used"] == {"cpu_seconds": 0, "memory_gb_hours": 0.0, "task_count": 0}
    assert service.get_task(task["id"])["status"] == "queued"


def test_budget_adjustments_require_reason_and_are_audited(client):
    create_template(client)
    set_budget(client, "project-a", 100, 10.0, 5)
    missing_reason = client.post(
        "/api/compute/budgets/adjustments?actor=administrator",
        json={"project_code": "project-a", "cpu_seconds_delta": 50},
    )
    assert missing_reason.status_code == 422

    adjusted = client.post(
        "/api/compute/budgets/adjustments?actor=administrator",
        json={"project_code": "project-a", "cpu_seconds_delta": 50, "task_count_delta": 2, "reason": "月底冲刺追加"},
    )
    assert adjusted.status_code == 200
    assert adjusted.json()["limits"] == {"cpu_seconds": 150, "memory_gb_hours": 10.0, "task_count": 7}

    negative = client.post(
        "/api/compute/budgets/adjustments?actor=administrator",
        json={"project_code": "project-a", "cpu_seconds_delta": -1000, "reason": "错误调整回滚"},
    )
    assert negative.status_code == 422

    history = client.get("/api/compute/budgets/project-a/adjustments").json()["items"]
    assert [item["reason"] for item in history] == ["月底冲刺追加", "周期预算核定"]
    assert history[0]["cpu_seconds_delta"] == 50
    assert history[0]["actor"] == "administrator"


def test_budget_period_rollover_does_not_mix_accounts(client):
    from app.database import init_db

    init_db()
    clock = FrozenClock(datetime(2026, 9, 30, 23, 0, tzinfo=UTC))
    service = ComputeOperationsService(get_connection(), clock)
    service.create_template(TEMPLATE, "administrator")
    service.set_budget(
        {"project_code": "project-a", "period": None, "cpu_seconds_limit": 1000, "memory_gb_hours_limit": 100.0, "task_count_limit": 10, "reason": "九月预算"},
        "administrator",
    )
    september_task = service.submit(submit_payload("rollover-september"))
    claimed = service.claim("worker-a", ["solver-a"], 7200)
    assert claimed and claimed["id"] == september_task["id"]

    clock.advance(hours=2)  # 跨入十月，旧任务仍持有九月账本的预留
    service.complete(september_task["id"], "worker-a", {"value": 1}, {"cpu_seconds": 80, "memory_gb_hours": 4.0})

    october_task = service.submit(submit_payload("rollover-october"))
    precheck = october_task["budget_precheck"]
    assert precheck["enforced"] is True and precheck["fits"] is True
    assert precheck["available"] == {"cpu_seconds": 1000, "memory_gb_hours": 100.0, "task_count": 10}
    claimed_october = service.claim("worker-b", ["solver-a"], 60)
    assert claimed_october and claimed_october["id"] == october_task["id"]

    september = service.budget_summary("project-a", "2026-09-15")
    assert september["used"] == {"cpu_seconds": 80, "memory_gb_hours": 4.0, "task_count": 1}
    assert september["reserved"] == {"cpu_seconds": 0, "memory_gb_hours": 0.0, "task_count": 0}

    october = service.budget_summary("project-a")
    assert october["used"] == {"cpu_seconds": 0, "memory_gb_hours": 0.0, "task_count": 0}
    assert october["reserved"] == {"cpu_seconds": 60, "memory_gb_hours": 1.5, "task_count": 1}
    assert october["limits"] == {"cpu_seconds": 1000, "memory_gb_hours": 100.0, "task_count": 10}

    history = service.list_budget_adjustments("project-a")
    assert any(item["actor"] == "system" and "周期结转" in item["reason"] for item in history)


def test_summary_explains_budget_breakdown(client):
    create_template(client)
    set_budget(client, "project-a", 100, 10.0, 5)
    client.post("/api/compute/tasks", json=submit_payload("summary-one", priority=90))
    client.post("/api/compute/tasks", json=submit_payload("summary-two", priority=80))
    client.post("/api/compute/tasks/claim", json={"worker_id": "w1", "capabilities": ["solver-a"], "lease_seconds": 60})
    client.post("/api/compute/tasks/claim", json={"worker_id": "w2", "capabilities": ["solver-a"], "lease_seconds": 60})

    summary = client.get("/api/compute/summary").json()
    budget = next(item for item in summary["budgets"] if item["project_code"] == "project-a")
    assert budget["limits"]["cpu_seconds"] == 100
    assert budget["reserved"]["cpu_seconds"] == 60
    assert budget["used"]["cpu_seconds"] == 0
    assert budget["available"]["cpu_seconds"] == 40
    assert budget["rejected_count"] == 1
    assert budget["active_reservations"] == 1

    detail = client.get("/api/compute/budgets/project-a").json()
    assert detail["available"] == {"cpu_seconds": 40, "memory_gb_hours": 8.5, "task_count": 4}
    unknown = client.get("/api/compute/budgets/project-unknown").json()
    assert unknown["enforced"] is False
