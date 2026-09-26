from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from typing import Any, Callable

from app.compute.repository import ComputeRepository
from app.core.clock import Clock, SystemClock, to_storage
from app.core.errors import ConflictError, NotFoundError, ValidationError
from app.database import get_connection, transaction


def digest(value: Any) -> str:
    text = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode()).hexdigest()


class _ClaimRaceLost(Exception):
    """领取条件更新未命中时触发整体回滚，避免遗留预算预留。"""


class ComputeOperationsService:
    """管理计算模板、配额、项目预算、任务租约、结果版本和人工干预。"""

    def __init__(self, connection: sqlite3.Connection | None = None, clock: Clock | None = None) -> None:
        self.connection = connection or get_connection()
        self.clock = clock or SystemClock()
        self.repository = ComputeRepository(self.connection)

    def list_templates(self) -> list[dict[str, Any]]:
        return self.repository.active_templates()

    def create_template(self, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        self._validate_schema(payload["parameter_schema"], payload["default_parameters"])
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            if repository.template_by_code(payload["code"]):
                raise ConflictError("参数模板编码已存在")
            return repository.create_template(
                code=payload["code"], name=payload["name"], algorithm=payload["algorithm"],
                parameter_schema=payload["parameter_schema"], defaults=payload["default_parameters"],
                max_runtime_seconds=payload["max_runtime_seconds"], max_attempts=payload["max_attempts"],
                est_cpu_seconds=int(payload.get("est_cpu_seconds", 0)),
                est_memory_gb_hours=float(payload.get("est_memory_gb_hours", 0.0)),
                created_by=actor, now=now,
            )

    def set_quota(self, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            return ComputeRepository(connection).upsert_quota(actor=actor, now=now, **payload)

    def submit(self, payload: dict[str, Any]) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            template = repository.template_by_code(payload["template_code"])
            if template is None or not template["active"]:
                raise NotFoundError("参数模板不存在或已经停用")
            parameters = self._validate_parameters(template, payload["parameters"])
            precheck = self._budget_precheck(repository, payload["project_code"], template, now_value)
            existing = repository.task_by_idempotency(payload["requested_by"], payload["idempotency_key"])
            parameter_digest = digest(parameters)
            if existing is not None:
                if existing["parameter_digest"] != parameter_digest:
                    raise ConflictError("同一幂等键对应了不同的计算参数")
                result = dict(repository.task_by_id(existing["id"]))
                result["budget_precheck"] = precheck
                return result
            self._check_quota(repository, payload["requested_by"], now_value)
            result = repository.create_task(
                template_id=template["id"], project_code=payload["project_code"],
                requested_by=payload["requested_by"], parameters=parameters,
                parameter_digest=parameter_digest, priority=payload["priority"],
                idempotency_key=payload["idempotency_key"], max_attempts=template["max_attempts"], now=now,
            )
            result["budget_precheck"] = precheck
            return result

    def list_tasks(self, *, status: str | None = None, project_code: str | None = None, requested_by: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        return self.repository.list_tasks(status=status, project_code=project_code, requested_by=requested_by, limit=max(1, min(limit, 500)))

    def get_task(self, task_id: int) -> dict[str, Any]:
        row = self.repository.task_by_id(task_id)
        if row is None:
            raise NotFoundError("计算任务不存在")
        result = dict(row)
        result["results"] = self.repository.result_versions(task_id)
        result["interventions"] = self.repository.interventions(task_id)
        return result

    def claim(self, worker_id: str, capabilities: list[str], lease_seconds: int) -> dict[str, Any] | None:
        now_value = self.clock.now()
        now = to_storage(now_value)
        lease_until = to_storage(now_value + timedelta(seconds=lease_seconds))
        try:
            with transaction(immediate=True) as connection:
                repository = ComputeRepository(connection)
                for candidate in repository.queued_candidates(capabilities, now, limit=20):
                    estimate = {"cpu_seconds": int(candidate["est_cpu_seconds"]), "memory_gb_hours": float(candidate["est_memory_gb_hours"]), "task_count": 1}
                    account = self._ensure_current_account(repository, candidate["project_code"], now_value)
                    if account is not None:
                        if not repository.try_reserve_budget(account["id"], cpu_seconds=estimate["cpu_seconds"], memory_gb_hours=estimate["memory_gb_hours"], task_count=estimate["task_count"], now=now):
                            self._record_budget_rejection(repository, account, candidate, estimate, now)
                            continue
                        repository.create_budget_reservation(
                            account_id=account["id"], task_id=candidate["id"], attempt=int(candidate["attempt_count"]) + 1,
                            cpu_seconds=estimate["cpu_seconds"], memory_gb_hours=estimate["memory_gb_hours"], task_count=estimate["task_count"], now=now,
                        )
                    cursor = connection.execute(
                        "UPDATE compute_tasks SET status='running',attempt_count=attempt_count+1,lease_owner=?,lease_expires_at=?,started_at=COALESCE(started_at,?),queue_reason_code='',queue_reason_message='',updated_at=?,version=version+1 WHERE id=? AND status='queued'",
                        (worker_id, lease_until, now, now, candidate["id"]),
                    )
                    if cursor.rowcount != 1:
                        raise _ClaimRaceLost()
                    return dict(repository.task_by_id(candidate["id"]))
                return None
        except _ClaimRaceLost:
            return None

    def heartbeat(self, task_id: int, worker_id: str, lease_seconds: int) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        expires = to_storage(now_value + timedelta(seconds=lease_seconds))
        with transaction(immediate=True) as connection:
            cursor = connection.execute(
                "UPDATE compute_tasks SET lease_expires_at=?,updated_at=?,version=version+1 WHERE id=? AND status='running' AND lease_owner=?",
                (expires, now, task_id, worker_id),
            )
            if cursor.rowcount != 1:
                raise ConflictError("任务未由当前工作者持有")
            return dict(ComputeRepository(connection).task_by_id(task_id))

    def complete(self, task_id: int, worker_id: str, result: dict[str, Any], metrics: dict[str, Any]) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            if task["status"] != "running" or task["lease_owner"] != worker_id:
                raise ConflictError("任务未由当前工作者持有")
            version = int(connection.execute("SELECT COALESCE(MAX(version),0)+1 FROM compute_results WHERE task_id=?", (task_id,)).fetchone()[0])
            connection.execute(
                "INSERT INTO compute_results(task_id,version,result_json,metrics_json,result_digest,created_by,created_at) VALUES(?,?,?,?,?,?,?)",
                (task_id, version, json.dumps(result, ensure_ascii=False, sort_keys=True), json.dumps(metrics, ensure_ascii=False, sort_keys=True), digest({"result": result, "metrics": metrics}), worker_id, now),
            )
            connection.execute(
                "UPDATE compute_tasks SET status='succeeded',current_result_version=?,lease_owner='',lease_expires_at='',finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                (version, now, now, task_id),
            )
            self._settle_budget(repository, task_id, metrics, now)
            return dict(repository.task_by_id(task_id))

    def fail(self, task_id: int, worker_id: str, error_code: str, message: str, retryable: bool, metrics: dict[str, Any] | None = None) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            if task["status"] != "running" or task["lease_owner"] != worker_id:
                raise ConflictError("任务未由当前工作者持有")
            can_retry = retryable and int(task["attempt_count"]) < int(task["max_attempts"])
            status = "queued" if can_retry else "failed"
            delay = min(300, 2 ** max(0, int(task["attempt_count"]) - 1)) if can_retry else 0
            available = to_storage(now_value + timedelta(seconds=delay))
            connection.execute(
                "UPDATE compute_tasks SET status=?,available_at=?,lease_owner='',lease_expires_at='',last_error_code=?,last_error_message=?,finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                (status, available, error_code, message[:2000], None if can_retry else now, now, task_id),
            )
            self._settle_budget(repository, task_id, metrics or {}, now)
            return dict(repository.task_by_id(task_id))

    def cancel(self, task_id: int, actor: str, reason: str, batch_key: str = "") -> dict[str, Any]:
        return self._intervene(task_id, actor, reason, "cancel", batch_key, self._cancel_mutation)

    def retry(self, task_id: int, actor: str, reason: str, priority: int | None = None, batch_key: str = "") -> dict[str, Any]:
        def mutate(connection: sqlite3.Connection, task: sqlite3.Row, now: str) -> None:
            if task["status"] not in {"failed", "cancelled"}:
                raise ConflictError("只有失败或已取消任务可以人工重试")
            chosen = task["priority"] if priority is None else priority
            connection.execute("UPDATE compute_tasks SET status='queued',priority=?,available_at=?,lease_owner='',lease_expires_at='',queue_reason_code='',queue_reason_message='',finished_at=NULL,updated_at=?,version=version+1 WHERE id=?", (chosen, now, now, task["id"]))
        return self._intervene(task_id, actor, reason, "retry", batch_key, mutate)

    def set_priority(self, task_id: int, actor: str, reason: str, priority: int, batch_key: str = "") -> dict[str, Any]:
        def mutate(connection: sqlite3.Connection, task: sqlite3.Row, now: str) -> None:
            if task["status"] not in {"queued", "running"}:
                raise ConflictError("只有排队或运行中的任务可以调整优先级")
            connection.execute("UPDATE compute_tasks SET priority=?,updated_at=?,version=version+1 WHERE id=?", (priority, now, task["id"]))
        return self._intervene(task_id, actor, reason, "priority", batch_key, mutate)

    def batch_operation(self, payload: dict[str, Any]) -> dict[str, Any]:
        batch_key = digest({"actor": payload["actor"], "task_ids": payload["task_ids"], "operation": payload["operation"], "reason": payload["reason"]})
        succeeded: list[dict[str, Any]] = []
        failed: list[dict[str, Any]] = []
        for task_id in list(dict.fromkeys(payload["task_ids"])):
            try:
                if payload["operation"] == "cancel":
                    value = self.cancel(task_id, payload["actor"], payload["reason"], batch_key)
                elif payload["operation"] == "retry":
                    value = self.retry(task_id, payload["actor"], payload["reason"], payload.get("priority"), batch_key)
                else:
                    value = self.set_priority(task_id, payload["actor"], payload["reason"], int(payload["priority"]), batch_key)
                succeeded.append({"task_id": task_id, "status": value["status"], "version": value["version"]})
            except (ConflictError, NotFoundError) as exc:
                failed.append({"task_id": task_id, "code": exc.code, "message": exc.message})
        return {"batch_key": batch_key, "succeeded": succeeded, "failed": failed}

    def recover_expired(self, actor: str = "recovery-worker") -> dict[str, Any]:
        now = to_storage(self.clock.now())
        recovered: list[int] = []
        exhausted: list[int] = []
        cancelled: list[int] = []
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            rows = connection.execute("SELECT * FROM compute_tasks WHERE status IN ('running','cancel_requested') AND lease_expires_at<>'' AND lease_expires_at<? ORDER BY id", (now,)).fetchall()
            for task in rows:
                before = dict(task)
                reservation = repository.active_reservation_for_task(task["id"])
                if reservation is not None:
                    repository.release_budget_reservation(reservation, now=now)
                if task["status"] == "cancel_requested":
                    status, finished_at = "cancelled", now
                    cancelled.append(int(task["id"]))
                elif int(task["attempt_count"]) < int(task["max_attempts"]):
                    status, finished_at = "queued", None
                    recovered.append(int(task["id"]))
                else:
                    status, finished_at = "failed", now
                    exhausted.append(int(task["id"]))
                connection.execute(
                    "UPDATE compute_tasks SET status=?,lease_owner='',lease_expires_at='',available_at=?,last_error_code='lease_expired',last_error_message='工作者租约已过期',finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                    (status, now, finished_at, now, task["id"]),
                )
                after = dict(repository.task_by_id(task["id"]))
                repository.add_intervention(task_id=task["id"], actor=actor, action="lease_recovery", reason="租约过期自动恢复", before=before, after=after, batch_key="", now=now)
        return {"recovered": recovered, "exhausted": exhausted, "cancelled": cancelled}

    def summary(self) -> dict[str, Any]:
        rows = self.connection.execute("SELECT status,COUNT(*) AS amount FROM compute_tasks GROUP BY status ORDER BY status").fetchall()
        oldest = self.connection.execute("SELECT MIN(created_at) FROM compute_tasks WHERE status='queued'").fetchone()[0]
        period_start, _ = self._period_bounds(self.clock.now())
        budgets = [
            self._budget_view(dict(account), active_reservations=self.repository.count_active_reservations(account["id"]))
            for account in self.repository.current_budget_accounts(to_storage(period_start))
        ]
        return {"states": {row["status"]: row["amount"] for row in rows}, "oldest_queued_at": oldest, "templates": len(self.repository.active_templates()), "budgets": budgets}

    def set_budget(self, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        moment = self._parse_period(payload.get("period")) or now_value
        period_start, period_end = self._period_bounds(moment)
        start_text = to_storage(period_start)
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            account = repository.budget_account(payload["project_code"], start_text)
            before = self._limits_of(account) if account is not None else {"cpu_seconds": 0, "memory_gb_hours": 0.0, "task_count": 0}
            after = {"cpu_seconds": int(payload["cpu_seconds_limit"]), "memory_gb_hours": float(payload["memory_gb_hours_limit"]), "task_count": int(payload["task_count_limit"])}
            if account is None:
                account = repository.create_budget_account(
                    project_code=payload["project_code"], period_start=start_text, period_end=to_storage(period_end),
                    cpu_seconds_limit=after["cpu_seconds"], memory_gb_hours_limit=after["memory_gb_hours"], task_count_limit=after["task_count"], now=now,
                )
            else:
                repository.update_budget_limits(account["id"], cpu_seconds_limit=after["cpu_seconds"], memory_gb_hours_limit=after["memory_gb_hours"], task_count_limit=after["task_count"], now=now)
            repository.add_budget_adjustment(
                account_id=account["id"], actor=actor, reason=payload["reason"],
                cpu_seconds_delta=after["cpu_seconds"] - before["cpu_seconds"],
                memory_gb_hours_delta=after["memory_gb_hours"] - before["memory_gb_hours"],
                task_count_delta=after["task_count"] - before["task_count"],
                before=before, after=after, now=now,
            )
            return self._budget_view(dict(repository.budget_account_by_id(account["id"])), active_reservations=repository.count_active_reservations(account["id"]))

    def adjust_budget(self, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        moment = self._parse_period(payload.get("period")) or now_value
        period_start, period_end = self._period_bounds(moment)
        start_text = to_storage(period_start)
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            account = self._ensure_account(repository, payload["project_code"], start_text, to_storage(period_end), now)
            before = self._limits_of(account) if account is not None else {"cpu_seconds": 0, "memory_gb_hours": 0.0, "task_count": 0}
            after = {
                "cpu_seconds": before["cpu_seconds"] + int(payload["cpu_seconds_delta"]),
                "memory_gb_hours": before["memory_gb_hours"] + float(payload["memory_gb_hours_delta"]),
                "task_count": before["task_count"] + int(payload["task_count_delta"]),
            }
            if min(after["cpu_seconds"], after["memory_gb_hours"], after["task_count"]) < 0:
                raise ValidationError("调整后的预算额度不能为负")
            if account is None:
                account = repository.create_budget_account(
                    project_code=payload["project_code"], period_start=start_text, period_end=to_storage(period_end),
                    cpu_seconds_limit=after["cpu_seconds"], memory_gb_hours_limit=after["memory_gb_hours"], task_count_limit=after["task_count"], now=now,
                )
            else:
                repository.update_budget_limits(account["id"], cpu_seconds_limit=after["cpu_seconds"], memory_gb_hours_limit=after["memory_gb_hours"], task_count_limit=after["task_count"], now=now)
            repository.add_budget_adjustment(
                account_id=account["id"], actor=actor, reason=payload["reason"],
                cpu_seconds_delta=int(payload["cpu_seconds_delta"]), memory_gb_hours_delta=float(payload["memory_gb_hours_delta"]), task_count_delta=int(payload["task_count_delta"]),
                before=before, after=after, now=now,
            )
            return self._budget_view(dict(repository.budget_account_by_id(account["id"])), active_reservations=repository.count_active_reservations(account["id"]))

    def budget_summary(self, project_code: str, period: str | None = None) -> dict[str, Any]:
        moment = self._parse_period(period) or self.clock.now()
        period_start, period_end = self._period_bounds(moment)
        start_text = to_storage(period_start)
        account = self.repository.budget_account(project_code, start_text)
        if account is not None:
            return self._budget_view(dict(account), active_reservations=self.repository.count_active_reservations(account["id"]))
        previous = self.repository.latest_budget_account_before(project_code, start_text)
        if previous is None:
            return {"project_code": project_code, "period_start": start_text, "period_end": to_storage(period_end), "enforced": False}
        inherited = dict(previous)
        for key in ("cpu_seconds_reserved", "memory_gb_hours_reserved", "task_count_reserved", "cpu_seconds_used", "memory_gb_hours_used", "task_count_used", "rejected_count"):
            inherited[key] = 0
        inherited["period_start"] = start_text
        inherited["period_end"] = to_storage(period_end)
        return self._budget_view(inherited, inherited=True)

    def list_budget_adjustments(self, project_code: str, period: str | None = None) -> list[dict[str, Any]]:
        period_start = None
        if period:
            moment = self._parse_period(period)
            period_start = to_storage(self._period_bounds(moment)[0])
        return self.repository.budget_adjustments(project_code, period_start)

    def _intervene(self, task_id: int, actor: str, reason: str, action: str, batch_key: str, mutation: Callable[[sqlite3.Connection, sqlite3.Row, str], None]) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            before = dict(task)
            mutation(connection, task, now)
            after = dict(repository.task_by_id(task_id))
            repository.add_intervention(task_id=task_id, actor=actor, action=action, reason=reason, before=before, after=after, batch_key=batch_key, now=now)
            return after

    @staticmethod
    def _cancel_mutation(connection: sqlite3.Connection, task: sqlite3.Row, now: str) -> None:
        if task["status"] not in {"queued", "running"}:
            raise ConflictError("当前任务状态不允许取消")
        status = "cancel_requested" if task["status"] == "running" else "cancelled"
        connection.execute("UPDATE compute_tasks SET status=?,finished_at=?,updated_at=?,version=version+1 WHERE id=?", (status, None if status == "cancel_requested" else now, now, task["id"]))

    def _check_quota(self, repository: ComputeRepository, requested_by: str, now: datetime) -> None:
        quota = repository.quota("user", requested_by)
        if quota is None:
            return
        states = repository.count_user_states(requested_by)
        if states.get("queued", 0) >= int(quota["max_queued"]):
            raise ConflictError("用户排队任务配额已用尽")
        if states.get("running", 0) >= int(quota["max_running"]):
            raise ConflictError("用户运行任务配额已用尽")
        day_start = to_storage(now.astimezone(UTC).replace(hour=0, minute=0, second=0, microsecond=0))
        if repository.count_user_submissions_since(requested_by, day_start) >= int(quota["daily_submissions"]):
            raise ConflictError("用户当日提交配额已用尽")

    @staticmethod
    def _period_bounds(moment: datetime) -> tuple[datetime, datetime]:
        """预算周期为 UTC 自然月，返回 [起始, 结束)。"""
        value = moment.astimezone(UTC)
        start = value.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if start.month == 12:
            end = start.replace(year=start.year + 1, month=1)
        else:
            end = start.replace(month=start.month + 1)
        return start, end

    @staticmethod
    def _parse_period(raw: str | None) -> datetime | None:
        if not raw:
            return None
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError:
            raise ValidationError("周期时间格式不正确，请使用 ISO 日期或时间") from None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)

    @staticmethod
    def _limits_of(account: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
        return {
            "cpu_seconds": int(account["cpu_seconds_limit"]),
            "memory_gb_hours": float(account["memory_gb_hours_limit"]),
            "task_count": int(account["task_count_limit"]),
        }

    @staticmethod
    def _available_of(account: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
        return {
            "cpu_seconds": int(account["cpu_seconds_limit"]) - int(account["cpu_seconds_used"]) - int(account["cpu_seconds_reserved"]),
            "memory_gb_hours": float(account["memory_gb_hours_limit"]) - float(account["memory_gb_hours_used"]) - float(account["memory_gb_hours_reserved"]),
            "task_count": int(account["task_count_limit"]) - int(account["task_count_used"]) - int(account["task_count_reserved"]),
        }

    def _ensure_account(self, repository: ComputeRepository, project_code: str, period_start: str, period_end: str, now: str) -> sqlite3.Row | None:
        """返回项目在给定周期的预算账本；缺失时继承最近周期额度自动结转，没有历史账本则返回 None（不强制预算）。"""
        account = repository.budget_account(project_code, period_start)
        if account is not None:
            return account
        previous = repository.latest_budget_account_before(project_code, period_start)
        if previous is None:
            return None
        limits = self._limits_of(previous)
        created = repository.create_budget_account(
            project_code=project_code, period_start=period_start, period_end=period_end,
            cpu_seconds_limit=limits["cpu_seconds"], memory_gb_hours_limit=limits["memory_gb_hours"], task_count_limit=limits["task_count"], now=now,
        )
        repository.add_budget_adjustment(
            account_id=created["id"], actor="system", reason="周期结转：自动继承最近周期额度",
            cpu_seconds_delta=0, memory_gb_hours_delta=0.0, task_count_delta=0,
            before={"cpu_seconds": 0, "memory_gb_hours": 0.0, "task_count": 0}, after=limits, now=now,
        )
        return repository.budget_account(project_code, period_start)

    def _ensure_current_account(self, repository: ComputeRepository, project_code: str, now_value: datetime) -> sqlite3.Row | None:
        period_start, period_end = self._period_bounds(now_value)
        return self._ensure_account(repository, project_code, to_storage(period_start), to_storage(period_end), to_storage(now_value))

    def _budget_precheck(self, repository: ComputeRepository, project_code: str, template: sqlite3.Row, now_value: datetime) -> dict[str, Any]:
        estimate = {"cpu_seconds": int(template["est_cpu_seconds"]), "memory_gb_hours": float(template["est_memory_gb_hours"]), "task_count": 1}
        account = self._ensure_current_account(repository, project_code, now_value)
        if account is None:
            return {"enforced": False, "estimate": estimate}
        available = self._available_of(account)
        shortages = [name for name in ("cpu_seconds", "memory_gb_hours", "task_count") if available[name] < estimate[name]]
        return {
            "enforced": True,
            "period_start": account["period_start"],
            "period_end": account["period_end"],
            "estimate": estimate,
            "available": available,
            "fits": not shortages,
            "shortages": shortages,
        }

    def _record_budget_rejection(self, repository: ComputeRepository, account: sqlite3.Row, candidate: sqlite3.Row, estimate: dict[str, Any], now: str) -> None:
        available = self._available_of(account)
        shortages = [name for name in ("cpu_seconds", "memory_gb_hours", "task_count") if available[name] < estimate[name]]
        if candidate["queue_reason_code"] != "budget_exceeded":
            repository.increment_budget_rejected(account["id"], now)
        repository.set_task_queue_reason(candidate["id"], "budget_exceeded", "项目预算不足：" + "、".join(shortages), now)

    def _settle_budget(self, repository: ComputeRepository, task_id: int, metrics: dict[str, Any], now: str) -> None:
        reservation = repository.active_reservation_for_task(task_id)
        if reservation is None:
            return
        actual_cpu, actual_memory = self._actual_usage(metrics, reservation)
        repository.settle_budget_reservation(reservation, actual_cpu_seconds=actual_cpu, actual_memory_gb_hours=actual_memory, now=now)

    @staticmethod
    def _actual_usage(metrics: dict[str, Any], reservation: sqlite3.Row) -> tuple[int, float]:
        """按实际指标结算；指标缺失或非法时保守地按预留估算值记账。"""
        cpu = metrics.get("cpu_seconds")
        if not isinstance(cpu, (int, float)) or isinstance(cpu, bool) or cpu < 0:
            cpu = reservation["cpu_seconds"]
        memory = metrics.get("memory_gb_hours")
        if not isinstance(memory, (int, float)) or isinstance(memory, bool) or memory < 0:
            memory = reservation["memory_gb_hours"]
        return int(round(cpu)), float(memory)

    def _budget_view(self, account: dict[str, Any], *, inherited: bool = False, active_reservations: int = 0) -> dict[str, Any]:
        return {
            "project_code": account["project_code"],
            "period_start": account["period_start"],
            "period_end": account["period_end"],
            "enforced": True,
            "inherited": inherited,
            "limits": self._limits_of(account),
            "reserved": {
                "cpu_seconds": int(account["cpu_seconds_reserved"]),
                "memory_gb_hours": float(account["memory_gb_hours_reserved"]),
                "task_count": int(account["task_count_reserved"]),
            },
            "used": {
                "cpu_seconds": int(account["cpu_seconds_used"]),
                "memory_gb_hours": float(account["memory_gb_hours_used"]),
                "task_count": int(account["task_count_used"]),
            },
            "available": self._available_of(account),
            "rejected_count": int(account["rejected_count"]),
            "active_reservations": active_reservations,
        }

    @staticmethod
    def _validate_schema(schema: dict[str, dict[str, Any]], defaults: dict[str, Any]) -> None:
        if not schema:
            raise ValidationError("参数模板至少包含一个参数")
        allowed = {"integer", "number", "string", "boolean"}
        for name, rule in schema.items():
            if not name or not isinstance(rule, dict) or rule.get("type") not in allowed:
                raise ValidationError(f"参数 {name or '<empty>'} 的规则不合法")
        if set(defaults) - set(schema):
            raise ValidationError("默认值包含未声明参数")

    def _validate_parameters(self, template: sqlite3.Row, supplied: dict[str, Any]) -> dict[str, Any]:
        schema = json.loads(template["parameter_schema_json"])
        values = {**json.loads(template["default_parameters_json"]), **supplied}
        unknown = set(values) - set(schema)
        if unknown:
            raise ValidationError("包含模板未声明的参数", context={"parameters": sorted(unknown)})
        normalized: dict[str, Any] = {}
        for name, rule in schema.items():
            if name not in values:
                if rule.get("required"):
                    raise ValidationError(f"缺少必填参数：{name}")
                continue
            value = values[name]
            kind = rule["type"]
            valid = {"integer": isinstance(value, int) and not isinstance(value, bool), "number": isinstance(value, (int, float)) and not isinstance(value, bool), "string": isinstance(value, str), "boolean": isinstance(value, bool)}[kind]
            if not valid:
                raise ValidationError(f"参数 {name} 类型不正确")
            if rule.get("minimum") is not None and value < rule["minimum"]:
                raise ValidationError(f"参数 {name} 小于允许的最小值")
            if rule.get("maximum") is not None and value > rule["maximum"]:
                raise ValidationError(f"参数 {name} 大于允许的最大值")
            if rule.get("choices") and value not in rule["choices"]:
                raise ValidationError(f"参数 {name} 不在允许的选项中")
            normalized[name] = value
        return normalized
