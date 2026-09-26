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


def budget_period(value: datetime) -> str:
    """预算周期按 UTC 自然月划分，例如 2026-09。"""
    return value.astimezone(UTC).strftime("%Y-%m")


class ComputeOperationsService:
    """管理计算模板、配额、任务租约、结果版本、项目预算和人工干预。"""

    def __init__(self, connection: sqlite3.Connection | None = None, clock: Clock | None = None) -> None:
        self.connection = connection or get_connection()
        self.clock = clock or SystemClock()
        self.repository = ComputeRepository(self.connection)

    def list_templates(self) -> list[dict[str, Any]]:
        return self.repository.active_templates()

    def create_template(self, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        self._validate_schema(payload["parameter_schema"], payload["default_parameters"])
        now = to_storage(self.clock.now())
        # 未显式给出 CPU 秒估算值时按模板最大运行时长保守估计
        estimated_cpu = payload.get("estimated_cpu_seconds")
        if estimated_cpu is None:
            estimated_cpu = payload["max_runtime_seconds"]
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            if repository.template_by_code(payload["code"]):
                raise ConflictError("参数模板编码已存在")
            return repository.create_template(
                code=payload["code"], name=payload["name"], algorithm=payload["algorithm"],
                parameter_schema=payload["parameter_schema"], defaults=payload["default_parameters"],
                max_runtime_seconds=payload["max_runtime_seconds"], max_attempts=payload["max_attempts"],
                estimated_cpu_seconds=estimated_cpu, estimated_memory_hours=payload.get("estimated_memory_hours", 0.0),
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
            existing = repository.task_by_idempotency(payload["requested_by"], payload["idempotency_key"])
            parameter_digest = digest(parameters)
            if existing is not None:
                if existing["parameter_digest"] != parameter_digest:
                    raise ConflictError("同一幂等键对应了不同的计算参数")
                task = dict(repository.task_by_id(existing["id"]))
            else:
                self._check_quota(repository, payload["requested_by"], now_value)
                task = repository.create_task(
                    template_id=template["id"], project_code=payload["project_code"],
                    requested_by=payload["requested_by"], parameters=parameters,
                    parameter_digest=parameter_digest, priority=payload["priority"],
                    idempotency_key=payload["idempotency_key"], max_attempts=template["max_attempts"],
                    estimated_cpu_seconds=int(template["estimated_cpu_seconds"]),
                    estimated_memory_hours=float(template["estimated_memory_hours"]), now=now,
                )
            task["budget_precheck"] = self._budget_precheck(repository, task, now_value)
            return task

    def adjust_budget(self, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        period = payload["period"] or budget_period(now_value)
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            repository.ensure_budget(payload["project_code"], period, now)
            adjusted = repository.adjust_budget(
                project_code=payload["project_code"], period=period,
                cpu_seconds=payload["cpu_seconds"], memory_hours=payload["memory_hours"],
                tasks=payload["tasks"], now=now,
            )
            if not adjusted:
                raise ValidationError("调整会使项目当期额度变为负数")
            repository.add_budget_event(
                project_code=payload["project_code"], period=period, task_id=None, event_type="grant",
                cpu_seconds=payload["cpu_seconds"], memory_hours=payload["memory_hours"], tasks=payload["tasks"],
                reason=payload["reason"], actor=actor, now=now,
            )
        return self.budget_summary(payload["project_code"], period)

    def budget_summary(self, project_code: str, period: str | None = None) -> dict[str, Any]:
        period = period or budget_period(self.clock.now())
        return self._summary_for(self.repository, project_code, period)

    @staticmethod
    def _summary_for(repository: ComputeRepository, project_code: str, period: str) -> dict[str, Any]:
        row = repository.budget(project_code, period)
        if row is None:
            return {
                "project_code": project_code, "period": period, "tracked": False,
                "granted": None, "reserved": None, "used": None, "available": None,
                "rejected_tasks": 0,
            }
        granted = {"cpu_seconds": int(row["granted_cpu_seconds"]), "memory_hours": float(row["granted_memory_hours"]), "tasks": int(row["granted_tasks"])}
        reserved = {"cpu_seconds": int(row["reserved_cpu_seconds"]), "memory_hours": float(row["reserved_memory_hours"]), "tasks": int(row["reserved_tasks"])}
        used = {"cpu_seconds": int(row["used_cpu_seconds"]), "memory_hours": float(row["used_memory_hours"]), "tasks": int(row["used_tasks"])}
        available = {key: granted[key] - reserved[key] - used[key] for key in granted}
        return {
            "project_code": project_code, "period": period, "tracked": True,
            "granted": granted, "reserved": reserved, "used": used, "available": available,
            "rejected_tasks": int(row["rejected_tasks"]),
        }

    def budget_events(self, project_code: str, period: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        period = period or budget_period(self.clock.now())
        return self.repository.budget_events(project_code=project_code, period=period, limit=max(1, min(limit, 500)))

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
        period = budget_period(now_value)
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            for candidate in repository.queued_candidates(capabilities, now):
                budget = repository.budget(candidate["project_code"], period)
                reserved = False
                if budget is not None:
                    # 项目配置了当期预算：先原子预留，额度不足则保留排队原因并跳过
                    reserved = repository.try_reserve(
                        project_code=candidate["project_code"], period=period,
                        cpu_seconds=int(candidate["estimated_cpu_seconds"]),
                        memory_hours=float(candidate["estimated_memory_hours"]), now=now,
                    )
                    if not reserved:
                        self._mark_budget_denied(connection, repository, candidate, period, now)
                        continue
                    repository.add_budget_event(
                        project_code=candidate["project_code"], period=period, task_id=candidate["id"], event_type="reserve",
                        cpu_seconds=int(candidate["estimated_cpu_seconds"]), memory_hours=float(candidate["estimated_memory_hours"]),
                        tasks=1, reason="领取任务按模板估算值预留预算", actor=worker_id, now=now,
                    )
                cursor = connection.execute(
                    "UPDATE compute_tasks SET status='running',attempt_count=attempt_count+1,lease_owner=?,lease_expires_at=?,started_at=COALESCE(started_at,?),updated_at=?,version=version+1 WHERE id=? AND status='queued'",
                    (worker_id, lease_until, now, now, candidate["id"]),
                )
                if cursor.rowcount != 1:
                    if reserved:
                        self._release_reservation(repository, candidate["project_code"], period, candidate["id"], int(candidate["estimated_cpu_seconds"]), float(candidate["estimated_memory_hours"]), "领取竞争失败，回滚预算预留", "system", now)
                    continue
                if reserved:
                    connection.execute(
                        "UPDATE compute_tasks SET budget_period=?,budget_reserved=1,reserved_cpu_seconds=?,reserved_memory_hours=?,budget_denied_period='' WHERE id=?",
                        (period, int(candidate["estimated_cpu_seconds"]), float(candidate["estimated_memory_hours"]), candidate["id"]),
                    )
                return dict(repository.task_by_id(candidate["id"]))
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
            self._settle_budget(connection, repository, task, metrics, worker_id, now)
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
            self._settle_budget(connection, repository, task, metrics or {}, worker_id, now)
            return dict(repository.task_by_id(task_id))

    def cancel(self, task_id: int, actor: str, reason: str, batch_key: str = "") -> dict[str, Any]:
        return self._intervene(task_id, actor, reason, "cancel", batch_key, self._cancel_mutation)

    def retry(self, task_id: int, actor: str, reason: str, priority: int | None = None, batch_key: str = "") -> dict[str, Any]:
        def mutate(connection: sqlite3.Connection, task: sqlite3.Row, now: str) -> None:
            if task["status"] not in {"failed", "cancelled"}:
                raise ConflictError("只有失败或已取消任务可以人工重试")
            chosen = task["priority"] if priority is None else priority
            connection.execute("UPDATE compute_tasks SET status='queued',priority=?,available_at=?,lease_owner='',lease_expires_at='',finished_at=NULL,updated_at=?,version=version+1 WHERE id=?", (chosen, now, now, task["id"]))
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
                if task["budget_reserved"]:
                    # 失联工作者不会回执，预留全额释放、不计已用，避免预算泄漏
                    self._release_reservation(repository, task["project_code"], task["budget_period"], task["id"], int(task["reserved_cpu_seconds"]), float(task["reserved_memory_hours"]), "租约过期自动恢复，释放预留预算", actor, now)
                    connection.execute(
                        "UPDATE compute_tasks SET budget_reserved=0,reserved_cpu_seconds=0,reserved_memory_hours=0 WHERE id=?",
                        (task["id"],),
                    )
                after = dict(repository.task_by_id(task["id"]))
                repository.add_intervention(task_id=task["id"], actor=actor, action="lease_recovery", reason="租约过期自动恢复", before=before, after=after, batch_key="", now=now)
        return {"recovered": recovered, "exhausted": exhausted, "cancelled": cancelled}

    def summary(self) -> dict[str, Any]:
        rows = self.connection.execute("SELECT status,COUNT(*) AS amount FROM compute_tasks GROUP BY status ORDER BY status").fetchall()
        oldest = self.connection.execute("SELECT MIN(created_at) FROM compute_tasks WHERE status='queued'").fetchone()[0]
        return {"states": {row["status"]: row["amount"] for row in rows}, "oldest_queued_at": oldest, "templates": len(self.repository.active_templates())}

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

    def _budget_precheck(self, repository: ComputeRepository, task: dict[str, Any], now: datetime) -> dict[str, Any]:
        """提交时的可见预检查：只报告估算值与当期可用额度，不拦截提交。"""
        period = budget_period(now)
        required = {
            "cpu_seconds": int(task["estimated_cpu_seconds"]),
            "memory_hours": float(task["estimated_memory_hours"]),
            "tasks": 1,
        }
        summary = self._summary_for(repository, task["project_code"], period)
        precheck: dict[str, Any] = {"period": period, "required": required, "tracked": summary["tracked"]}
        if summary["tracked"]:
            available = summary["available"]
            precheck["available"] = available
            precheck["fits"] = all(required[key] <= available[key] for key in required)
        else:
            precheck["available"] = None
            precheck["fits"] = None
        return precheck

    def _mark_budget_denied(self, connection: sqlite3.Connection, repository: ComputeRepository, task: sqlite3.Row, period: str, now: str) -> None:
        """超预算任务保持排队并记录原因；同一周期内同一任务只记一次拒绝。"""
        if task["last_error_code"] == "budget_exceeded" and task["budget_denied_period"] == period:
            return
        message = f"项目 {task['project_code']} 在 {period} 周期的预算不足以预留该任务"
        connection.execute(
            "UPDATE compute_tasks SET last_error_code='budget_exceeded',last_error_message=?,budget_denied_period=?,updated_at=?,version=version+1 WHERE id=?",
            (message, period, now, task["id"]),
        )
        repository.increment_rejected(project_code=task["project_code"], period=period, now=now)
        repository.add_budget_event(
            project_code=task["project_code"], period=period, task_id=task["id"], event_type="deny",
            cpu_seconds=int(task["estimated_cpu_seconds"]), memory_hours=float(task["estimated_memory_hours"]),
            tasks=1, reason="项目当期预算不足，任务保持排队", actor="system", now=now,
        )

    def _release_reservation(self, repository: ComputeRepository, project_code: str, period: str, task_id: int, cpu_seconds: int, memory_hours: float, reason: str, actor: str, now: str) -> None:
        repository.release_reservation(project_code=project_code, period=period, cpu_seconds=cpu_seconds, memory_hours=memory_hours, now=now)
        repository.add_budget_event(
            project_code=project_code, period=period, task_id=task_id, event_type="release",
            cpu_seconds=cpu_seconds, memory_hours=memory_hours, tasks=1, reason=reason, actor=actor, now=now,
        )

    def _settle_budget(self, connection: sqlite3.Connection, repository: ComputeRepository, task: sqlite3.Row, metrics: dict[str, Any], worker_id: str, now: str) -> None:
        """回执结算：预留全额退回，按实际指标计入已用，预留周期不变避免跨周期串账。"""
        if not task["budget_reserved"]:
            return
        reserved_cpu = int(task["reserved_cpu_seconds"])
        reserved_memory = float(task["reserved_memory_hours"])
        actual_cpu, actual_memory = self._usage_from_metrics(metrics, reserved_cpu, reserved_memory)
        project_code, period = task["project_code"], task["budget_period"]
        self._release_reservation(repository, project_code, period, task["id"], reserved_cpu, reserved_memory, "回执结算，释放预留额度", worker_id, now)
        repository.settle_usage(project_code=project_code, period=period, cpu_seconds=actual_cpu, memory_hours=actual_memory, now=now)
        repository.add_budget_event(
            project_code=project_code, period=period, task_id=task["id"], event_type="settle",
            cpu_seconds=actual_cpu, memory_hours=actual_memory, tasks=1,
            reason="按回执实际指标结算", actor=worker_id, now=now,
        )
        connection.execute(
            "UPDATE compute_tasks SET budget_reserved=0,reserved_cpu_seconds=0,reserved_memory_hours=0 WHERE id=?",
            (task["id"],),
        )

    @staticmethod
    def _usage_from_metrics(metrics: dict[str, Any], reserved_cpu: int, reserved_memory: float) -> tuple[int, float]:
        """从回执指标提取实际用量；缺项按预留量结算，避免失联或漏报造成预算泄漏。"""
        cpu = metrics.get("cpu_seconds", reserved_cpu)
        if isinstance(cpu, bool) or not isinstance(cpu, (int, float)) or cpu < 0 or int(cpu) != cpu:
            raise ValidationError("指标 cpu_seconds 必须是非负整数")
        memory = metrics.get("memory_hours", reserved_memory)
        if isinstance(memory, bool) or not isinstance(memory, (int, float)) or memory < 0:
            raise ValidationError("指标 memory_hours 必须是非负数值")
        return int(cpu), float(memory)

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
