from __future__ import annotations

import json
import sqlite3
from typing import Any, Iterable


class ComputeRepository:
    """封装计算任务运营领域的 SQLite 读写。"""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self.connection = connection

    def template_by_code(self, code: str) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_templates WHERE code=?", (code,)).fetchone()

    def template_by_id(self, template_id: int) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_templates WHERE id=?", (template_id,)).fetchone()

    def active_templates(self) -> list[dict[str, Any]]:
        rows = self.connection.execute("SELECT * FROM compute_templates WHERE active=1 ORDER BY code,version").fetchall()
        return [dict(row) for row in rows]

    def create_template(self, *, code: str, name: str, algorithm: str, parameter_schema: dict[str, Any], defaults: dict[str, Any], max_runtime_seconds: int, max_attempts: int, estimated_cpu_seconds: int, estimated_memory_hours: float, created_by: str, now: str) -> dict[str, Any]:
        cursor = self.connection.execute(
            "INSERT INTO compute_templates(code,name,algorithm,version,parameter_schema_json,default_parameters_json,max_runtime_seconds,max_attempts,estimated_cpu_seconds,estimated_memory_hours,active,created_by,created_at,updated_at) VALUES(?,?,?,1,?,?,?,?,?,?,1,?,?,?)",
            (code, name, algorithm, json.dumps(parameter_schema, ensure_ascii=False, sort_keys=True), json.dumps(defaults, ensure_ascii=False, sort_keys=True), max_runtime_seconds, max_attempts, estimated_cpu_seconds, estimated_memory_hours, created_by, now, now),
        )
        return dict(self.template_by_id(cursor.lastrowid))

    def quota(self, subject_type: str, subject_key: str) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_quotas WHERE subject_type=? AND subject_key=?", (subject_type, subject_key)).fetchone()

    def upsert_quota(self, *, subject_type: str, subject_key: str, max_queued: int, max_running: int, daily_submissions: int, actor: str, now: str) -> dict[str, Any]:
        self.connection.execute(
            "INSERT INTO compute_quotas(subject_type,subject_key,max_queued,max_running,daily_submissions,updated_by,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(subject_type,subject_key) DO UPDATE SET max_queued=excluded.max_queued,max_running=excluded.max_running,daily_submissions=excluded.daily_submissions,updated_by=excluded.updated_by,updated_at=excluded.updated_at",
            (subject_type, subject_key, max_queued, max_running, daily_submissions, actor, now, now),
        )
        return dict(self.quota(subject_type, subject_key))

    def count_user_states(self, requested_by: str) -> dict[str, int]:
        rows = self.connection.execute("SELECT status,COUNT(*) AS amount FROM compute_tasks WHERE requested_by=? GROUP BY status", (requested_by,)).fetchall()
        return {str(row["status"]): int(row["amount"]) for row in rows}

    def count_user_submissions_since(self, requested_by: str, since: str) -> int:
        return int(self.connection.execute("SELECT COUNT(*) FROM compute_tasks WHERE requested_by=? AND created_at>=?", (requested_by, since)).fetchone()[0])

    def task_by_id(self, task_id: int) -> sqlite3.Row | None:
        return self.connection.execute("SELECT t.*,tpl.code AS template_code,tpl.algorithm AS template_algorithm FROM compute_tasks t JOIN compute_templates tpl ON tpl.id=t.template_id WHERE t.id=?", (task_id,)).fetchone()

    def task_by_idempotency(self, requested_by: str, key: str) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_tasks WHERE requested_by=? AND idempotency_key=?", (requested_by, key)).fetchone()

    def create_task(self, *, template_id: int, project_code: str, requested_by: str, parameters: dict[str, Any], parameter_digest: str, priority: int, idempotency_key: str, max_attempts: int, estimated_cpu_seconds: int, estimated_memory_hours: float, now: str) -> dict[str, Any]:
        cursor = self.connection.execute(
            "INSERT INTO compute_tasks(template_id,project_code,requested_by,parameters_json,parameter_digest,priority,idempotency_key,status,attempt_count,max_attempts,available_at,estimated_cpu_seconds,estimated_memory_hours,created_at,updated_at) VALUES(?,?,?,?,?,?,?,'queued',0,?,?,?,?,?,?)",
            (template_id, project_code, requested_by, json.dumps(parameters, ensure_ascii=False, sort_keys=True), parameter_digest, priority, idempotency_key, max_attempts, now, estimated_cpu_seconds, estimated_memory_hours, now, now),
        )
        return dict(self.task_by_id(cursor.lastrowid))

    def queued_candidates(self, capabilities: Iterable[str], now: str, limit: int = 25) -> list[sqlite3.Row]:
        capability_list = sorted(set(capabilities))
        params: list[Any] = [now]
        condition = ""
        if capability_list:
            placeholders = ",".join("?" for _ in capability_list)
            condition = f" AND tpl.algorithm IN ({placeholders})"
            params.extend(capability_list)
        params.append(limit)
        return self.connection.execute(
            "SELECT t.*,tpl.algorithm AS template_algorithm FROM compute_tasks t JOIN compute_templates tpl ON tpl.id=t.template_id WHERE t.status='queued' AND t.available_at<=?" + condition + " ORDER BY t.priority DESC,t.created_at ASC,t.id ASC LIMIT ?",
            params,
        ).fetchall()

    def result_versions(self, task_id: int) -> list[dict[str, Any]]:
        return [dict(row) for row in self.connection.execute("SELECT * FROM compute_results WHERE task_id=? ORDER BY version", (task_id,)).fetchall()]

    def interventions(self, task_id: int) -> list[dict[str, Any]]:
        return [dict(row) for row in self.connection.execute("SELECT * FROM compute_interventions WHERE task_id=? ORDER BY id", (task_id,)).fetchall()]

    def add_intervention(self, *, task_id: int, actor: str, action: str, reason: str, before: dict[str, Any], after: dict[str, Any], batch_key: str, now: str) -> None:
        self.connection.execute(
            "INSERT INTO compute_interventions(task_id,actor,action,reason,before_json,after_json,batch_key,created_at) VALUES(?,?,?,?,?,?,?,?)",
            (task_id, actor, action, reason, json.dumps(before, ensure_ascii=False, sort_keys=True), json.dumps(after, ensure_ascii=False, sort_keys=True), batch_key, now),
        )

    def list_tasks(self, *, status: str | None, project_code: str | None, requested_by: str | None, limit: int) -> list[dict[str, Any]]:
        clauses: list[str] = []
        values: list[Any] = []
        if status:
            clauses.append("t.status=?")
            values.append(status)
        if project_code:
            clauses.append("t.project_code=?")
            values.append(project_code)
        if requested_by:
            clauses.append("t.requested_by=?")
            values.append(requested_by)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        values.append(limit)
        rows = self.connection.execute(
            "SELECT t.*,tpl.code AS template_code,tpl.algorithm AS template_algorithm FROM compute_tasks t JOIN compute_templates tpl ON tpl.id=t.template_id" + where + " ORDER BY t.priority DESC,t.created_at DESC,t.id DESC LIMIT ?",
            values,
        ).fetchall()
        return [dict(row) for row in rows]

    def budget(self, project_code: str, period: str) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_budgets WHERE project_code=? AND period=?", (project_code, period)).fetchone()

    def ensure_budget(self, project_code: str, period: str, now: str) -> None:
        self.connection.execute(
            "INSERT OR IGNORE INTO compute_budgets(project_code,period,created_at,updated_at) VALUES(?,?,?,?)",
            (project_code, period, now, now),
        )

    def adjust_budget(self, *, project_code: str, period: str, cpu_seconds: int, memory_hours: float, tasks: int, now: str) -> bool:
        """按增量调整授予额度，任何维度会变为负数时拒绝。"""
        cursor = self.connection.execute(
            "UPDATE compute_budgets SET granted_cpu_seconds=granted_cpu_seconds+?,granted_memory_hours=granted_memory_hours+?,granted_tasks=granted_tasks+?,updated_at=? WHERE project_code=? AND period=? AND granted_cpu_seconds+?>=0 AND granted_memory_hours+?>=0 AND granted_tasks+?>=0",
            (cpu_seconds, memory_hours, tasks, now, project_code, period, cpu_seconds, memory_hours, tasks),
        )
        return cursor.rowcount == 1

    def try_reserve(self, *, project_code: str, period: str, cpu_seconds: int, memory_hours: float, now: str) -> bool:
        """原子预留预算：仅当可用额度足够时才扣减，返回是否成功。"""
        cursor = self.connection.execute(
            "UPDATE compute_budgets SET reserved_cpu_seconds=reserved_cpu_seconds+?,reserved_memory_hours=reserved_memory_hours+?,reserved_tasks=reserved_tasks+1,updated_at=? WHERE project_code=? AND period=? AND reserved_cpu_seconds+used_cpu_seconds+?<=granted_cpu_seconds AND reserved_memory_hours+used_memory_hours+?<=granted_memory_hours AND reserved_tasks+used_tasks+1<=granted_tasks",
            (cpu_seconds, memory_hours, now, project_code, period, cpu_seconds, memory_hours),
        )
        return cursor.rowcount == 1

    def release_reservation(self, *, project_code: str, period: str, cpu_seconds: int, memory_hours: float, now: str) -> None:
        self.connection.execute(
            "UPDATE compute_budgets SET reserved_cpu_seconds=reserved_cpu_seconds-?,reserved_memory_hours=reserved_memory_hours-?,reserved_tasks=reserved_tasks-1,updated_at=? WHERE project_code=? AND period=?",
            (cpu_seconds, memory_hours, now, project_code, period),
        )

    def settle_usage(self, *, project_code: str, period: str, cpu_seconds: int, memory_hours: float, now: str) -> None:
        self.connection.execute(
            "UPDATE compute_budgets SET used_cpu_seconds=used_cpu_seconds+?,used_memory_hours=used_memory_hours+?,used_tasks=used_tasks+1,updated_at=? WHERE project_code=? AND period=?",
            (cpu_seconds, memory_hours, now, project_code, period),
        )

    def increment_rejected(self, *, project_code: str, period: str, now: str) -> None:
        self.connection.execute(
            "UPDATE compute_budgets SET rejected_tasks=rejected_tasks+1,updated_at=? WHERE project_code=? AND period=?",
            (now, project_code, period),
        )

    def add_budget_event(self, *, project_code: str, period: str, task_id: int | None, event_type: str, cpu_seconds: int, memory_hours: float, tasks: int, reason: str, actor: str, now: str) -> None:
        self.connection.execute(
            "INSERT INTO compute_budget_events(project_code,period,task_id,event_type,cpu_seconds,memory_hours,tasks,reason,actor,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
            (project_code, period, task_id, event_type, cpu_seconds, memory_hours, tasks, reason, actor, now),
        )

    def budget_events(self, *, project_code: str, period: str, limit: int) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT * FROM compute_budget_events WHERE project_code=? AND period=? ORDER BY id LIMIT ?",
            (project_code, period, limit),
        ).fetchall()
        return [dict(row) for row in rows]
