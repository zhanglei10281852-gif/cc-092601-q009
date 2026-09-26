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

    def create_template(self, *, code: str, name: str, algorithm: str, parameter_schema: dict[str, Any], defaults: dict[str, Any], max_runtime_seconds: int, max_attempts: int, est_cpu_seconds: int, est_memory_gb_hours: float, created_by: str, now: str) -> dict[str, Any]:
        cursor = self.connection.execute(
            "INSERT INTO compute_templates(code,name,algorithm,version,parameter_schema_json,default_parameters_json,max_runtime_seconds,max_attempts,est_cpu_seconds,est_memory_gb_hours,active,created_by,created_at,updated_at) VALUES(?,?,?,1,?,?,?,?,?,?,1,?,?,?)",
            (code, name, algorithm, json.dumps(parameter_schema, ensure_ascii=False, sort_keys=True), json.dumps(defaults, ensure_ascii=False, sort_keys=True), max_runtime_seconds, max_attempts, est_cpu_seconds, est_memory_gb_hours, created_by, now, now),
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

    def create_task(self, *, template_id: int, project_code: str, requested_by: str, parameters: dict[str, Any], parameter_digest: str, priority: int, idempotency_key: str, max_attempts: int, now: str) -> dict[str, Any]:
        cursor = self.connection.execute(
            "INSERT INTO compute_tasks(template_id,project_code,requested_by,parameters_json,parameter_digest,priority,idempotency_key,status,attempt_count,max_attempts,available_at,created_at,updated_at) VALUES(?,?,?,?,?,?,?,'queued',0,?,?,?,?)",
            (template_id, project_code, requested_by, json.dumps(parameters, ensure_ascii=False, sort_keys=True), parameter_digest, priority, idempotency_key, max_attempts, now, now, now),
        )
        return dict(self.task_by_id(cursor.lastrowid))

    def queued_candidates(self, capabilities: Iterable[str], now: str, limit: int = 20) -> list[sqlite3.Row]:
        capability_list = sorted(set(capabilities))
        params: list[Any] = [now]
        condition = ""
        if capability_list:
            placeholders = ",".join("?" for _ in capability_list)
            condition = f" AND tpl.algorithm IN ({placeholders})"
            params.extend(capability_list)
        params.append(limit)
        return self.connection.execute(
            "SELECT t.*,tpl.algorithm AS template_algorithm,tpl.est_cpu_seconds AS est_cpu_seconds,tpl.est_memory_gb_hours AS est_memory_gb_hours FROM compute_tasks t JOIN compute_templates tpl ON tpl.id=t.template_id WHERE t.status='queued' AND t.available_at<=?" + condition + " ORDER BY t.priority DESC,t.created_at ASC,t.id ASC LIMIT ?",
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

    def budget_account(self, project_code: str, period_start: str) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_budget_accounts WHERE project_code=? AND period_start=?", (project_code, period_start)).fetchone()

    def budget_account_by_id(self, account_id: int) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_budget_accounts WHERE id=?", (account_id,)).fetchone()

    def latest_budget_account_before(self, project_code: str, period_start: str) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_budget_accounts WHERE project_code=? AND period_start<? ORDER BY period_start DESC LIMIT 1", (project_code, period_start)).fetchone()

    def current_budget_accounts(self, period_start: str) -> list[sqlite3.Row]:
        return self.connection.execute("SELECT * FROM compute_budget_accounts WHERE period_start=? ORDER BY project_code", (period_start,)).fetchall()

    def create_budget_account(self, *, project_code: str, period_start: str, period_end: str, cpu_seconds_limit: int, memory_gb_hours_limit: float, task_count_limit: int, now: str) -> dict[str, Any]:
        cursor = self.connection.execute(
            "INSERT INTO compute_budget_accounts(project_code,period_start,period_end,cpu_seconds_limit,memory_gb_hours_limit,task_count_limit,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)",
            (project_code, period_start, period_end, cpu_seconds_limit, memory_gb_hours_limit, task_count_limit, now, now),
        )
        return dict(self.budget_account_by_id(cursor.lastrowid))

    def update_budget_limits(self, account_id: int, *, cpu_seconds_limit: int, memory_gb_hours_limit: float, task_count_limit: int, now: str) -> None:
        self.connection.execute(
            "UPDATE compute_budget_accounts SET cpu_seconds_limit=?,memory_gb_hours_limit=?,task_count_limit=?,updated_at=? WHERE id=?",
            (cpu_seconds_limit, memory_gb_hours_limit, task_count_limit, now, account_id),
        )

    def try_reserve_budget(self, account_id: int, *, cpu_seconds: int, memory_gb_hours: float, task_count: int, now: str) -> bool:
        cursor = self.connection.execute(
            "UPDATE compute_budget_accounts SET cpu_seconds_reserved=cpu_seconds_reserved+?,memory_gb_hours_reserved=memory_gb_hours_reserved+?,task_count_reserved=task_count_reserved+?,updated_at=? WHERE id=? AND cpu_seconds_limit-cpu_seconds_used-cpu_seconds_reserved>=? AND memory_gb_hours_limit-memory_gb_hours_used-memory_gb_hours_reserved>=? AND task_count_limit-task_count_used-task_count_reserved>=?",
            (cpu_seconds, memory_gb_hours, task_count, now, account_id, cpu_seconds, memory_gb_hours, task_count),
        )
        return cursor.rowcount == 1

    def create_budget_reservation(self, *, account_id: int, task_id: int, attempt: int, cpu_seconds: int, memory_gb_hours: float, task_count: int, now: str) -> None:
        self.connection.execute(
            "INSERT INTO compute_budget_reservations(account_id,task_id,attempt,cpu_seconds,memory_gb_hours,task_count,status,created_at) VALUES(?,?,?,?,?,?,'active',?)",
            (account_id, task_id, attempt, cpu_seconds, memory_gb_hours, task_count, now),
        )

    def active_reservation_for_task(self, task_id: int) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_budget_reservations WHERE task_id=? AND status='active' ORDER BY id DESC LIMIT 1", (task_id,)).fetchone()

    def count_active_reservations(self, account_id: int) -> int:
        return int(self.connection.execute("SELECT COUNT(*) FROM compute_budget_reservations WHERE account_id=? AND status='active'", (account_id,)).fetchone()[0])

    def settle_budget_reservation(self, reservation: sqlite3.Row, *, actual_cpu_seconds: int, actual_memory_gb_hours: float, now: str) -> None:
        cursor = self.connection.execute("UPDATE compute_budget_reservations SET status='settled',closed_at=? WHERE id=? AND status='active'", (now, reservation["id"]))
        if cursor.rowcount != 1:
            return
        self.connection.execute(
            "UPDATE compute_budget_accounts SET cpu_seconds_reserved=MAX(0,cpu_seconds_reserved-?),memory_gb_hours_reserved=MAX(0.0,memory_gb_hours_reserved-?),task_count_reserved=MAX(0,task_count_reserved-?),cpu_seconds_used=cpu_seconds_used+?,memory_gb_hours_used=memory_gb_hours_used+?,task_count_used=task_count_used+?,updated_at=? WHERE id=?",
            (reservation["cpu_seconds"], reservation["memory_gb_hours"], reservation["task_count"], actual_cpu_seconds, actual_memory_gb_hours, reservation["task_count"], now, reservation["account_id"]),
        )

    def release_budget_reservation(self, reservation: sqlite3.Row, *, now: str) -> None:
        cursor = self.connection.execute("UPDATE compute_budget_reservations SET status='released',closed_at=? WHERE id=? AND status='active'", (now, reservation["id"]))
        if cursor.rowcount != 1:
            return
        self.connection.execute(
            "UPDATE compute_budget_accounts SET cpu_seconds_reserved=MAX(0,cpu_seconds_reserved-?),memory_gb_hours_reserved=MAX(0.0,memory_gb_hours_reserved-?),task_count_reserved=MAX(0,task_count_reserved-?),updated_at=? WHERE id=?",
            (reservation["cpu_seconds"], reservation["memory_gb_hours"], reservation["task_count"], now, reservation["account_id"]),
        )

    def increment_budget_rejected(self, account_id: int, now: str) -> None:
        self.connection.execute("UPDATE compute_budget_accounts SET rejected_count=rejected_count+1,updated_at=? WHERE id=?", (now, account_id))

    def set_task_queue_reason(self, task_id: int, code: str, message: str, now: str) -> None:
        self.connection.execute("UPDATE compute_tasks SET queue_reason_code=?,queue_reason_message=?,updated_at=? WHERE id=?", (code, message, now, task_id))

    def add_budget_adjustment(self, *, account_id: int, actor: str, reason: str, cpu_seconds_delta: int, memory_gb_hours_delta: float, task_count_delta: int, before: dict[str, Any], after: dict[str, Any], now: str) -> None:
        self.connection.execute(
            "INSERT INTO compute_budget_adjustments(account_id,actor,reason,cpu_seconds_delta,memory_gb_hours_delta,task_count_delta,before_json,after_json,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
            (account_id, actor, reason, cpu_seconds_delta, memory_gb_hours_delta, task_count_delta, json.dumps(before, ensure_ascii=False, sort_keys=True), json.dumps(after, ensure_ascii=False, sort_keys=True), now),
        )

    def budget_adjustments(self, project_code: str, period_start: str | None = None) -> list[dict[str, Any]]:
        clause = "WHERE a.project_code=?"
        values: list[Any] = [project_code]
        if period_start is not None:
            clause += " AND a.period_start=?"
            values.append(period_start)
        rows = self.connection.execute(
            "SELECT adj.*,a.project_code AS project_code,a.period_start AS period_start FROM compute_budget_adjustments adj JOIN compute_budget_accounts a ON a.id=adj.account_id " + clause + " ORDER BY adj.id DESC",
            values,
        ).fetchall()
        return [dict(row) for row in rows]
