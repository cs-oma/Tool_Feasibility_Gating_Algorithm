# =============================================================================
#  TOOL FEASIBILITY GATING ALGORITHM (TFG)
#  Product Signature: TFG
# ------------------------------------------------------------------------------
#  File: Metrics/SQLite_Logger.py
#  Purpose: Persist simulation run summaries and per-task outcomes to SQLite.
# =============================================================================

import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable

from Configurations import Simulation_Config
from Core.Task import Mode, Task
from Models.Utility import Utility_Model


def _Task_Missed(task_obj: Task) -> int:
    if task_obj.dropped_in_queue_bool:
        return 1
    if task_obj.completion_time_f64_opt is None:
        return 1
    return 1 if float(task_obj.completion_time_f64_opt) > float(task_obj.deadline) else 0


def _Expected_Utility(task_obj: Task, cfg_simulation_config: Simulation_Config) -> float:
    if _Task_Missed(task_obj):
        return float(cfg_simulation_config.utility_config.missed_deadline_utility_f64)

    mode_opt = task_obj.chosen_mode_mode_opt
    high_priority = bool(getattr(task_obj, "high_priority_bool", False))
    util_cfg = cfg_simulation_config.utility_config
    if mode_opt == Mode.SLOW:
        if high_priority:
            return float(util_cfg.high_priority_slow_success_utility_f64)
        return float(util_cfg.slow_success_utility_f64)
    if mode_opt == Mode.FAST:
        if high_priority:
            return float(util_cfg.high_priority_fast_success_utility_f64)
        return float(util_cfg.fast_success_utility_f64)
    return float(util_cfg.missed_deadline_utility_f64)


class SQLite_Logger:
    def __init__(self, tasklog_db_path: Path, summary_db_path: Path) -> None:
        self.tasklog_db_path = Path(tasklog_db_path)
        self.summary_db_path = Path(summary_db_path)
        self.tasklog_db_path.parent.mkdir(parents=True, exist_ok=True)
        self.summary_db_path.parent.mkdir(parents=True, exist_ok=True)
        self._Init_Schemas()

    def _Init_Schemas(self) -> None:
        with sqlite3.connect(self.tasklog_db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS task_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL,
                    policy_name TEXT NOT NULL,
                    task_id INTEGER NOT NULL,
                    agent_id INTEGER,
                    high_priority INTEGER NOT NULL,
                    arrival_time REAL NOT NULL,
                    deadline REAL NOT NULL,
                    start_service_time REAL,
                    completion_time REAL,
                    drop_time REAL,
                    waiting_time REAL,
                    response_time REAL,
                    service_time REAL,
                    mode TEXT,
                    mode_switches INTEGER,
                    dropped_in_queue INTEGER NOT NULL,
                    missed INTEGER NOT NULL,
                    completed_on_time INTEGER NOT NULL,
                    utility_expected REAL,
                    utility_observed REAL,
                    trace_prompt_type TEXT,
                    trace_fast_sec REAL,
                    trace_slow_sec REAL,
                    created_at_utc TEXT NOT NULL
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_task_logs_run_id ON task_logs(run_id)")

        with sqlite3.connect(self.summary_db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS simulation_runs (
                    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at_utc TEXT NOT NULL,
                    policy_name TEXT NOT NULL,
                    num_agents INTEGER NOT NULL,
                    arrival_lambda_base REAL NOT NULL,
                    arrival_lambda_effective REAL NOT NULL,
                    n_tasks_total INTEGER NOT NULL,
                    n_completed INTEGER NOT NULL,
                    n_dropped_in_queue INTEGER NOT NULL,
                    n_unfinished INTEGER NOT NULL,
                    miss_rate REAL,
                    mean_utility REAL,
                    queue_len_mean REAL,
                    response_time_mean REAL,
                    waiting_time_mean REAL
                )
                """
            )

    def Insert_Run_Summary(self, policy_name: str, cfg_simulation_config: Simulation_Config, aggregate_dict_obj: Dict[str, object]) -> int:
        created_at_utc = datetime.utcnow().isoformat(timespec="seconds")
        response_time_obj = aggregate_dict_obj.get("response_time")
        waiting_time_obj = aggregate_dict_obj.get("waiting_time")

        with sqlite3.connect(self.summary_db_path) as conn:
            cursor = conn.execute(
                """
                INSERT INTO simulation_runs (
                    created_at_utc,
                    policy_name,
                    num_agents,
                    arrival_lambda_base,
                    arrival_lambda_effective,
                    n_tasks_total,
                    n_completed,
                    n_dropped_in_queue,
                    n_unfinished,
                    miss_rate,
                    mean_utility,
                    queue_len_mean,
                    response_time_mean,
                    waiting_time_mean
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    created_at_utc,
                    str(policy_name),
                    int(cfg_simulation_config.num_agents_i32),
                    float(cfg_simulation_config.arrival_config.lambda_rate_f64),
                    float(cfg_simulation_config.Effective_Arrival_Rate()),
                    int(aggregate_dict_obj.get("n_tasks_total", 0)),
                    int(aggregate_dict_obj.get("n_completed", 0)),
                    int(aggregate_dict_obj.get("n_dropped_in_queue", 0)),
                    int(aggregate_dict_obj.get("n_unfinished", 0)),
                    float(aggregate_dict_obj.get("miss_rate", float("nan"))),
                    float(aggregate_dict_obj.get("mean_utility", float("nan"))),
                    float(aggregate_dict_obj.get("queue_len_mean", float("nan"))),
                    float(getattr(response_time_obj, "mean_f64", float("nan"))),
                    float(getattr(waiting_time_obj, "mean_f64", float("nan"))),
                ),
            )
            run_id_i32 = int(cursor.lastrowid)
            conn.commit()

        return run_id_i32

    def Insert_Task_Logs(
        self,
        run_id_i32: int,
        policy_name: str,
        cfg_simulation_config: Simulation_Config,
        utility_model: Utility_Model,
        tasks_iterable_task: Iterable[Task],
    ) -> None:
        created_at_utc = datetime.utcnow().isoformat(timespec="seconds")
        rows = []
        for task_obj in tasks_iterable_task:
            missed_i32 = int(_Task_Missed(task_obj))
            completed_on_time_i32 = int(
                task_obj.completion_time_f64_opt is not None
                and float(task_obj.completion_time_f64_opt) <= float(task_obj.deadline)
            )
            rows.append(
                (
                    int(run_id_i32),
                    str(policy_name),
                    int(task_obj.task_id),
                    int(task_obj.assigned_agent_id_i32_opt) if task_obj.assigned_agent_id_i32_opt is not None else None,
                    int(1 if bool(getattr(task_obj, "high_priority_bool", False)) else 0),
                    float(task_obj.arrival_time),
                    float(task_obj.deadline),
                    float(task_obj.start_service_time_f64_opt) if task_obj.start_service_time_f64_opt is not None else None,
                    float(task_obj.completion_time_f64_opt) if task_obj.completion_time_f64_opt is not None else None,
                    float(task_obj.drop_time_f64_opt) if task_obj.drop_time_f64_opt is not None else None,
                    float(task_obj.Waiting_Time) if task_obj.Waiting_Time is not None else None,
                    float(task_obj.Response_Time) if task_obj.Response_Time is not None else None,
                    float(task_obj.service_time_f64_opt) if task_obj.service_time_f64_opt is not None else None,
                    str(task_obj.chosen_mode_mode_opt.value) if task_obj.chosen_mode_mode_opt is not None else None,
                    int(task_obj.mode_switches_i32),
                    int(1 if task_obj.dropped_in_queue_bool else 0),
                    int(missed_i32),
                    int(completed_on_time_i32),
                    float(_Expected_Utility(task_obj, cfg_simulation_config)),
                    float(utility_model.Utility(task_obj)),
                    str(task_obj.meta_dict_obj.get("trace_prompt_type", "")),
                    float(task_obj.meta_dict_obj.get("trace_fast_sec")) if task_obj.meta_dict_obj.get("trace_fast_sec") is not None else None,
                    float(task_obj.meta_dict_obj.get("trace_slow_sec")) if task_obj.meta_dict_obj.get("trace_slow_sec") is not None else None,
                    created_at_utc,
                )
            )

        if not rows:
            return

        with sqlite3.connect(self.tasklog_db_path) as conn:
            conn.executemany(
                """
                INSERT INTO task_logs (
                    run_id,
                    policy_name,
                    task_id,
                    agent_id,
                    high_priority,
                    arrival_time,
                    deadline,
                    start_service_time,
                    completion_time,
                    drop_time,
                    waiting_time,
                    response_time,
                    service_time,
                    mode,
                    mode_switches,
                    dropped_in_queue,
                    missed,
                    completed_on_time,
                    utility_expected,
                    utility_observed,
                    trace_prompt_type,
                    trace_fast_sec,
                    trace_slow_sec,
                    created_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            conn.commit()
