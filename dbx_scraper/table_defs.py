from .parsers import *

table_defs = {
  "query_history" : {
    "schema_file": "query_history",
    "checkpoint_cols": {"value":"start_time", "filters": ["workspace_id"]},
    "cluster_cols": ["start_time", "workspace_id"],
    "parser": QueryHistory
  },
  "clusters": {
    "schema_file": "cluster_info",
    "checkpoint_cols": {"value":"change_time", "filters": ["workspace_id"]},
    "cluster_cols": ["create_time", "workspace_id"],
    "parser": Clusters
  },
  "warehouses": {
    "schema_file": "warehouses",
    "checkpoint_cols": {"value":"change_time", "filters": ["workspace_id"]},
    "cluster_cols": ["change_time", "workspace_id"],
    "parser": Warehouses    
  },
  "cluster_events": {
    "schema_file": "cluster_events",
    "checkpoint_cols": {"value":"timestamp", "filters": ["workspace_id", "cluster_id"]},
    "cluster_cols": ["timestamp", "workspace_id", "cluster_id"],
    "parser": ClusterEvents
  },
  "warehouse_events": {
    "schema_file": "warehouse_events",
    "checkpoint_cols": {"value":"event_time", "filters": ["workspace_id", "warehouse_id"]},
    "cluster_cols": ["event_time", "workspace_id", "warehouse_id"],
    "parser": WarehouseEvents
  },
  "warehouse_metrics": {
    "schema_file": "warehouse_metrics",
    "checkpoint_cols": {"value":"timerange_end", "filters": ["workspace_id", "warehouse_id"]},
    "cluster_cols": ["timerange_start", "workspace_id", "warehouse_id"],
    "parser": WarehouseMetrics
  },
  "jobs": {
    "schema_file": "jobs",
    "checkpoint_cols": {"value":"change_time", "filters": ["workspace_id", "job_id"]},
    "cluster_cols": ["change_time", "workspace_id"],
    "parser": Jobs
  },
  "job_tasks": {
    "schema_file": "job_tasks",
    "checkpoint_cols": {"value":"change_time", "filters": ["workspace_id", "job_id"]},
    "cluster_cols": ["change_time", "workspace_id", "job_id"],
    "parser": JobTasks
  },
  "job_run_timeline": {
    "schema_file": "job_run_timeline",
    "checkpoint_cols": {"value":"period_start_time", "filters": ["workspace_id"]},
    "cluster_cols": ["period_start_time", "workspace_id", "job_id"],
    "parser": JobRuns
  },
  "job_run_task_timeline": {
    "schema_file": "job_run_task_timeline",
    "checkpoint_cols": {"value":"period_start_time", "filters": ["workspace_id"]},
    "cluster_cols": ["period_start_time", "workspace_id", "job_id"],
    "parser": JobRunTasks
  },
"user_mapping": {
    "schema_file": "users",
    "checkpoint_cols": {"value":"user_id", "filters": ["workspace_id"]},
    "cluster_cols": ["workspace_id", "user_id"],
    "parser": Users
  },
"command_history": {
    "schema_file": "command_history",
    "checkpoint_cols": {"value":"event_time", "filters": ["account_id"]},
    "cluster_cols": ["event_time", "workspace_id"],
    "parser": CommandHistory
  }
}