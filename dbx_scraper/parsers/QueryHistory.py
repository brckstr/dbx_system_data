from datetime import datetime
import re
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import workspace
from databricks.sdk.service import sql

class QueryHistory(object):
  update_type = "append"
  pattern = r'sparkui\/(?P<session_id>[^\/]+)\/driver\-(?P<driver_id>\d+)'

  def __init__(self, target_table, spark, checkpoint=None):
    self.target_table = target_table
    self.spark = spark
    self.checkpoint = checkpoint
    self.rows = []
    self.schema = spark.read.table(target_table).schema
  
  def execute(self):
    w = WorkspaceClient()
    workspace_id = w.get_workspace_id()
    print(workspace_id)
    if self.checkpoint:
      response = w.query_history.list(
        filter_by=sql.QueryFilter(
          query_start_time_range=sql.TimeRange(start_time_ms=int(1000*self.checkpoint.timestamp()), end_time_ms=int(1000*time.time()))
        ),
        include_metrics=True
      )
    else:
      response = w.query_history.list(include_metrics=True)
    for row in response:
      print(row)
      self.process_row(row, {"workspace_id": workspace_id})
    self.update_target()

  def process_row(self, input_row, context):
    r = input_row.as_dict()
    if (spark_url := r.get("spark_ui_url", None)):
      session_match = re.search(self.pattern, spark_url).groupdict()
    else:
      session_match = None

    queue_time = r.get("overloading_queue_start_timestamp", None)
    scale_time = r.get("overloading_queue_start_timestamp", None)
    comp_time = r.get("query_compilation_start_timestamp", None)
    if (cache_bytes := r.get("metrics",{}).get("read_cache_bytes", None)):
      if (read_bytes := r.get("metrics",{}).get("read_bytes", None)):
        read_io_cache_percent = round(100*cache_bytes/read_bytes, 3)
      else:
        read_io_cache_percent = 100
    else:
      read_io_cache_percent = None
    waiting_for_compute_duration_ms = comp_time - scale_time if scale_time else None
    waiting_at_capacity_duration_ms = comp_time - queue_time if queue_time else None

    if (end_time_ms:=r.get("query_end_time_ms", None)):
      end_time = datetime.fromtimestamp(end_time_ms/1000)
    else:
      end_time = None

    row = {
      "account_id": None, 
      "workspace_id": context["workspace_id"],
      "statement_id": r.get("query_id", None),
      "executed_by": r.get("user_name", None),
      "session_id": session_match["session_id"] if session_match else None, 
      "execution_status": r.get("status",None),
      "compute": {
        "type": "WAREHOUSE",
        "cluster_id": None,
        "warehouse_id": r.get("warehouse_id", None)
      },
      "executed_by_user_id": r.get("user_id", None), 
      "statement_text": r.get("statement_text", None),
      "statement_type": r.get("statement_type", None), 
      "error_message": r.get("error_message", None), 
      "client_application": r.get("client_application", None), 
      "client_driver": session_match["driver_id"] if session_match else None,
      "total_duration_ms": r.get("metrics",{}).get("total_time_ms", None),  
      "waiting_for_compute_duration_ms": waiting_for_compute_duration_ms, 
      "waiting_at_capacity_duration_ms": waiting_at_capacity_duration_ms, 
      "execution_duration_ms": r.get("metrics",{}).get("execution_time_ms", None), 
      "compilation_duration_ms": r.get("metrics",{}).get("compilation_time_ms", None),
      "total_task_duration_ms": r.get("metrics",{}).get("task_total_time_ms", None), 
      "result_fetch_duration_ms": r.get("metrics",{}).get("result_fetch_time_ms", None),  
      "start_time": datetime.fromtimestamp(r.get("query_start_time_ms", None)/1000),
      "end_time": end_time, 
      "update_time": end_time, 
      "read_partitions": r.get("metrics",{}).get("read_partitions_count", None), 
      "pruned_files": r.get("metrics",{}).get("pruned_files_count", None),
      "read_files": r.get("metrics",{}).get("read_files_count", None),
      "read_rows": r.get("metrics",{}).get("rows_read_count", None), 
      "produced_rows": r.get("metrics",{}).get("rows_produced_count", None), 
      "read_bytes": r.get("metrics",{}).get("read_bytes", None), 
      "read_io_cache_percent": read_io_cache_percent,
      "from_result_cache": r.get("metrics",{}).get("result_from_cache", None), 
      "spilled_local_bytes": r.get("metrics",{}).get("spill_to_disk_bytes", None),
      "written_bytes": r.get("metrics",{}).get("write_remote_bytes", None),
      "shuffle_read_bytes": r.get("metrics",{}).get("network_sent_bytes", None), 
      "query_source": {
        "job_info": {
          "job_id": r.get("query_source",{}).get("job_info", {}).get("job_id", None), 
          "job_run_id": r.get("query_source",{}).get("job_info", {}).get("job_run_id", None),
          "job_task_run_id": r.get("query_source",{}).get("job_info", {}).get("job_task_run_id", None)
        }, 
        "legacy_dashboard_id": r.get("query_source",{}).get("legacy_dashboard_id", None), 
        "dashboard_id": r.get("query_source",{}).get("dashboard_id", None), 
        "alert_id": r.get("query_source",{}).get("alert_id", None), 
        "notebook_id": r.get("query_source",{}).get("notebook_id", None), 
        "sql_query_id": r.get("query_source",{}).get("sql_query_id", None), 
        "genie_space_id": r.get("query_source",{}).get("genie_space_id", None) 
      },
      "executed_as_user_id": r.get("executed_as_user_id", None), 
      "executed_as": r.get("executed_as_user_name", None)
    }
    print(row)
    self.rows.append(row)
    if len(self.rows) > 10000:
      self.update_target()
    
  def update_target(self):
    if len(self.rows) > 0:
      df = self.spark.createDataFrame(self.rows, self.schema)
      df.write.mode(self.update_type).saveAsTable(self.target_table)
      self.rows = []
