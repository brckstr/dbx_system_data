from datetime import datetime
import re
import json

from delta.tables import DeltaTable

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import workspace
from databricks.sdk.service import sql

class JobRunTasks(object):
  update_type = "append"

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
    if self.checkpoint.get(str(workspace_id), None):
      chckpnt = self.checkpoint[str(workspace_id)]["value"]
      response = w.jobs.list_runs(limit=0, start_time_from=int(1000*chckpnt.timestamp()), expand_tasks=True, completed_only=True)
    else:
      response = w.jobs.list_runs(limit=0, expand_tasks=True, completed_only=True)
    for row in response:
      for task in row.tasks:
        self.process_row(task, {"workspace_id": workspace_id, "job_id": row.job_id, "run_id": row.run_id})
    self.update_target()

  def process_row(self, input_row, context):
    r = input_row.as_dict()

    row = {
        "account_id": None, 
        "workspace_id": context["workspace_id"],
        "job_id": context["job_id"],
        "run_id": r.get("run_id", None),
        "period_start_time": datetime.fromtimestamp(r.get("start_time", None)/1000),
        "period_end_time": datetime.fromtimestamp(r.get("end_time", None)/1000),
        "result_state": r.get("state", {}).get("result_state", None),
        "state_message": r.get("state", {}).get("state_message", None),
        "job_run_id": context["run_id"],
        "task_duration": r.get("execution_duration", None),
        "task_key": r.get("task_key", None),
        "task_type": r.get("task_type", None),
        "task_name": r.get("display_name"),
        "compute_ids": r.get("job_clusters", None),
        "definition": json.dumps(r),
        "job_run_id": context["run_id"],
        "parent_run_id": None,
        "termination_code": None
      }
    self.rows.append(row)
    if len(self.rows) > 10000:
      self.update_target()
    
  def update_target(self):
    if len(self.rows) > 0:
      df = self.spark.createDataFrame(self.rows, self.schema)
      df.write.mode(self.update_type).saveAsTable(self.target_table)
      self.rows = []
