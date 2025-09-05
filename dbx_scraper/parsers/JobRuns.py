from datetime import datetime
import re
import json

from delta.tables import DeltaTable

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import workspace
from databricks.sdk.service import sql

class JobRuns(object):
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
    response = w.jobs.list_runs(limit=0)
    if self.checkpoint.get(str(workspace_id), None):
      chckpnt = self.checkpoint[str(workspace_id)]["value"]
      response = w.jobs.list_runs(limit=0, start_time_from=int(1000*chckpnt.timestamp()))
    else:
      response = w.jobs.list_runs(limit=0)
    for row in response:
      self.process_row(row, {"workspace_id": workspace_id})
    self.update_target()

  def process_row(self, input_row, context):
    r = input_row.as_dict()

    row = {
        "account_id": None, 
        "workspace_id": context["workspace_id"],
        "job_id": r.get("job_id"),
        "run_id": r.get("run_id", None),
        "period_start_time": datetime.fromtimestamp(r.get("start_time", None)/1000),
        "period_end_time": datetime.fromtimestamp(r.get("end_time", None)/1000),
        "trigger_type": r.get("trigger", None),
        "result_state": r.get("state", {}).get("result_state", None),
        "state_message": r.get("state", {}).get("state_message", None),
        "run_type": r.get("run_type", None),
        "run_name": r.get("run_name", None),
        "run_duration": r.get("run_duration", None),
        "compute_ids": r.get("job_clusters", None),
        "termination_code": None,
        "job_parameters": None, #r.get("job_parameters", None),
        "definition": json.dumps(r)
      }
    self.rows.append(row)
    if len(self.rows) > 10000:
      self.update_target()
    
  def update_target(self):
    if len(self.rows) > 0:
      df = self.spark.createDataFrame(self.rows, self.schema)
      df.write.mode(self.update_type).saveAsTable(self.target_table)
      self.rows = []
