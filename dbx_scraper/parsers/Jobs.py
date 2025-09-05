from datetime import datetime
import re
import json

from delta.tables import DeltaTable

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import workspace
from databricks.sdk.service import sql

class Jobs(object):
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
    response = w.jobs.list(limit=100)
    for row in response:
      self.process_row(row, {"workspace_id": workspace_id})
    self.update_target()

  def process_row(self, input_row, context):
    r = input_row.as_dict()

    row = {
        "account_id": None, 
        "workspace_id": context["workspace_id"],
        "job_id": r.get("job_id", None),
        "name": r.get("name", None),
        "creator_user_name": r.get("creator_user_name", None),
        "tags": r.get("settings",{}).get("tags", None),
        "run_as": r.get("run_as", None),
        "change_time": None,
        "delete_time": None,
        "description": r.get("description", None),
        "definition": json.dumps(r)
      }
    self.rows.append(row)
    if len(self.rows) > 10000:
      self.update_target()
    
  def update_target(self):
    if len(self.rows) > 0:
      df = self.spark.createDataFrame(self.rows, self.schema)
      target = DeltaTable.forName(self.spark, self.target_table)
      target.alias("target").merge(
        df.alias("source"), "target.workspace_id = source.workspace_id AND target.job_id = source.job_id"
      ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
      self.rows = []
