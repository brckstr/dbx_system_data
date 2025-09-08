from datetime import datetime
import re
import time

from delta.tables import DeltaTable

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import workspace
from databricks.sdk.service import sql
from databricks.sdk.service import iam

class Users(object):
  update_type = "merge"

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
    response = w.users.list(
        attributes="id,userName,displayName,groups,externalId,active",
        sort_by="userName",
        sort_order=iam.ListSortOrder.DESCENDING,
    )
    for row in response:
      self.process_row(row, {"workspace_id": workspace_id})
    self.update_target()

  def process_row(self, input_row, context):

    row = {
        "account_id": None, 
        "workspace_id": context["workspace_id"],
        "user_id": input_row.id,
        "user_name": input_row.user_name,
        "display_name": input_row.display_name,
        "groups": [g.display for g in input_row.groups],
        "active": input_row.active
      }
    self.rows.append(row)
    if len(self.rows) > 10000:
      self.update_target()
    
  def update_target(self):
    if len(self.rows) > 0:
      df = self.spark.createDataFrame(self.rows, self.schema)
      target = DeltaTable.forName(self.spark, self.target_table)
      target.alias("target").merge(
        df.alias("source"), "target.workspace_id = source.workspace_id AND target.user_id = source.user_id"
      ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
      self.rows = []
