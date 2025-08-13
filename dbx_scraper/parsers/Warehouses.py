from datetime import datetime
import re
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import workspace
from databricks.sdk.service import sql

class Warehouses(object):
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
    response = w.warehouses.list()
    for row in response:
      self.process_row(row, {"workspace_id": workspace_id})
    self.update_target()

  def process_row(self, input_row, context):
    r = input_row.as_dict()

    row = {
      "warehouse_id": r.get("id", None),
      "workspace_id": context["workspace_id"],
      "account_id": None,
      "warehouse_name": r.get("name", None),
      "warehouse_type": r.get("warehouse_type", None), 
      "warehouse_channel": r.get("channel", {}).get("name", None),
      "warehouse_size": r.get("cluster_size", None), 
      "min_clusters": r.get("min_num_clusters", 0),
      "max_clusters": r.get("max_num_clusters", 0),
      "auto_stop_minutes": r.get("auto_stop_mins", None), 
      "tags": { tag["key"]: tag["value"] for tag in r.get("tags",{}).get("custom_tags",[])},
      "change_time": None,
      "delete_time": None 
    }
    print(row)
    self.rows.append(row)
    if len(self.rows) > 10000:
      self.update_target()
    
  def update_target(self):
    if len(self.rows) > 0:
      df = self.spark.createDataFrame(self.rows, self.schema)
      target = DeltaTable(self.target_table)
      target.alias("target").merge(
        df.alias("source"), "target.workspace_id = source.workspace_id AND target.warehouse_id = source.warehouse_id"
      ).whenMatchedUpdateAll()
      .whenNotMatchedInsertAll()
      .execute()
      self.rows = []
