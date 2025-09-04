from datetime import datetime
import json
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import workspace
from databricks.sdk.service import sql

class ClusterEvents(object):
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
    response = w.clusters.list()
    for row in response:
      cluster_id = row.cluster_id
      if cluster_id in self.checkpoint.get(str(workspace_id), {}):
        response = w.clusters.events(
          cluster_id=cluster_id, 
          start_time=self.checkpoint[str(workspace_id)][cluster_id]["value"]
        )
      else:
        response = w.clusters.events(cluster_id=cluster_id)
      for row in response:
        print(row)
        self.process_row(row, {"workspace_id": workspace_id, "cluster_id": cluster_id})
    self.update_target()

  def process_row(self, input_row, context):
    r = input_row.as_dict()

    row = {
      "account_id": None, 
      "workspace_id": context["workspace_id"],
      "cluster_id": context["cluster_id"],
      "timestamp": datetime.fromtimestamp(r.get("timestamp", None)/1000),
      "type": r.get("type", None), 
      "details": json.dumps(r.get("details", None)) # r.get("details", None)# 
    }
    print(row)
    self.rows.append(row)
    if len(self.rows) > 10000:
      self.update_target()
    
  def update_target(self):
    if len(self.rows) > 0:
      df = self.spark.createDataFrame(self.rows, self.schema)
      print(len(self.rows))
      df.write.mode(self.update_type).saveAsTable(self.target_table)
      self.rows = []
