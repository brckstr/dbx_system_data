from datetime import datetime
import json
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import workspace
from databricks.sdk.service import sql

class WarehouseMetrics(object):
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
      warehouse_id = row.id
      warehouse_name = row.name
      now = datetime.now().timestamp()
      if warehouse_id in self.checkpoint.get(str(workspace_id), {}):
        query = {"endpoint_id": warehouse_id, "time_range.start_time_ms": int(1000*self.checkpoint[str(workspace_id)][warehouse_id]["value"].timestamp()), "time_range.end_time_ms": int(1000*now)}
      else:
        query = {"endpoint_id": warehouse_id, "time_range.start_time_ms": int(1000*(now-14*86400)), "time_range.end_time_ms": int(1000*now)}
      while True:
        json = w.api_client.do("GET", f"/api/2.0/sql/history/endpoint-metrics", query=query)
        if "metrics" in json:
            for row in json["metrics"]:
                self.process_row(row, {"workspace_id": workspace_id, "warehouse_id": warehouse_id, "warehouse_name": warehouse_name})
        if "next_page_token" not in json or not json["next_page_token"] or "metrics" not in json:
            break
        query = {"page_token": json["next_page_token"]}
    self.update_target()

  def process_row(self, input_row, context):
    r = input_row

    row = {
      "account_id": None, 
      "workspace_id": context["workspace_id"],
      "warehouse_id": context["warehouse_id"],
      "warehouse_name": context["warehouse_name"],
      "timerange_start": datetime.fromtimestamp(r.get("time_range",{}).get("start_time_ms", 0)/1000),
      "timerange_end": datetime.fromtimestamp(r.get("time_range",{}).get("end_time_ms", 0)/1000),
      "max_running_slots": r.get("max_running_slots", None), 
      "max_queued_slots": r.get("max_queued_slots", None),
      "throughput": r.get("throughput", None),
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
