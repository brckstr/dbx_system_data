from datetime import datetime
import re
import time

from delta.tables import DeltaTable

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import workspace
from databricks.sdk.service import sql

class Clusters(object):
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
      self.process_row(row, {"workspace_id": workspace_id})
    self.update_target()

  def process_row(self, input_row, context):
    r = input_row.as_dict()

    row = {
        "account_id": None, 
        "workspace_id": context["workspace_id"],
        "cluster_id": r.get("cluster_id", None),
        "cluster_name": r.get("cluster_name", None),
        "owned_by": r.get("creator_user_name", None),
        "create_time": None,
        "delete_time": None,
        "driver_node_type": r.get("driver_node_type_id", None),
        "worker_node_type": r.get("node_type_id", None),
        "worker_count": r.get("num_workers", 0),
        "min_autoscale_workers": r.get("autoscale", {}).get("min_workers", None),
        "max_autoscale_workers": r.get("autoscale", {}).get("max_workers", None),
        "auto_termination_minutes": r.get("autotermination_minutes", 0),
        "enable_elastic_disk": r.get("enable_elastic_disk",False),
        "tags": r.get("custom_tags", {}),
        "cluster_source": r.get("cluster_source", None),
        "init_scripts": r.get("init_scripts", []),
        "aws_attributes": {
          "first_on_demand": r.get("aws_attributes", None).get("first_on_demand", None), 
          "availability": r.get("aws_attributes", None).get("availability", None), 
          "zone_id": r.get("aws_attributes", None).get("zone_id", None), 
          "instance_profile_arn": r.get("aws_attributes", None).get("instance_profile_arn", None), 
          "spot_bid_price_percent": r.get("aws_attributes", None).get("spot_bid_price_percent", None), 
          "ebs_volume_type": r.get("aws_attributes", None).get("ebs_volume_type", None), 
          "ebs_volume_count": r.get("aws_attributes", None).get("ebs_volume_count", None), 
          "ebs_volume_size": r.get("aws_attributes", None).get("ebs_volume_size", None), 
          "ebs_volume_iops": r.get("aws_attributes", None).get("ebs_volume_iops", None), 
          "ebs_volume_throughput": r.get("aws_attributes", None).get("ebs_volume_throughput", None)
        },
        "azure_attributes": None,
        "gcp_attributes": None,
        "driver_instance_pool_id": r.get("driver_instance_pool_id", None),
        "worker_instance_pool_id": r.get("instance_pool_id", None),
        "dbr_version": r.get("spark_version", None),
        "change_time": None,
        "change_date": None,
        "data_security_mode": r.get("data_security_mode",None),
        "policy_id": r.get("policy_id", None)
      }
    self.rows.append(row)
    if len(self.rows) > 10000:
      self.update_target()
    
  def update_target(self):
    if len(self.rows) > 0:
      df = self.spark.createDataFrame(self.rows, self.schema)
      target = DeltaTable.forName(self.spark, self.target_table)
      target.alias("target").merge(
        df.alias("source"), "target.workspace_id = source.workspace_id AND target.cluster_id = source.cluster_id"
      ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
      self.rows = []
