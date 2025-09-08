
class CommandHistory(object):
  update_type = "append"

  def __init__(self, target_table, spark, checkpoint=None):
    self.target_table = target_table
    self.spark = spark
    self.checkpoint = checkpoint
    self.rows = []
    self.schema = spark.read.table(target_table).schema
  
  def execute(self):
    account_id = self.spark.sql("SELECT account_id FROM system.access.audit LIMIT 1").collect()[0][0]
    if self.checkpoint.get(str(account_id), None):
      chckpnt = self.checkpoint[str(account_id)]["value"]
      query = f"""
      SELECT
  a.account_id,
  a.workspace_id,
  a.event_time,
  a.user_identity.email AS user_name,
  a.service_name,
  a.request_params.warehouseId AS compute_id,
  NULL AS notebook_id,
  'sql' AS statement_language,
  a.request_params.commandId AS command_id,
  a.request_params.commandText AS command_text,
  EXTRACT(SECOND FROM (b.event_time - a.event_time)) AS duration, -- DATEDIFF(SECOND, a.event_time, b.event_time) AS duration_ms,
  CASE b.response.status_code WHEN 200 THEN 'finished' ELSE 'failed' END AS status,
  b.response.status_code,
  b.response.error_message
FROM
  system.access.audit a
    JOIN system.access.audit b
      ON a.request_params.commandId = b.request_params.commandId
where
  a.service_name = 'databrickssql'
  AND a.action_name = 'commandSubmit'
  AND b.action_name = 'commandFinish'
  AND a.event_time >= '{chckpnt.strftime("%Y-%m-%d %H:%M:%S")}' AND b.event_time >= '{chckpnt.strftime("%Y-%m-%d %H:%M:%S")}'

UNION ALL

SELECT
  account_id,
  workspace_id,
  event_time,
  user_identity.email AS user_name,
  service_name,
  request_params.clusterId AS compute_id,
  request_params.notebookId AS notebook_id,
  request_params.commandLanguage AS statement_language,
  request_params.commandId AS command_id,
  request_params.commandText AS command_text,
  DOUBLE(request_params.executionTime) AS duration,
  request_params.status,
  response.status_code,
  response.error_message
FROM
  system.access.audit a
where
  service_name IN ('jobs','notebook')
  AND action_name = 'runCommand'
  AND event_time >= '{chckpnt.strftime("%Y-%m-%d %H:%M:%S")}'
  """
    else:
      query = f"""
      SELECT
  a.account_id,
  a.workspace_id,
  a.event_time,
  a.user_identity.email AS user_name,
  a.service_name,
  a.request_params.warehouseId AS compute_id,
  NULL AS notebook_id,
  'sql' AS statement_language,
  a.request_params.commandId AS command_id,
  a.request_params.commandText AS command_text,
  EXTRACT(SECOND FROM (b.event_time - a.event_time)) AS duration, -- DATEDIFF(SECOND, a.event_time, b.event_time) AS duration_ms,
  CASE b.response.status_code WHEN 200 THEN 'finished' ELSE 'failed' END AS status,
  b.response.status_code,
  b.response.error_message
FROM
  system.access.audit a
    JOIN system.access.audit b
      ON a.request_params.commandId = b.request_params.commandId
where
  a.service_name = 'databrickssql'
  AND a.action_name = 'commandSubmit'
  AND b.action_name = 'commandFinish'

UNION ALL

SELECT
  account_id,
  workspace_id,
  event_time,
  user_identity.email AS user_name,
  service_name,
  request_params.clusterId AS compute_id,
  request_params.notebookId AS notebook_id,
  request_params.commandLanguage AS statement_language,
  request_params.commandId AS command_id,
  request_params.commandText AS command_text,
  DOUBLE(request_params.executionTime) AS duration,
  request_params.status,
  response.status_code,
  response.error_message
FROM
  system.access.audit a
where
  service_name IN ('jobs','notebook')
  AND action_name = 'runCommand'
  """
    df = self.spark.sql(query)
    df.write.mode(self.update_type).saveAsTable(self.target_table)
    




