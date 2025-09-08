# dbx_system_data
Tools for generating system tables by scraping data from the Databricks APIs

  * __Query History:__ Pulls the query history data for all queries executed against SQL warehouses from the Workspace API. Needs to be run in each workspace.

  * __Cluster Info:__ Pulls data about configured clusters from the Workspace API. Needs to be run in each workspace.

  * __Cluster Events:__ Pulls cluster event data from the Workspace API including cluster start, termination, auto-scaling, etc. Needs to be run in each workspace.

  * __Warehouses:__ Pulls data about configured SQL Warehouses from the Workspace API. Needs to be run in each workspace.

  * __Warehouse Events:__ Pulls SQL warehouse event data from the Workspace API including warehouse start, termination, auto-scaling, etc. Needs to be run in each workspace.

  * __Warehouse Metrics:__ Pulls metric data about SQL Warehouse utilization from the Workspace API including queuing, concurrency and total throughput. Needs to be run in each workspace.

  * __Jobs:__ Pulls data about configured jobs from the Workspace API. Needs to be run in each workspace.

  * __Job Tasks:__ Pulls data about the tasks of configured configured jobs from the Workspace API. Needs to be run in each workspace.

  * __Job Run Timeline:__ Pulls timing and status details about job runs from the Workspace API. Needs to be run in each workspace.

  * __Job Run Task Timeline:__ Pulls timing and status details about tasks within job runs from the Workspace API. Needs to be run in each workspace.

  * __User Mapping:__ Pulls data about users and the groups that they belong to from the Workspace API. Needs to be run in each workspace.

  * __Command History:__ Pulls data about all executed commands from notebooks, jobs or sql queries from the System Audit Tables. Only needs to be run in a single workspace.
