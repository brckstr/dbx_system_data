from .parsers import *

table_defs = {
  "query_history" : {
    "schema_file": "query_history",
    "checkpoint_column": "start_time",
    "parser": QueryHistory
  },
  "clusters": {
    "schema_file": "cluster_info",
    "checkpoint_column": "change_time",
    "cluster_cols": ["create_time"],
    "parser": Clusters
  },
  "warehouses": {
    "schema_file": "warehouses",
    "checkpoint_column": "change_time",
    "cluster_cols": ["change_time"],
    "parser": Warehouses    
  }
}