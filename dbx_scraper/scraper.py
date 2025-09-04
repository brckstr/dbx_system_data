from collections import defaultdict
import os

from .table_defs import table_defs


class DBXScraper(object):
    """docstring for DBXScraper"""

    def __init__(self, data_type, target_table, spark):
        self.data_type = data_type
        self.target_table = target_table
        self.spark = spark
        self.checkpoint = table_defs[self.data_type]["checkpoint_cols"]
        self.cluster_columns = ", ".join(table_defs[self.data_type]["cluster_cols"])
        self.schema = self.load_schema(data_type)
        self.parser = table_defs[self.data_type]["parser"]
    
    def load_schema(self, data_type):
        file_name = table_defs[self.data_type]["schema_file"]
        schema_file = os.path.join(os.path.dirname(__file__),f"schemas/{file_name}.txt")
        return open(schema_file, "r").read()

    def execute(self):
        self.spark.sql(f"CREATE TABLE IF NOT EXISTS {self.target_table} ( {self.schema} ) USING delta CLUSTER BY ({self.cluster_columns})")
        checkpoint_data = self.spark.sql(f"SELECT {', '.join(self.checkpoint['filters'])}, MAX({self.checkpoint['value']}) AS key FROM {self.target_table} GROUP BY {', '.join(self.checkpoint['filters'])}").collect()
        latest_checkpoint = self.build_checkpoint(checkpoint_data)
        print(f"latest_checkpoint: {latest_checkpoint}")
        print(f"Target table: {self.target_table}")
        self.parser(self.target_table, self.spark, latest_checkpoint).execute()
        self.spark.sql(f"OPTIMIZE {self.target_table}")
    
    def build_checkpoint(self, checkpoint_data):
        checkpoint_object = defaultdict(dict)
        for row in checkpoint_data:
            scoped_object = checkpoint_object
            for f in self.checkpoint["filters"]:
                if row[f] not in scoped_object:
                    scoped_object[row[f]] = {}
                scoped_object = scoped_object[row[f]]
            scoped_object["value"] = row["key"]
        return checkpoint_object
            

