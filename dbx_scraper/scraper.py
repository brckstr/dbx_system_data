import os

from .table_defs import table_defs


class DBXScraper(object):
    """docstring for DBXScraper"""

    def __init__(self, data_type, target_table, spark):
        self.data_type = data_type
        self.target_table = target_table
        self.spark = spark
        self.checkpoint = table_defs[self.data_type]["checkpoint_column"]
        self.schema = self.load_schema(data_type)
    
    def load_schema(self, data_type):
        file_name = table_defs[self.data_type]["schema_file"]
        schema_file = os.path.join(os.path.dirname(__file__),f"schemas/{file_name}.txt")
        return open(schema_file, "r").read()

    def execute(self):
        self.spark.sql(f"CREATE TABLE IF NOT EXISTS {self.target_table} ( {self.schema} ) USING delta CLUSTER BY AUTO")
        latest_checkpoint = self.spark.sql(f"SELECT MAX({self.checkpoint}) FROM {self.target_table}").collect()[0][0]
        print(f"latest_checkpoint: {latest_checkpoint}")
        print(f"Hello {self.target_table}")