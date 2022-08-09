from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from mediumpipeline.config.ConfigStore import *
from mediumpipeline.udfs.UDFs import *

def tpch_nations(spark: SparkSession) -> DataFrame:
    return spark.read.format("delta").load("dbfs:/databricks-datasets/tpch/delta-001/nation/")
