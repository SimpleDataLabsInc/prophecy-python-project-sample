from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from mediumpipeline.config.ConfigStore import *
from mediumpipeline.udfs.UDFs import *

def orders_per_nation_2(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"maciej.orders_per_nation")
