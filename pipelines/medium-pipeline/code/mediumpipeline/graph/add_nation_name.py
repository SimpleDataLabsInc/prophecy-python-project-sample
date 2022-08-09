from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from mediumpipeline.config.ConfigStore import *
from mediumpipeline.udfs.UDFs import *

def add_nation_name(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("c_custkey"), 
        lookup("nations", col("c_nationkey")).getField("n_name").alias("c_nation_name"), 
        col("o_orderkey"), 
        concat(col("o_orderstatus"), lit("123")).alias("o_orderstatus")
    )
