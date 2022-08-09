from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from mediumpipeline.config.ConfigStore import *
from mediumpipeline.udfs.UDFs import *

def orders_per_nation(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("c_nation_name"), col("o_orderstatus"))

    return df1.agg(countDistinct(col("c_custkey")).alias("customers"), count(col("o_orderkey")).alias("orders"))
