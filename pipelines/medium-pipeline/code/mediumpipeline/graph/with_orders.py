from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from mediumpipeline.config.ConfigStore import *
from mediumpipeline.udfs.UDFs import *

def with_orders(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.c_custkey") == col("in1.o_custkey")), "inner")\
        .select(col("in0.c_custkey").alias("c_custkey"), col("in0.c_nationkey").alias("c_nationkey"), col("in1.o_orderkey").alias("o_orderkey"), col("in1.o_orderstatus").alias("o_orderstatus"))
