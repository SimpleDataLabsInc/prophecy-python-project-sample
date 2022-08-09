from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from mediumpipeline.config.ConfigStore import *
from mediumpipeline.udfs.UDFs import *

def orders_per_nation_1(spark: SparkSession, in0: DataFrame):
    in0.write.format("delta").mode("append").saveAsTable("maciej.orders_per_nation")
