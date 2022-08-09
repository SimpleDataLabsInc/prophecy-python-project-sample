from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from mediumpipeline.config.ConfigStore import *
from mediumpipeline.udfs.UDFs import *

def passthrough(spark: SparkSession, in0: DataFrame) -> DataFrame:
    out0 = in0

    return out0
