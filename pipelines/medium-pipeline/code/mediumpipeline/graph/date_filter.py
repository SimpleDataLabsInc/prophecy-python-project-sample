from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from mediumpipeline.config.ConfigStore import *
from mediumpipeline.udfs.UDFs import *

def date_filter(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(
        (
          (col("o_orderdate") >= to_date(lit(Config.date_from), "yyyy-MM-dd"))
          & (col("o_orderdate") <= to_date(lit(Config.date_to), "yyyy-MM-dd"))
        )
    )
