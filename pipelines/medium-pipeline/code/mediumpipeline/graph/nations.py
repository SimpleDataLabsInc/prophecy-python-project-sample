from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from mediumpipeline.config.ConfigStore import *
from mediumpipeline.udfs.UDFs import *

def nations(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''n_nationkey''']
    valueColumns = ['''n_name''']
    createLookup("nations", in0, spark, keyColumns, valueColumns)
