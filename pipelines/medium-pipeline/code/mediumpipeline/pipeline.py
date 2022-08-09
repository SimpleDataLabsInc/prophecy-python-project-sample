from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from mediumpipeline.config.ConfigStore import *
from mediumpipeline.udfs.UDFs import *
from prophecy.utils import *
from mediumpipeline.graph import *

def pipeline(spark: SparkSession) -> None:
    df_tpch_nations = tpch_nations(spark)
    nations(spark, df_tpch_nations)
    df_tpch_customers = tpch_customers(spark)
    df_tpch_orders = tpch_orders(spark)
    df_date_filter = date_filter(spark, df_tpch_orders)
    df_with_orders = with_orders(spark, df_tpch_customers, df_date_filter)
    df_add_nation_name = add_nation_name(spark, df_with_orders)
    df_orders_per_nation = orders_per_nation(spark, df_add_nation_name)
    df_passthrough = passthrough(spark, df_orders_per_nation)
    df_passthrough_sql = passthrough_sql(spark, df_passthrough)
    orders_per_nation_1(spark, df_passthrough_sql)
    df_orders_per_nation_2 = orders_per_nation_2(spark)
    df_preview = preview(spark, df_orders_per_nation_2)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "2722/pipelines/medium-pipeline")
    MetricsCollector.start(spark = spark, pipelineId = "2722/pipelines/medium-pipeline")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
