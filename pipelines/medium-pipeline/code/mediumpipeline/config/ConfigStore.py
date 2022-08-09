from pyspark.sql import SparkSession
from prophecy.config.utils import *
from prophecy.udfs.scala_udf_wrapper import initializeUDFBase
from .Config import Config as ConfigClass
Config: ConfigClass = ConfigClass()


class Utils:
    @staticmethod
    def initializeFromArgs(spark: SparkSession, args):
        global Config
        Config.updateSpark(spark)
        initializeUDFBase(spark)
        conf = parse_config(args)
        Config.update(**conf)
