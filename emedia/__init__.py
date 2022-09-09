# coding: utf-8

import os, sys, logging, configparser

log = logging.getLogger("log")
log.setLevel(logging.INFO)

_formatter = logging.Formatter("[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s")

# console
_console_handler = logging.StreamHandler(sys.stdout)
_console_handler.setFormatter(_formatter)
log.addHandler(_console_handler)


def _get_dbr_env():
    # Local dev
    if not os.path.exists('/dbfs/mnt/databricks_conf.ini'):
        return 'qa'
    
    dbfsCfgParser = configparser.ConfigParser()
    dbfsCfgParser.read('/dbfs/mnt/databricks_conf.ini')

    if dbfsCfgParser['common']['env'] == 'qa':
        return 'qa'
    else:
        return 'prod'


dbr_env = _get_dbr_env()


from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


def get_spark():
    spark = SparkSession.builder.appName('media_social')\
        .config("spark.jars.packages","com.crealytics:spark-excel_2.12:0.13.4").config("spark.sql.legacy.timeParserPolicy", "LEGACY")\
        .getOrCreate()
    return spark

