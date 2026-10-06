import sys

from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession

from fda import curate

args = getResolvedOptions(sys.argv, ["RAW_ROOT", "CURATED_ROOT", "RUN_DATE"])
spark = SparkSession.builder.getOrCreate()
run_date = curate.parse_run_date(args["RUN_DATE"])
curate.run(spark, args["RAW_ROOT"], args["CURATED_ROOT"], run_date)
