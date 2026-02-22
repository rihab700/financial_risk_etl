from pyspark.sql import SparkSession
from ..schemas import parse_airbyte_payload

def run_local_batch(spark: SparkSession, df):
    parsed = parse_airbyte_payload(df)
    return parsed