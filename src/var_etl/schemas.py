
from pyspark.sql.types import StructType, StringType
from pyspark.sql.functions import col, from_json, to_timestamp

PAYLOAD_SCHEMA  = (
    StructType()
      .add("symbol", StringType())
      .add("datetime", StringType())
      .add("open", StringType())
      .add("high", StringType())
      .add("low", StringType())
      .add("close", StringType())
      .add("volume", StringType())
)

def parse_airbyte_payload(df):
    return (
    df
      .withColumn("payload", from_json(col("_airbyte_data"), PAYLOAD_SCHEMA))
      .select("payload.*")   # 👈 FLATTEN:  symbol, datetime, open...
      .withColumn("datetime", to_timestamp(col("datetime")))
      .withColumn("open", col("open").cast("double"))
      .withColumn("high", col("high").cast("double"))
      .withColumn("low", col("low").cast("double"))
      .withColumn("close", col("close").cast("double"))
      .withColumn("volume", col("volume").cast("double"))

)
