from pyspark.sql import SparkSession, DataFrame
from delta.tables import DeltaTable

from ..schemas import parse_airbyte_payload


def upsert_to_delta(microbatch_df: DataFrame, batch_id: int, target_table: str) -> None:
    # Deduplicate within micro-batch
    microbatch_df = microbatch_df.dropDuplicates(["symbol", "datetime"])

    # If table doesn't exist yet, create it
    spark = microbatch_df.sparkSession
    if not spark.catalog.tableExists(target_table):
        (
            microbatch_df.write.format("delta")
            .mode("overwrite")  # first creation only
            .saveAsTable(target_table)
        )
        return

    # Merge into existing Delta table
    delta_table = DeltaTable.forName(spark, target_table)

    (
        delta_table.alias("t")
        .merge(microbatch_df.alias("s"), "t.symbol = s.symbol AND t.datetime = s.datetime")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def run_autoloader_to_delta(
    spark: SparkSession,
    source_path: str,
    schema_loc: str,
    checkpoint: str,
    target_table: str,
):
    raw = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")  # JSONL is fine here
        .option("cloudFiles.schemaLocation", schema_loc)
        # optional useful to know: 
        # .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        # .option("cloudFiles.inferColumnTypes", "true")  # default is false, which means all columns inferred as string
        .load(source_path)
    )

    parsed = parse_airbyte_payload(raw)

    query = (
        parsed.writeStream.foreachBatch(lambda df, bid: upsert_to_delta(df, bid, target_table))
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .outputMode("update")
        .start()
    )

    query.awaitTermination()
    return query
