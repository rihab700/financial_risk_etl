from pyspark.sql.window import Window
from pyspark.sql.functions import col,lag,log,desc
from pyspark.sql.types import DoubleType
from pyspark.sql import functions as F
from delta.tables import DeltaTable

from pyspark.sql import SparkSession, DataFrame

def compute_returns(df:DataFrame, spark: SparkSession, silver_table : str) :
    window_spec = Window.partitionBy('symbol').orderBy(desc('datetime'))
    df_return =( df
                .withColumn('prev_close', lag('close',-1).over(window_spec))
                .withColumn('return', log(col('close') /col('prev_close')).cast(DoubleType()))
                 .select(
                    "symbol",
                    F.to_date("datetime").alias("date"),
                    "close",
                    "prev_close",
                    "return",
                    "volume",
                )
                .filter(col("prev_close").isNotNull())
            )
    max_date_row = (
    df_return
    .select(F.max(F.to_date("date")).alias("max_date"))
    .collect()[0]
    )
    max_available_date = max_date_row["max_date"]
    if not spark.catalog.tableExists(silver_table):
        (df_return.write.format("delta").mode("overwrite").saveAsTable(silver_table))
        return max_available_date

    # Upsert (merge) to avoid duplicates
    tgt = DeltaTable.forName(spark, silver_table)

    (tgt.alias("t")
        .merge(
            df_return.alias("s"),
            "t.symbol = s.symbol AND t.date = s.date"
        )
        .whenMatchedUpdate(set={
            "close": "s.close",
            "prev_close": "s.prev_close",
            "return": "s.return",
            "volume": "s.volume"
        })
        .whenNotMatchedInsertAll()
        .execute()
    )


    return max_available_date