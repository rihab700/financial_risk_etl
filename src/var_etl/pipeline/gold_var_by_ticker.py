from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, desc, row_number, lit
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, IntegerType, DoubleType
from delta.tables import DeltaTable


def compute_var_by_ticker(
    df: DataFrame,
    spark: SparkSession,
    as_of_date,          # python date or "YYYY-MM-DD"
    look_back: int,
    alpha: float,        # confidence level, e.g. 0.95
    gold_var_table: str
) -> DataFrame:

    look_back = int(look_back)
    alpha = float(alpha)

    as_of_date_col = lit(as_of_date).cast(DateType())

    w = Window.partitionBy("symbol").orderBy(desc("date"))

    df_filtered = (
        df
        # safer: compare by DATE not TIMESTAMP midnight edge cases
        .filter(F.to_date("date") <= as_of_date_col)
        .withColumn("rn", row_number().over(w))
        .filter(col("rn") <= look_back)
    )

    df_var_results = (
        df_filtered
        .groupBy("symbol")
        .agg(F.expr(f"percentile_approx(return, {1 - alpha})").alias("var"))
        .withColumn("as_of_date", as_of_date_col)
        .withColumn("look_back", lit(look_back).cast(IntegerType()))
        .withColumn("alpha", lit(alpha).cast(DoubleType()))
        .withColumn("method", lit("historical"))
    )

    # First run: create table
    if not spark.catalog.tableExists(gold_var_table):
        (df_var_results.write.format("delta").mode("overwrite").saveAsTable(gold_var_table))
        return df_var_results

    # Upsert (merge) to avoid duplicates
    tgt = DeltaTable.forName(spark, gold_var_table)

    (tgt.alias("t")
        .merge(
            df_var_results.alias("s"),
            """
            t.symbol = s.symbol
            AND t.as_of_date = s.as_of_date
            AND t.look_back = s.look_back
            AND t.alpha = s.alpha
            AND t.method = s.method
            """
        )
        .whenMatchedUpdate(set={
            "var": "s.var"
        })
        .whenNotMatchedInsertAll()
        .execute()
    )

    return df_var_results