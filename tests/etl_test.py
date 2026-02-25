import json
import math
from pyspark.sql import functions as F

from var_etl.pipeline.bronze_batch import run_local_batch
from var_etl.pipeline.silver_market_returns import compute_returns

def test_parse_airbyte(spark):
    raw = spark.createDataFrame([
        {"_airbyte_raw_id":"019c3c65-b98b-7aa4-8a69-14bb3489c685","_airbyte_extracted_at":1770539823321,"_airbyte_meta":json.dumps({"sync_id":65,"changes":[]}),"_airbyte_generation_id":1,"_airbyte_data":json.dumps({"datetime":"2026-02-06","open":"277.12000","high":"280.91000","low":"276.92999","close":"278.12000","volume":"50420700","symbol":"AAPL"})},
        {"_airbyte_raw_id":"019c3c65-b998-7711-a48f-9ebe4c9d5cad","_airbyte_extracted_at":1770539823322,"_airbyte_meta":json.dumps({"sync_id":65,"changes":[]}),"_airbyte_generation_id":1,"_airbyte_data":json.dumps({"datetime":"2026-02-05","open":"278.13000","high":"279.5","low":"273.23001","close":"275.91000","volume":"52977400","symbol":"AAPL"})}
    ])
    df = run_local_batch(spark,raw)
    rows = df.collect()
    assert df.count() == 2
    assert df.columns == ["symbol", "datetime", "open", "high", "low", "close", "volume"]
    assert rows[0]["symbol"] == "AAPL"

def test_compute_returns(spark):
    df = spark.createDataFrame([
        {"symbol":"AAPL", "datetime":"2026-02-06", "open":277.12000, "high":280.91000, "low":276.92999, "close":278.12000, "volume":50420700},
        {"symbol":"AAPL", "datetime":"2026-02-05", "open":278.13000, "high":279.5, "low":273.23001, "close":275.91000, "volume":52977400},
        {"symbol":"AAPL", "datetime":"2026-02-04", "open":277.14000, "high":280.91000, "low":276.92999, "close":276.12000, "volume":50420700},
        {"symbol":"AAPL", "datetime":"2026-02-03", "open":278.13000, "high":279.5, "low":273.23001, "close":279.91000, "volume":52977400},
        {"symbol":"MSFT", "datetime":"2026-02-06", "open":277.12000, "high":280.91000, "low":276.92999, "close":278.12000, "volume":50420700},
        {"symbol":"MSFT", "datetime":"2026-02-05", "open":278.13000, "high":279.5, "low":273.23001, "close":275.91000, "volume":52977400},
        {"symbol":"MSFT", "datetime":"2026-02-04", "open":277.14000, "high":280.91000, "low":276.92999, "close":276.12000, "volume":50420700}
    ])
    df_return = compute_returns(df)
    row = (
    df_return
    .filter((F.col("symbol") == "AAPL") & (F.col("date") == F.lit("2026-02-06")))
    .orderBy(F.col("close").desc())  # pick the row you expect
    .collect()[0]
)   
     # Check the computed return value with a tolerance for floating-point comparison
     #return value should be log(278.12000 / 275.91000) = 0.007977949648101178
    assert math.isclose(row["return"] , 0.007977949648101178, rel_tol=1e-6)
    assert df_return.filter(F.col("prev_close").isNull()).count() == 0  # Ensure no nulls in prev_close

