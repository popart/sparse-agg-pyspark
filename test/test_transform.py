from datetime import datetime, date
from pyspark.sql import SparkSession, Row
import pyspark.pandas as ps
import pandas as pd
import pytest

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.getOrCreate()

@pytest.fixture()
def raw_data(spark):
    # assume raw data is partitioned by "date"
    return spark.createDataFrame([
        Row(d=date(2021, 1, 1), ticker=7203, quantity=10, user_id=1),
        Row(d=date(2021, 1, 1), ticker=3382, quantity=30, user_id=1),
        Row(d=date(2021, 1, 3), ticker=3382, quantity=65, user_id=2),
        Row(d=date(2021, 1, 4), ticker=7203, quantity=20, user_id=1),
        Row(d=date(2021, 1, 9), ticker=3382, quantity=75, user_id=2),
        Row(d=date(2021, 1, 10), ticker=7203, quantity=30, user_id=1),
        Row(d=date(2021, 1, 10), ticker=3382, quantity=40, user_id=1),
        Row(d=date(2021, 1, 19), ticker=3382, quantity=25, user_id=2),
    ], schema='d date, ticker long, quantity long, user_id long')

def test_split(raw_data):
    df = raw_data
    ticker = 3382

    # should be a way to avoid collecting into a python list
    # using map functions e.g.
    user_ids = df.select(df.user_id).distinct().rdd.flatMap(lambda x: x).collect()
    print(user_ids)

    # slower, could try to use groupBy(df.user_id).agg(my_pandas_udf)
    # https://stackoverflow.com/questions/40006395/applying-udfs-on-groupeddata-in-pyspark-with-functioning-python-example/47497815#47497815
    for user_id in user_ids:
        print(user_id)
        user_tx_df = df.filter((df.user_id == user_id) & (df.ticker == ticker))
        user_tx_df.show()

def test_ffill(spark, raw_data):
    df = raw_data
    user_id = 1
    ticker = 3382

    start_date = date(2021, 1, 1)
    end_date = date(2021, 1, 31)

    #date_range = pd.date_range(start=start_date, end=end_date).to_frame(index=False, name="d")
    #full_dates_df = spark.createDataFrame(date_range)
    full_dates = ps.date_range(start_date, end_date)
    print(full_dates)


    user_tx_df = df.filter((df.user_id == user_id) & (df.ticker == ticker))

    psdf = user_tx_df.pandas_api(index_col="d")

    psdf = psdf.reindex(full_dates).sort_index()

    psdf.to_spark(index_col="d").show()


