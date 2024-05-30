from datetime import date
from pyspark.sql import SparkSession
import pytest

from src.report import (
    ffill_per_user, reduce_to_per_user, split_to_daily_data, sum_daily_data
)

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.getOrCreate()

def test_reduce_to_per_user(spark):
    data = [
        (1, date(2021, 1, 1), 7203, 10),
        (1, date(2021, 1, 1), 3382, 30),
        (2, date(2021, 1, 3), 3382, 65),
        (1, date(2021, 1, 4), 7203, 20),
        (2, date(2021, 1, 9), 3382, 75),
        (1, date(2021, 1, 10), 7203, 30),
        (1, date(2021, 1, 10), 3382, 40),
        (2, date(2021, 1, 19), 3382, 2),
    ]
    rdd = spark.sparkContext.parallelize(data)
    ticker = 3382

    rdd = reduce_to_per_user(rdd, 3382)

    assert list(rdd.collect()) == [
        (1, [
            (date(2021, 1, 1), 30),
            (date(2021, 1, 10), 40)
        ]),
        (2, [
            (date(2021, 1, 3), 65),
            (date(2021, 1, 9), 75),
            (date(2021, 1, 19), 2)
        ])
    ]

def test_ffill(spark):
    # from test_reduce_by_user
    data = [
        (1, [
            (date(2021, 1, 1), 30),
            (date(2021, 1, 10), 40)
        ]),
        (2, [
            (date(2021, 1, 3), 65),
            (date(2021, 1, 9), 75),
            (date(2021, 1, 19), 2)
        ])
    ]
    rdd = spark.sparkContext.parallelize(data)

    rdd = ffill_per_user(
        rdd=rdd,
        start_date=date(2021,1,1),
        end_date=date(2021,1,31)
    )

    res = list(rdd.collect())
    assert len(res) == 2
    assert len(res[0][1]) == 31
    assert len(res[1][1]) == 29 
    assert [r[2] for r in res[0][1]] == (
        [0] + [30]*8 + [0] + [40]*21
    )

def test_split_to_daily_data(spark):
    data = [
        (1, [
            (date(2021, 1, 1), 30, 0),
            (date(2021, 1, 2), 0, 30),
            (date(2021, 1, 3), 0, 30),
            (date(2021, 1, 4), 0, 30),
            (date(2021, 1, 5), 10, 0),
        ]),
        (2, [
            (date(2021, 1, 2), 5, 0),
            (date(2021, 1, 3), 0, 5),
            (date(2021, 1, 4), 11, 0),
            (date(2021, 1, 5), 0, 11),
        ])
    ]
    rdd = spark.sparkContext.parallelize(data)

    rdd = split_to_daily_data(rdd)

    res = list(rdd.collect())
    assert res == [
        (date(2021, 1, 1), [30, 0, 1]),
        (date(2021, 1, 2), [0, 30, 1]),
        (date(2021, 1, 3), [0, 30, 1]),
        (date(2021, 1, 4), [0, 30, 1]),
        (date(2021, 1, 5), [10, 0, 1]),

        (date(2021, 1, 2), [5, 0, 1]),
        (date(2021, 1, 3), [0, 5, 1]),
        (date(2021, 1, 4), [11, 0, 1]),
        (date(2021, 1, 5), [0, 11, 1]),
    ]

def test_sum_daily_data(spark):
    data = [
        # user 1
        (date(2021, 1, 1), [30, 0, 1]),
        (date(2021, 1, 2), [0, 30, 1]),
        (date(2021, 1, 3), [0, 30, 1]),
        (date(2021, 1, 4), [0, 30, 1]),
        (date(2021, 1, 5), [10, 0, 1]),

        # user 2
        (date(2021, 1, 2), [5, 0, 1]),
        (date(2021, 1, 3), [0, 5, 1]),
        (date(2021, 1, 4), [11, 0, 1]),
        (date(2021, 1, 5), [0, 11, 1]),

        # user 3
        (date(2021, 1, 3), [7, 0, 1]),
        (date(2021, 1, 4), [0, 7, 1]),
        (date(2021, 1, 5), [0, 7, 1]),
    ]
    rdd = spark.sparkContext.parallelize(data)

    rdd = sum_daily_data(rdd)

    res = list(rdd.collect())

    assert res == [
        (date(2021, 1, 1), [30, 0, 1]),
        (date(2021, 1, 2), [5, 30, 2]),
        (date(2021, 1, 3), [7, 35, 3]),
        (date(2021, 1, 4), [11, 37, 3]),
        (date(2021, 1, 5), [10, 18, 3]),
    ]

