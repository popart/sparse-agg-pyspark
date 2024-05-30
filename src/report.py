from datetime import date, timedelta
from pyspark.sql import SparkSession
from pyspark import RDD
import pytest

spark = SparkSession.builder.getOrCreate()

def reduce_to_per_user(rdd: RDD, ticker: int) -> RDD:
    """Converts raw rows into one row per user.

    Each user row is in the form of (user_id, data[]).
    """
    # filter by the ticker and then drop it
    rdd = rdd.filter(lambda x: x[2] == ticker)
    rdd = rdd.map(lambda x: (x[0], [(x[1], x[3])]))

    def merge(list1: list[tuple], list2: list[tuple]) -> list[tuple]:
        return list1 + list2

    return rdd.reduceByKey(merge)


def ffill_per_user(rdd: RDD, start_date: date, end_date: date) -> RDD:
    """Forward-fills missing dates for rdd with rows per user (user_id, data[])

    Adds inferred_qty
    """
    def gen_ffill_fn(start_date, end_date):
        def ffill(user_row: tuple) -> tuple:
            """assumes user_row is sorted by date"""
            user_id: int = user_row[0]
            user_data: list[tuple] = user_row[1].copy()

            res = []
            current_date = start_date
            inferred_qty = 0
            while (current_date <= end_date):
                if user_data and user_data[0][0] == current_date:
                    res.append((
                        current_date,    # date
                        user_data[0][1], # qty
                        0,               # inferred_qty
                    ))
                    inferred_qty = user_data[0][1]
                    user_data.pop(0)
                else:
                    res.append((
                        current_date,    # date
                        0,               # qty
                        inferred_qty,
                    ))
                current_date += timedelta(days=1)
            return (user_id, res)
        return ffill

    ffill = gen_ffill_fn(date(2021, 1, 1), date(2021, 1, 31))
    # sort just in case
    rdd = rdd.map(lambda row: (row[0], sorted(row[1])))

    return rdd.map(ffill)

def split_to_daily_data(rdd: RDD) -> RDD:
    """Removes the user_id from data, returns just the daily rows

    Also adds num_users value
    Result is keyed by date: (date, data[])
    """
    rdd = rdd.flatMap(lambda row: row[1]) # all the tuples
    return rdd.map(lambda row: (
        row[0], # date
        [
            row[1],  # qty
            row[2],  # inferred_qty
            1,       # num_users
        ]
    ))

def sum_daily_data(rdd: RDD) -> RDD:
    def sum_dates(r1: list[int], r2: list[int]) -> list[int]:
        return [
            r1[0] + r2[0], # qty
            r1[1] + r2[1], # inferred_qty
            r1[2] + r2[2], # num_users
        ]
    return rdd.reduceByKey(sum_dates).sortByKey()

def run_report(rdd: RDD, ticker: int, start_date: date, end_date: date) -> RDD:
    rdd = reduce_to_per_user(rdd, ticker)
    rdd = ffill_per_user(rdd, start_date, end_date)
    rdd = split_to_daily_data(rdd)
    rdd = sum_daily_data(rdd)

    return rdd

if __name__ == "__main__":
    # data for 3 users
    data = [
        (1, date(2021, 1, 1), 7203, 10),
        (1, date(2021, 1, 1), 3382, 30),
        (2, date(2021, 1, 3), 3382, 65),
        (1, date(2021, 1, 4), 7203, 20),
        (2, date(2021, 1, 9), 3382, 75),
        (1, date(2021, 1, 10), 7203, 30),
        (1, date(2021, 1, 10), 3382, 40),
        (3, date(2021, 1, 15), 3382, 7),
        (2, date(2021, 1, 19), 3382, 2),
    ]
    rdd = spark.sparkContext.parallelize(data)
    ticker = 3382
    report = run_report(
        rdd=rdd,
        ticker=ticker,
        start_date=date(2021,1,1),
        end_date=date(2021,1,31),
    )

    for row in list(report.collect()):
        print(row)
