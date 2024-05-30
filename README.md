# sparse-agg-pyspark

## Dependencies
pyenv (for setting local python version, 3.11)
poetry (https://python-poetry.org/docs/)
Java 8 jre (https://www.java.com/en/download/)
python >=3.8,<3.12
- 3.8 minimum for pyspark
- 3.12 removed distutils which breaks some things
pandas <2.0.0
- because breaking chnges in 2.0.0 don't work w/ psypark.pandas

## Setup
poetry install --with dev
poetry run pytest

## Notes
Report is per ticker, so first step is to filter raw_data by ticker,
which gives us `[date, user_id, qty]`

For each row, we only know if the qty is increasing or decreasing the total if we know the user's previous holding qty.

The naive thing to do is sort the rows by date, and then foldLeft a dict of `{user_id: qty}`.

Maybe this dict doesn't fit into memory (it probably does, 10B/user, 100M users => <1000MB)
But the problem is you have to start at the beginning of the data and iterate through it sequentially, like building up state from an event log.

So a method that works on partitions of the data would be better... since we have a spark cluster...

We can partition the data per user. Data for one user & one ticker should easily fit in mem.
(100 years is only 36,500 rows)

We need some structure where we can merge 2 users together, and then keep merging that in a map-reduce fashion.

I think we can calculate the deltas for each day. The deltas all sum up together for that day. And then if you have the total deltas for each day it's easy to iterate over them to get the daily totals.

Hmm, but the held values are supposed to be 0 when there's no data measurement. The total held is just the same of the held for each day. The total inferred is the sum of the inferred for that day. Don't actually need to calculate daily deltas.

All that needs calculating is the inferred values. You can just:
- copy the held values to a new col
- forward fill the null values for each row
- subtract the held values.


raw -> filter-by-ticker -> split-by-user-id -> map-fill-inferred-qty -> reduce-sum-cols

## Closing Notes
The qty and inferred numbers look right, but the num_users is equal to the total for all dates.
That's because I filled in the entire date range for each user, instead of just starting from their first day of data.
Actually that's a pretty quick fix...

Other things to improve:
- add schemas/dtypes for data
- maybe use fancier pyspark/spark-pandas functions?
  - i tried at the beginning but seemed like a lot of overhead to learn, vs basic map-reduce which is like 3 functions.
- spark session should be injected, not instantiated whenever anyone loads the module.
- pyright/black checks etc
