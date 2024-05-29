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
