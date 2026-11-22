"""Job task: append the metrics of the last silver run to the history table."""

import argparse

from pyspark.sql import SparkSession

from refinery.silver.metrics import new_runs


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--latest", required=True, help="Table with the metrics of the last run")
    parser.add_argument("--history", required=True, help="Table with the metrics of all runs")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    latest = spark.read.table(args.latest)
    if spark.catalog.tableExists(args.history):
        latest = new_runs(latest, spark.read.table(args.history))
    latest.write.mode("append").saveAsTable(args.history)


if __name__ == "__main__":
    main()
