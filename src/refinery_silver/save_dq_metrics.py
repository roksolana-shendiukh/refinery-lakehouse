"""Job task: append the metrics of the last silver run to the history table, which Terraform creates."""

import argparse

from pyspark.sql import SparkSession

from refinery.silver.metrics import new_runs, require_table


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--latest", required=True, help="Table with the metrics of the last run")
    parser.add_argument("--history", required=True, help="Table with the metrics of all runs")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    require_table(spark, args.history)
    runs = new_runs(spark.read.table(args.latest), spark.read.table(args.history))
    runs.writeTo(args.history).append()


if __name__ == "__main__":
    main()
