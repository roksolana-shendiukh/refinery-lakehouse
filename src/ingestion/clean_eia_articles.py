"""Lakeflow job task: parse and check the landed EIA pages into silver eia_articles, the rest into quarantine."""

import argparse
import logging

from pyspark.sql import SparkSession

from refinery.rag.silver import run_clean

logger = logging.getLogger("refinery.ingestion.clean_eia_articles")


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bronze-table", required=True)
    parser.add_argument("--silver-table", required=True)
    parser.add_argument("--quarantine-table", required=True)
    return parser.parse_args()


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        force=True,
    )
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()
    counts = run_clean(spark, args.bronze_table, args.silver_table, args.quarantine_table)

    logger.info("Cleaned EIA pages: %s", counts)


if __name__ == "__main__":
    main()
