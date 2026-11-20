"""Lakeflow job task: split the silver EIA pages into overlapping chunks for the vector search index."""

import argparse
import logging

from pyspark.sql import SparkSession

from refinery.rag.chunking import run_chunk

logger = logging.getLogger("refinery.ingestion.chunk_eia_articles")


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--silver-table", required=True)
    parser.add_argument("--chunks-table", required=True)
    return parser.parse_args()


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        force=True,
    )
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()
    counts = run_chunk(spark, args.silver_table, args.chunks_table)

    logger.info("Chunked EIA pages: %s", counts)


if __name__ == "__main__":
    main()
