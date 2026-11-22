"""Lakeflow job task: start, sync or stop vector search for the RAG assistant.

start: create the endpoint and the index if they are missing, then sync the index with the chunks table.
sync: sync the index after refinery-rag-index changed the chunks, if the index exists.
stop: delete the index and the endpoint, so vector search costs nothing outside demos.
"""

import argparse
import logging

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

from refinery.rag.search_index import (
    EMBEDDING_MODEL,
    ENDPOINT_NAME,
    SearchConfig,
    refresh_search,
    start_search,
    stop_search,
)

logger = logging.getLogger("refinery.ingestion.vector_search")


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["start", "sync", "stop"])
    parser.add_argument("--chunks-table", required=True)
    parser.add_argument("--index-name", required=True)
    parser.add_argument("--endpoint-name", default=ENDPOINT_NAME)
    parser.add_argument("--embedding-model", default=EMBEDDING_MODEL)
    return parser.parse_args()


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        force=True,
    )
    args = parse_args()
    client = WorkspaceClient()
    config = SearchConfig(
        endpoint_name=args.endpoint_name,
        index_name=args.index_name,
        source_table=args.chunks_table,
        embedding_model=args.embedding_model,
    )

    if args.action == "stop":
        result = stop_search(client, config)
    else:
        # The sync is done when the index holds as many rows as the chunks table
        chunk_rows = SparkSession.builder.getOrCreate().table(args.chunks_table).count()
        run = start_search if args.action == "start" else refresh_search
        result = run(client, config, chunk_rows)

    logger.info("Vector search %s: %s", args.action, result)


if __name__ == "__main__":
    main()
