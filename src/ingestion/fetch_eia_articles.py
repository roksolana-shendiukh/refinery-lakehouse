"""Lakeflow job task: land Today in Energy articles and the current STEO pages for the RAG assistant."""

import argparse
import logging
from datetime import UTC, datetime

from refinery.rag.eia_site import EiaClient
from refinery.rag.ingest import run_fetch

logger = logging.getLogger("refinery.ingestion.fetch_eia_articles")


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--landing-path", required=True)
    return parser.parse_args()


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        force=True,
    )
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    args = parse_args()
    load_id, counts = run_fetch(EiaClient(), args.landing_path, datetime.now(UTC))

    logger.info("RAG load %s finished: %s", load_id, counts)


if __name__ == "__main__":
    main()
