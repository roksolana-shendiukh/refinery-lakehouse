"""Lakeflow job task: load weekly EIA supply and prices into the landing volume."""

import argparse
import logging
from datetime import UTC, datetime

from databricks.sdk.runtime import dbutils

from refinery.bronze.eia import run_load
from refinery.bronze.eia_client import EiaClient
from refinery.bronze.reference import land_reference

logger = logging.getLogger("refinery.ingestion.fetch_eia")


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--landing-path", required=True)
    parser.add_argument("--reference-path", required=True)
    parser.add_argument("--secret-scope", required=True)
    parser.add_argument("--api-key-name", default="refinery-eia-api-key")
    return parser.parse_args()


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        force=True,
    )
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    args = parse_args()
    api_key = dbutils.secrets.get(scope=args.secret_scope, key=args.api_key_name)
    client = EiaClient(api_key)

    landed = land_reference(args.reference_path, args.landing_path)
    logger.info("Reference files landed: %s", landed or "none changed")

    load_id, counts = run_load(client, args.landing_path, args.reference_path, datetime.now(UTC))

    logger.info("EIA load %s finished: %s", load_id, counts)


if __name__ == "__main__":
    main()
