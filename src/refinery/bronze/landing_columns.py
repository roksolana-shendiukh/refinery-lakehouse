"""Bronze shape of the landed EIA records, as agreed in the STM (sheet "Bronze tables")."""

from pyspark.sql import functions as F

from refinery.bronze.eia import DATASETS

EIA_FIELDS = {
    "period": "period",
    "duoarea": "duoarea",
    "area-name": "area_name",
    "product": "product",
    "product-name": "product_name",
    "process": "process",
    "series": "series",
    "value": "value",
    "units": "units",
}


def route_of(dataset):
    """Short EIA route of a dataset, as the STM names it: cons/wpsup, pri/gnd, pri/spt."""
    return DATASETS[dataset].route.removeprefix("petroleum/")


def route_column():
    """route, read from the landing folder in source_file."""
    route = F.lit(None).cast("string")
    for dataset, spec in DATASETS.items():
        in_folder = F.col("source_file").contains(f"/eia/{spec.folder}/")
        route = F.when(in_folder, route_of(dataset)).otherwise(route)
    return route


def to_bronze(df):
    """Landed EIA records in the bronze columns: EIA values untouched, plus where and when."""
    ingested_at = F.current_timestamp()
    return df.select(
        route_column().alias("route"),
        *[F.col(f"`{field}`").alias(column) for field, column in EIA_FIELDS.items()],
        "source_file",
        ingested_at.alias("ingested_at"),
        "_rescued_data",
        F.to_date(ingested_at).alias("load_date"),
    )
