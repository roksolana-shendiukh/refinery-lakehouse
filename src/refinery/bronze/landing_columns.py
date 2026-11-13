"""Bronze shape of the landed EIA records, as agreed in the STM (sheet "Bronze tables")."""

from pyspark.sql import functions as F

from refinery.bronze.eia import DATASETS
from refinery.bronze.reference import REFERENCE_COLUMNS

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


def with_landing_metadata(df, data_columns):
    """Data columns first, then the metadata every bronze table has (STM): where and when."""
    ingested_at = F.current_timestamp()
    return df.select(
        *data_columns,
        "source_file",
        ingested_at.alias("ingested_at"),
        "_rescued_data",
        F.to_date(ingested_at).alias("load_date"),
    )


def to_bronze(df):
    """Landed EIA records in the bronze columns: EIA values untouched, plus route and metadata."""
    eia_columns = [F.col(f"`{field}`").alias(column) for field, column in EIA_FIELDS.items()]
    return with_landing_metadata(df, [route_column().alias("route"), *eia_columns])


def to_reference_bronze(df, name):
    """Landed reference CSV rows in the bronze columns: CSV values untouched, plus metadata."""
    return with_landing_metadata(df, REFERENCE_COLUMNS[name])
