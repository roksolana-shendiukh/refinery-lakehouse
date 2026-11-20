"""Gold keys: surrogate keys and the special members every dimension carries."""

from functools import reduce

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

UNKNOWN = -1
NOT_APPLICABLE = -2
NOT_YET = -3

SPECIAL_MEMBERS = [
    (UNKNOWN, "Unknown"),
    (NOT_APPLICABLE, "Not applicable"),
    (NOT_YET, "Not yet"),
]


def with_special_members(dim: DataFrame, sk_col: str) -> DataFrame:
    """Add the -1, -2 and -3 rows: the key column gets the number, text columns the label, others NULL."""
    members = []
    for sk, label in SPECIAL_MEMBERS:
        values = []
        for field in dim.schema.fields:
            if field.name == sk_col:
                value = F.lit(sk)
            elif isinstance(field.dataType, StringType):
                value = F.lit(label)
            else:
                value = F.lit(None)
            values.append(value.cast(field.dataType).alias(field.name))
        members.append(dim.sparkSession.range(1).select(*values))
    return reduce(DataFrame.unionByName, members, dim)


def hashed_sk(business_key: Column) -> Column:
    return F.xxhash64(business_key)
