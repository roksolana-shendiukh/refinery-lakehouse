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
_KNOWN = "_known_bk"


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


def lookup_sk(fact: DataFrame, reference: DataFrame, bk_col: str, reference_bk_col: str, sk_col: str) -> DataFrame:
    known = reference.select(F.col(reference_bk_col).alias(_KNOWN)).distinct()
    joined = fact.join(F.broadcast(known), F.col(bk_col) == F.col(_KNOWN), "left")
    key = F.when(F.col(_KNOWN).isNull(), F.lit(UNKNOWN).cast("bigint")).otherwise(hashed_sk(F.col(bk_col)))
    return joined.withColumn(sk_col, key).drop(_KNOWN)
