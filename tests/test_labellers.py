from datetime import date

from pyspark.sql.types import (
    BooleanType,
    DateType,
    StringType,
    StructType,
)

from exercises.c_labellers.dates import label_weekend
from tests.comparers import assert_frames_functionally_equivalent


def test_label_weekend(spark):
    # Tests almost always follow the arrange-act-assert pattern.
    # ARRANGE
    ...

    # ACT
    ...

    # ASSERT
    #assert_frames_functionally_equivalent(result, expected)
