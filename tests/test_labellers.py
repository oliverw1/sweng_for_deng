from datetime import date

from pyspark.sql.types import BooleanType, DateType, StringType, StructType

from exercises.c_labellers.dates import label_weekend
from tests.comparers import assert_frames_functionally_equivalent


def test_label_weekend(spark):
    # Tests almost always follow the arrange-act-assert pattern.
    # ARRANGE
    ...
    expected = spark.createDataFrame([("2022-10-19", "False")], ["date", "is_weekend"])
    test_frame.drop("is_weekend")
    # ACT
    result = label_weekend(test_frame, "date", "is_weekend")
    # ASSERT
    assert_frames_functionally_equivalent(result, expected)
