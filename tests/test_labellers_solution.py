from datetime import date

from pyspark.sql.types import BooleanType, DateType, StringType, StructType

from exercises.c_labellers.dates_solution import label_weekend
from tests.comparers import assert_frames_functionally_equivalent


def test_label_weekend(spark):
    # Tests almost always follow the arrange-act-assert pattern.
    # ARRANGE
    expected = spark.createDataFrame(
        data=[
            (date(2018, 5, 12), "a", True),
            (date(2019, 5, 13), "b", False),
        ],
        schema=(
            StructType()
            .add("date", DateType())
            .add("foo", StringType())
            .add("is_weekend", BooleanType())
        ),
    )
    frame_in = expected.select("date", "foo")  # Easy, no?

    # ACT
    result = label_weekend(frame_in)

    # ASSERT
    assert_frames_functionally_equivalent(result, expected)
