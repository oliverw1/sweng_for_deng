from datetime import date

import pyspark.sql.functions as sf
from pyspark.sql.types import BooleanType, DateType, StringType, StructType

from exercises.c_labellers.dates import label_weekend
from tests.comparers import assert_frames_functionally_equivalent


def test_label_weekend(spark):
    # Tests almost always follow the arrange-act-assert pattern.
    # ARRANGE
    colnames = ["date", "is_weekend"]

    data = [
        (date(2022, 10, 19), False),
        (date(2022, 10, 20), False),
        (date(2022, 10, 21), False),
        (date(2022, 10, 22), True),
        (date(2022, 10, 23), True),
    ]

    data2 = [
        (None, None),
        (date(2022, 10, 20), False),
        (date(2022, 10, 21), False),
        (date(2022, 10, 22), True),
        (date(2022, 10, 23), True),
    ]

    df_test = spark.createDataFrame(data, colnames)
    df_test2 = spark.createDataFrame(data, colnames)

    # ACT
    result = label_weekend(df_test.select("date"), "date", "is_weekend")
    result.printSchema()
    result.show()

    expected = df_test
    expected.printSchema()
    expected.show()

    # ASSERT
    assert_frames_functionally_equivalent(result, expected)
