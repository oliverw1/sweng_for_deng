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
    import pyspark.sql.functions as sf
    df = spark.createDataFrame([('2022-10-15',True),('2022-10-16',True),('2022-10-18',False)], ['dt','is_weekend'])
    df.show()
    # ACT
    ...
    result = label_weekend(df.select('dt'), 'dt')
    expected = df

    # ASSERT
    assert_frames_functionally_equivalent(result, expected, False)
