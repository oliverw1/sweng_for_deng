import pyspark.sql.functions as sf
import spark
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("Python Spark SQL basic example").getOrCreate()


def label_weekend(
    frame: DataFrame, colname: str = "date", new_colname: str = "is_weekend"
) -> DataFrame:
    def is_weekend(date_col):
        # return sf.dayofweek(sf.to_date(sf.col(date_col), format='yyyy-MM-dd')).isin([1,7]).cast("str")
        return sf.dayofweek(sf.col(date_col)).isin([1, 7]).cast("str")

    frame = frame.withColumn(new_colname, is_weekend(colname))

    return frame


import pandas as pd

test_frame_pd = pd.DataFrame({"date": ["2022-10-19"]}).astype(str)
test_frame = spark.createDataFrame([("2022-10-19",)], ["date"], StringType())
test_frame.printSchema()
label_weekend(test_frame).show()
