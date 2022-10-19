from pyspark.sql import DataFrame
from pyspark.sql.functions import col, dayofweek


def label_weekend(
    frame: DataFrame, colname: str = "date", new_colname: str = "is_weekend"
) -> DataFrame:

    frame = frame.withColumn(new_colname, (dayofweek(col(colname)) - 1) % 6 == 0)
    return frame
