from pyspark.sql import DataFrame


def label_weekend(
    frame: DataFrame, colname: str = "date", new_colname: str = "is_weekend"
) -> DataFrame:
    frame.withColumn(
        new_colname,
        sf.when(sf.dayofweek(colname) in [1, 7], sf.lit(True).otherwise(sf.lit(False))),
    )
    return frame
