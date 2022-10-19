from pyspark.sql import DataFrame
from datetime import datetime


def label_weekend(
    frame: DataFrame, colname: str = "date", new_colname: str = "is_weekend"
) -> DataFrame:


    #frame = frame.withColumn('is_weekend', sf.when( sf.to_date(sf.col("date"),"MM-dd-yyyy").weekday().isin([5,6]) ,True ).otherwise(False))
    frame = frame.withColumn('is_weekend', sf.when( sf.dayofweek(sf.col("date")).isin([5,6]) ,True ).otherwise(False))
    pass
