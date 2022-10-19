import pyspark.sql.functions as sf
from pyspark.sql import DataFrame
from pyspark.sql.functions import when


def label_weekend(
    frame: DataFrame, colname: str = "date", new_colname: str = "is_weekend"
) -> DataFrame:
    df = frame.withColumn(col, sf.to_date(sf.col(col),"MM-dd-yyyy"))
    df = df.withColumn("day_of_the_week",sf.dayofweek(sf.col(col)))
    df = df.withColumn(new_colname,when((df.day_of_the_week == 1) | (df.day_of_the_week == 7),1).otherwise(0))
    df = df.drop('day_of_the_week')
    return df
