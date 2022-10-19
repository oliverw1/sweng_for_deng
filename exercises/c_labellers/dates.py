from pyspark.sql import DataFrame


def label_weekend(
    frame: DataFrame, colname: str = "date", new_colname: str = "is_weekend"
) -> DataFrame:
    import pyspark.sql.functions as sf
    frame = frame.withColumn(new_colname, sf.when(sf.dayofweek(sf.col(colname)).isin([7,1]), sf.lit(True)).otherwise(sf.lit(False)))
    frame.show()
    return frame
