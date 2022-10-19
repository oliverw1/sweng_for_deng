from pyspark.sql import DataFrame
from pyspark.sql.functions import col, dayofweek


def label_weekend(
    frame: DataFrame, colname: str = "date", new_colname: str = "is_weekend"
) -> DataFrame:
    # The line below is one solution. There is a more performant version of it
    # too, involving the modulo operator, but it is more complex. The
    # performance gain might not outweigh the cost of a programmer trying to
    # understand that arithmetic.

    # Keep in mind, that we're using "magic numbers" here too: unless you know
    # the API by heart, you're better off defining "SUNDAY = 1" and
    # "SATURDAY = 7" and using those identifiers in the call to the `isin`
    # method.

    # Finally, programming languages and even frameworks within the same
    # programming language tend to differ in the convention whether Monday is
    # day 1 or not. You should always check the documentation corresponding to
    # your library.
    return frame.withColumn(new_colname, dayofweek(col(colname)).isin(1, 7))
