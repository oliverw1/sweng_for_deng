# Exercise:
# “clean” a CSV file using PySpark.
# * Grab sample data from https://packages.revolutionanalytics.com/datasets/AirlineSubsetCsv.tar.gz
#   A copy of the data can be found on our S3 bucket (link shared in class).
# * Inspect the columns: what type of data do they hold?
# * Create an ETL job with PySpark where you read in the csv file, and perform
#   the cleansing steps mentioned in the classroom:
#   - improve column names (subjective) - withColumnRenamed or select(col("foo").alias("bar"))
#   - Assign the right data types - col("foo").cast(IntegerType()) or various functions, like to_date
#   - flag missing or unknown data
#   - remove redundant data  - drop() or select()
# For explanations on the columns, check https://www.transtats.bts.gov/Fields.asp?gnoyr_VQ=FGK
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as sf

from pyspark.sql.types import IntegerType, StringType, DateType,DoubleType,StructField,StructType

flights_schema = StructType(
    [
        StructField("YEAR", IntegerType(), True),
        StructField("MONTH", IntegerType(), True),
        StructField("DAY_OF_MONTH", IntegerType(), True),
        StructField("DAY_OF_WEEK", IntegerType(), True),
        StructField("FL_DATE", DateType(), True),
        StructField("UNIQUE_CARRIER", StringType(), True),
        StructField("TAIL_NUM", StringType(), True),
        StructField("FL_NUM", IntegerType(), True),
        StructField("ORIGIN_AIRPORT_ID", StringType(), True),
        StructField("ORIGIN", StringType(), True),
        StructField("ORIGIN_STATE_ABR", StringType(), True),
        StructField("DEST_AIRPORT_ID", IntegerType(), True),
        StructField("DEST", StringType(), True),
        StructField("DEST_STATE_ABR", StringType(), True),
        StructField("CRS_DEP_TIME", StringType(), True),
        StructField("DEP_TIME", StringType(), True),
        StructField("DEP_DELAY", DoubleType(), True),
        StructField("DEP_DELAY_NEW", DoubleType(), True),
        StructField("DEP_DEL15", DoubleType(), True),
        StructField("DEP_DELAY_GROUP", IntegerType(), True),
        StructField("TAXI_OUT", StringType(), True),
        StructField("WHEELS_OFF", StringType(), True),
        StructField("WHEELS_ON", StringType(), True),
        StructField("TAXI_IN", StringType(), True),
        StructField("CRS_ARR_TIME", StringType(), True),
        StructField("ARR_TIME", StringType(), True),
        StructField("ARR_DELAY", DoubleType(), True),
        StructField("ARR_DELAY_NEW", DoubleType(), True),
        StructField("ARR_DEL15", DoubleType(), True),
        StructField("ARR_DELAY_GROUP", IntegerType(), True),
        StructField("CANCELLED", DoubleType(), True),
        StructField("CANCELLATION_CODE", StringType(), True),
        StructField("DIVERTED", DoubleType(), True),
        StructField("CRS_ELAPSED_TIME", DoubleType(), True),
        StructField("ACTUAL_ELAPSED_TIME", DoubleType(), True),
        StructField("AIR_TIME", DoubleType(), True),
        StructField("FLIGHTS", DoubleType(), True),
        StructField("DISTANCE", DoubleType(), True),
        StructField("DISTANCE_GROUP", DoubleType(), True),
        StructField("CARRIER_DELAY", DoubleType(), True),
        StructField("WEATHER_DELAY", DoubleType(), True),
        StructField("NAS_DELAY", DoubleType(), True),
        StructField("SECURITY_DELAY", DoubleType(), True),
        StructField("LATE_AIRCRAFT_DELAY", DoubleType(), True),
        StructField("_c44", StringType(), True),
    ]
)


def read_data(path: Path):
    spark = SparkSession.builder.getOrCreate()
    return spark.read.csv(
        str(path),
        # For a CSV, `inferSchema=False` means every column stays of the string
        # type. There is no time wasted on inferring the schema, which is
        # arguably not something you would depend on in production either.
        schema=flights_schema,
        header=True,
        # The dataset mixes two values for null: sometimes there's an empty attribute,
        # which you will see in the CSV file as two neighboring commas. But there are
        # also literal "null" strings, like in this sample: `420.0,null,,1.0,`
        # The following option makes a literal null-string equivalent to the empty value.
        nullValue="null",
    )


from pyspark.sql.types import IntegerType, StringType, DateType

# flights_schema =
obsolete_columns = ['YEAR','MONTH','DAY_OF_MONTH','DAY_OF_WEEK','_c44']

def clean(frame: DataFrame) -> DataFrame:
    frame = frame.drop(*obsolete_columns)
    frame = frame.withColumn('CRS_DEP_TIME',sf.to_timestamp(
                                                    sf.concat(
                                                        sf.col('FL_DATE'),
                                                        sf.lpad(sf.col('CRS_DEP_TIME'),4,'0')),'yyyy-MM-ddHHmm'))
    return frame


if __name__ == "__main__":
    # Use relative paths, so that the location of this project on your system
    # won't mean editing paths.
    path_to_exercises = Path(__file__).parents[1]
    resources_dir = path_to_exercises / "resources"
    target_dir = path_to_exercises / "target"
    # Create the folder where the results of this script's ETL-pipeline will
    # be stored.
    target_dir.mkdir(exist_ok=True)

    # Extract
    frame = read_data(resources_dir / "flights")
    print(frame.schema)
    # Transform
    cleaned_frame = clean(frame)
    # Load
    cleaned_frame.printSchema()
    cleaned_frame.write.parquet(
        path=str(target_dir / "cleaned_flights"),
        mode="overwrite",
        compression="snappy",
    )
