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
from pyspark.sql.types import *



def distinct_frequency(df,string):
  display(df.groupBy(string).count().withColumnRenamed("count", "Frequency").orderBy(desc("Frequency")))


def read_data(path: Path):
    spark = SparkSession.builder.getOrCreate()
    return spark.read.csv(
        str(path),
        # For a CSV, `inferSchema=False` means every column stays of the string
        # type. There is no time wasted on inferring the schema, which is
        # arguably not something you would depend on in production either.
        inferSchema=True,
        header=True,
        # The dataset mixes two values for null: sometimes there's an empty attribute,
        # which you will see in the CSV file as two neighboring commas. But there are
        # also literal "null" strings, like in this sample: `420.0,null,,1.0,`
        # The following option makes a literal null-string equivalent to the empty value.
        nullValue="null",
    )


def clean(frame: DataFrame) -> DataFrame:
    
    columns_to_drop = ['_c44','YEAR','MONTH','DAY_OF_MONTH']
    processed_frame = frame.drop(*columns_to_drop)


    processed_frame = change_type(processed_frame,'FL_DATE', StringType())
    return processed_frame



# gives you a count of nans, nulls, specific string values, etc for each col

def check_null_values(frame: DataFrame):
    # columns_ = []
    # for column in frame.columns:
    #     try:
    #         sf.isnan(column)
    #         columns_.append(column)

    #     except :
    #         pass

    frame = frame.select([sf.count(sf.when(sf.isnan(i) | \
                                    sf.col(i).contains('NA') | \
                                    sf.col(i).contains('NULL') | \
                                    sf.col(i).isNull(), i)).alias(i) \
                    for i in [column for column in frame.columns if column not in ['FL_DATE'] ]])


    
    frame.show()

def change_type( df, column, imported_type ):
    return df.withColumn('column', sf.col(column).cast(imported_type))

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
    

    # Transform
    frame.printSchema()
    cleaned_frame = clean(frame)
    cleaned_frame.printSchema()

    cleaned_frame.show(5)

    check_null_values(cleaned_frame)

    # Load
    cleaned_frame.write.parquet(
        path=str(target_dir / "cleaned_flights"),
        mode="overwrite",
        compression="snappy",
    )
