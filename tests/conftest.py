import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark(request):
    """Fixture for creating a SparkSession."""

    spark = (
        SparkSession.builder.appName("Workshop: TestSession")
        .config("spark.ui.showConsoleProgress", "true")
        .getOrCreate()
    )
    request.addfinalizer(spark.stop)

    return spark
