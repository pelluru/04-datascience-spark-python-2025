from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

from app.square_udf import square

def test_square_udf():
    spark = SparkSession.builder.getOrCreate()

    square_udf = udf(square, IntegerType())

    data = [(1,), (2,), (3,)]
    df = spark.createDataFrame(data, ["x"])

    result_df = df.withColumn("x_squared", square_udf("x"))

    expected_data = [(1, 1), (2, 4), (3, 9)]
    expected_df = spark.createDataFrame(expected_data, ["x", "x_squared"])

    assert result_df.collect() == expected_df.collect()