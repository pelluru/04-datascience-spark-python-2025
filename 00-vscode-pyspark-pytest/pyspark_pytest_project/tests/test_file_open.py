import pytest
from unittest import mock
import hashlib
from pyspark.sql import SparkSession,Row
from app.compute_file_sha2 import read_file_udf, compute_sha2_udf  # Import your functions here

@pytest.fixture(scope="module")
def spark():
    # Create a Spark session for testing
    spark_session = SparkSession.builder.master("local[1]").appName("pytest").getOrCreate()
    yield spark_session
    spark_session.stop()

def test_sha2_from_file(spark):
    # Define the mock content you want to return when reading the file
    mock_content = b"File content to hash"
    expected_hash = hashlib.sha256(mock_content).hexdigest()

    # Sample data with file paths (these don't actually need to exist)
    data = [("/path/to/file1.txt",), ("/path/to/file2.txt",), ("/path/to/file3.txt",)]
    df = spark.createDataFrame(data, ["file_path"])

    # Use `mock.patch` to mock the `open` function globally
    with mock.patch("builtins.open", mock.mock_open(read_data=mock_content)) as mock_open:
        # Apply the UDFs: read file and compute SHA-2
        df_with_sha2 = df.withColumn("file_content", read_file_udf(F.col("file_path"))) \
                         .withColumn("file_sha2", compute_sha2_udf(F.col("file_content")))

        # Collect the results into a list of Row objects
        result = df_with_sha2.select("file_path", "file_sha2").collect()

        # Expected result with the SHA-2 hash
        expected_result = [
            Row(file_path="/path/to/file1.txt", file_sha2=expected_hash),
            Row(file_path="/path/to/file2.txt", file_sha2=expected_hash),
            Row(file_path="/path/to/file3.txt", file_sha2=expected_hash)
        ]

        # Assert that the result DataFrame matches the expected result
        assert result == expected_result

        # Ensure the `open` function was called with the expected arguments
        mock_open.assert_any_call("/path/to/file1.txt", 'rb')
        mock_open.assert_any_call("/path/to/file2.txt", 'rb')
        mock_open.assert_any_call("/path/to/file3.txt", 'rb')