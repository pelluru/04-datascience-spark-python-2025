import pytest
from unittest import mock
import hashlib
from pyspark.sql import SparkSession
from pyspark.sql import Row
from your_module import sha2_from_file  # Replace with the correct import path for your function

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

    # Use `mock.patch` to mock the `open` function and simulate reading file content
    with mock.patch("builtins.open", mock.mock_open(read_data=mock_content)) as mock_open:
        # Call the function to compute the SHA-2 hash
        result_df = sha2_from_file(df, "file_path")

        # Collect the results into a list of Row objects
        result = result_df.select("file_path", "file_sha2").collect()

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

