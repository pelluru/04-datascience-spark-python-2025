import pytest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf, col, sha2
from pyspark.sql.types import StringType
from unittest.mock import Mock, patch, mock_open
from pyspark.sql.functions import UserDefinedFunction

# Import the functions to test
from app.read_file_path_compute_hash import read_contents_of_the_file, add_column_with_sha2, read_file_content_udf

@pytest.fixture
def spark_session():
    """Create a test Spark session"""
    return SparkSession.builder \
        .appName("test") \
        .master("local[1]") \
        .getOrCreate()

@pytest.fixture
def sample_dataframe(spark_session):
    """Create a sample DataFrame for testing"""
    data = [
        ("file1.txt", "/path/to/file1.txt"),
        ("file2.txt", "/path/to/file2.txt")
    ]
    return spark_session.createDataFrame(data, ["filename", "file_path"])

def test_read_contents_of_the_file_success():
    """Test successful file reading"""
    test_content = b"test file content"
    mock_file = mock_open(read_data=test_content)
    
    with patch("builtins.open", mock_file):
        result = read_contents_of_the_file("/dummy/path/file.txt")
        assert result == test_content
        mock_file.assert_called_once_with("/dummy/path/file.txt", "rb")


def test_add_column_with_sha2_success(spark_session, sample_dataframe):
    """Test successful SHA2 column addition"""
    # Mock content for the UDF
    test_content = "test_content"
    mock_udf = udf(lambda _: test_content, StringType())
    
    with patch('app.read_file_path_compute_hash.read_file_content_udf', mock_udf):
        result_df = add_column_with_sha2(sample_dataframe)
        
        # Verify the result
        assert isinstance(result_df, DataFrame)
        assert "file_checksum" in result_df.columns
        assert "file_content" not in result_df.columns
        
        # Get the results
        results = result_df.collect()
        assert len(results) == 2
        
        # SHA2 was applied to the content
        for row in results:
            assert row["file_checksum"] is not None


def test_add_column_with_sha2_invalid_input():
    """Test handling of invalid input DataFrame"""
    with patch('builtins.print') as mock_print:
        result = add_column_with_sha2(None)
        assert result is None
        mock_print.assert_called_once()
        assert "Error computing SHA2 for the  file" in mock_print.call_args[0][0]
