import pytest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
from unittest.mock import Mock, patch, mock_open
from pyspark.sql.functions import UserDefinedFunction

# Import your module
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

def test_add_column_with_sha2(spark_session, sample_dataframe):
    """Test adding SHA2 hash column to DataFrame"""
    test_udf = udf(lambda x: "test_content", StringType())
    
    with patch('app.read_file_path_compute_hash.read_file_content_udf', test_udf):
        result_df = add_column_with_sha2(sample_dataframe)
        assert isinstance(result_df, DataFrame)
        assert "file_checksum" in result_df.columns
        
        results = result_df.collect()
        assert len(results) == 2
        assert all(row["file_checksum"] == "test_content" for row in results)


def test_read_contents_of_the_file():
    """Test reading file contents"""
    test_content = b"test file content"
    mock_file = mock_open(read_data=test_content)
    
    with patch("builtins.open", mock_file):
        result = read_contents_of_the_file("/dummy/path/file.txt")
        assert result == test_content
        mock_file.assert_called_once_with("/dummy/path/file.txt", "rb")

