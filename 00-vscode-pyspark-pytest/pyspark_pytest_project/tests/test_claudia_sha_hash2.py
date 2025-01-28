import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import hashlib
from unittest.mock import Mock
from typing import List, Dict

from app.claudia_sha_hash2 import process_files_with_hash,FileProcessor

# Test fixtures
@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for testing."""
    return (SparkSession.builder
            .master("local[1]")
            .appName("file_hash_test")
            .getOrCreate())

@pytest.fixture
def sample_files() -> List[Dict[str, str]]:
    """Sample file data for testing."""
    return [
        {'file_path': '/path/to/file1.txt'},
        {'file_path': '/path/to/file2.txt'},
        {'file_path': '/path/to/file3.txt'}
    ]

@pytest.fixture
def mock_file_contents() -> Dict[str, bytes]:
    """Mock file contents and their expected hashes."""
    return {
        '/path/to/file1.txt': b'content1',
        '/path/to/file2.txt': b'content2',
        '/path/to/file3.txt': b'content3'
    }

@pytest.fixture
def expected_hashes(mock_file_contents) -> Dict[str, str]:
    """Compute expected hashes for mock file contents."""
    return {
        path: hashlib.sha256(content).hexdigest()
        for path, content in mock_file_contents.items()
    }

# Tests
def test_process_files_with_hash(spark, sample_files, mock_file_contents, expected_hashes):
    """Test file processing and hash computation with mocked files."""
    
    # Create input DataFrame
    input_df = spark.createDataFrame(sample_files)
    
    # Create processor with mock contents
    processor = FileProcessor(mock_contents=mock_file_contents)
    
    # Process the DataFrame
    result_df = process_files_with_hash(input_df, processor)
    
    # Convert result to Python objects for easier assertions
    results = result_df.collect()
    
    # Verify results
    for row in results:
        file_path = row['file_path']
        assert file_path in expected_hashes, f"Unexpected file path: {file_path}"
        assert row['content_hash'] == expected_hashes[file_path], \
            f"Hash mismatch for {file_path}"
    
    # Verify all expected files were processed
    processed_files = {row['file_path'] for row in results}
    assert processed_files == set(expected_hashes.keys()), \
        "Not all files were processed"

def test_process_files_with_missing_file(spark):
    """Test handling of missing files."""
    
    # Create input DataFrame with non-existent file
    input_df = spark.createDataFrame([
        {'file_path': '/path/to/nonexistent.txt'}
    ])
    
    # Create processor with empty mock contents to simulate missing file
    processor = FileProcessor(mock_contents={})
    
    # Process should raise an error for missing file
    with pytest.raises(Exception):

        process_files_with_hash(input_df, processor).collect()