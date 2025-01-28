# test_claudia_sha_hash.py
import pytest
from pyspark.sql import SparkSession
import hashlib
from unittest.mock import MagicMock
from app.claudia_sha_hash_improved import process_files_with_hash, FileProcessor

class MockFileProcessor:
    """Mock version of FileProcessor for testing"""
    def __init__(self, mock_contents):
        self.mock_contents = mock_contents

    def read_file(self, file_path: str) -> bytes:
        """Mock method to read file contents"""
        if file_path in self.mock_contents:
            return self.mock_contents[file_path]
        raise FileNotFoundError(f"Mock file not found: {file_path}")

    def compute_hash(self, content: bytes) -> str:
        """Real hash computation for testing"""
        return hashlib.sha256(content).hexdigest()

@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for testing."""
    return (SparkSession.builder
            .master("local[1]")
            .appName("file_hash_test")
            .getOrCreate())

@pytest.fixture
def mock_file_paths():
    """Sample file paths for testing."""
    return [
        {'file_path': '/path/to/file1.txt'},
        {'file_path': '/path/to/file2.txt'},
        {'file_path': '/path/to/file3.txt'}
    ]

@pytest.fixture
def mock_file_contents():
    """Mock file contents."""
    return {
        '/path/to/file1.txt': b'content1',
        '/path/to/file2.txt': b'content2',
        '/path/to/file3.txt': b'content3'
    }

@pytest.fixture
def expected_hashes(mock_file_contents):
    """Compute expected hashes for mock file contents."""
    return {
        path: hashlib.sha256(content).hexdigest()
        for path, content in mock_file_contents.items()
    }

@pytest.fixture
def mock_processor(mock_file_contents):
    """Create a mock processor with test data."""
    return MockFileProcessor(mock_file_contents)

def test_process_files_with_hash(spark, mock_file_paths, expected_hashes, mock_processor):
    """Test file processing and hash computation with mocked files."""
    # Create input DataFrame
    input_df = spark.createDataFrame(mock_file_paths)
    
    # Process the DataFrame with mock processor
    result_df = process_files_with_hash(input_df, processor=mock_processor)
    
    # Convert result to Python objects for assertions
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

def test_file_processor_error_handling(spark, mock_processor):
    """Test error handling for non-existent files."""
    # Create DataFrame with non-existent file
    input_df = spark.createDataFrame([{'file_path': '/non/existent/file.txt'}])
    
    # Process the DataFrame with mock processor
    result_df = process_files_with_hash(input_df, processor=mock_processor)
    
    # Expect FileNotFoundError when collecting results
    with pytest.raises(Exception) as exc_info:
        result_df.collect()
    assert "FileNotFoundError" in str(exc_info.value)