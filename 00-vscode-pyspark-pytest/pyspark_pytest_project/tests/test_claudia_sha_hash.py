import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import hashlib
from unittest.mock import Mock
from typing import List, Dict

# Function to test
class FileProcessor:
    """Class to handle file processing with mockable methods"""
    def __init__(self, mock_contents=None):
        self.mock_contents = mock_contents or {}
    
    def read_file(self, file_path: str) -> bytes:
        """Mockable method to read file contents"""
        if self.mock_contents:
            if file_path in self.mock_contents:
                return self.mock_contents[file_path]
            raise FileNotFoundError(f"Mock file not found: {file_path}")
        with open(file_path, 'rb') as f:
            return f.read()

    def compute_hash(self, content: bytes) -> str:
        """Compute SHA-256 hash of content"""
        return hashlib.sha256(content).hexdigest()

def process_files_with_hash(df, processor=None):
    """
    Process a DataFrame containing file paths, read their contents,
    and compute SHA-256 hashes.
    
    Args:
        df: PySpark DataFrame with a 'file_path' column
        processor: FileProcessor instance for testing
    """
    if processor is None:
        processor = FileProcessor()
    
    def read_and_hash_file(file_path: str) -> str:
        content = processor.read_file(file_path)
        return processor.compute_hash(content)
    
    hash_udf = udf(read_and_hash_file, StringType())
    return df.withColumn('content_hash', hash_udf('file_path'))

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

    result_df.show()
    print(expected_hashes)
    
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
    
