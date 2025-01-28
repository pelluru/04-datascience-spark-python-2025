from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import hashlib

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