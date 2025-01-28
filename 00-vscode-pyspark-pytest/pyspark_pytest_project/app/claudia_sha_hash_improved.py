# claudia_sha_hash.py
import hashlib
from pyspark.sql.functions import udf
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

class FileProcessor:
    """Class to handle file processing"""
    def read_file(self, file_path: str) -> bytes:
        """Method to read file contents"""
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
        processor: Optional FileProcessor instance for testing
    """
    if processor is None:
        processor = FileProcessor()
    
    def read_and_hash_file(file_path: str) -> str:
        content = processor.read_file(file_path)
        return processor.compute_hash(content)

    hash_udf = udf(read_and_hash_file, StringType())
    return df.withColumn('file_checksum', hash_udf('file_path'))


if __name__ == '__main__':

    spark = SparkSession.builder.appName("test sha2").getOrCreate()

    data = [("Alice", 25,"/Users/prabhakarapelluru/Downloads/Lato-Regular.bin"), ("Bob", 30,"/Users/prabhakarapelluru/Downloads/Lato-Regular.bin"), ("Charlie", 35,"/Users/prabhakarapelluru/Downloads/Lato-Regular.bin")]
    columns = ["name", "age","file_path"]

    df = spark.createDataFrame(data, columns)

    df_sha = process_files_with_hash(df)
    df_sha.show(truncate=False)