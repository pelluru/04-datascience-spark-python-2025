import hashlib
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# Function to read file content
def read_file(file_path):
    try:
        with open(file_path, 'rb') as f:
            return f.read()
    except Exception as e:
        return None  # Handle errors (file not found, permission error, etc.)
    
    # Function to compute SHA-2 (SHA-256) hash from file content
def compute_sha2(file_content):
    return hashlib.sha256(file_content).hexdigest()


from pyspark.sql.functions import udf
from pyspark.sql.types import BinaryType

# UDF for reading file content
def read_file_udf(file_path):
    return read_file(file_path)

# UDF for computing SHA-2 hash from file content
def compute_sha2_udf(file_content):
    return compute_sha2(file_content)

# Register UDFs
read_file_spark_udf = udf(read_file_udf, BinaryType())
compute_sha2_spark_udf = udf(compute_sha2_udf, StringType())

from pyspark.sql import SparkSession

def main(): 

    # Create a Spark session
    spark = SparkSession.builder.master("local[1]").appName("SHA2 Test").getOrCreate()

    # Example DataFrame with file paths (replace with actual file paths)
    data = [("/path/to/file1.txt",), ("/path/to/file2.txt",), ("/path/to/file3.txt",)]
    df = spark.createDataFrame(data, ["file_path"])

    # Apply UDFs: read file and compute SHA-2
    df_with_sha2 = df.withColumn("file_content", read_file_spark_udf(F.col("file_path"))) \
                    .withColumn("file_sha2", compute_sha2_spark_udf(F.col("file_content")))

    # Show the resulting DataFrame with file paths and SHA-2 hashes
    df_with_sha2.show()


if __name__ == "__main__":
    main()