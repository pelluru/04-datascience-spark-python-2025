from pyspark.sql import DataFrame
from pyspark.sql.functions import udf,col
from pyspark.sql.types import StringType

def read_contents_of_the_file(file_path: str):
    try:
        with open(file_path, "rb") as f:
            content = f.read()
            return content
    except Exception as e:
        error_message = f"Error reading a file,{e}"
        return None

read_file_content_udf = udf(read_contents_of_the_file, StringType())

def add_column_with_sha2(source_dataFrame, sha2_bits=256):
    try:
        source_dataFrame_with_file_content = source_dataFrame.withColumn(
            "file_content", read_file_content_udf("file_path")
        )
        source_dataFrame_with_sha2 = source_dataFrame_with_file_content.withColumn(
            "file_checksum", col("file_content")
        ).drop("file_content")
        return source_dataFrame_with_sha2
    except Exception as e:
        error_message = f"Error computing SHA2 for the  file,{e}"
        print(error_message)
        return None