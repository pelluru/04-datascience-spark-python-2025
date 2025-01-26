from pyspark.sql import SparkSession

def create_spark_session(app_name):
    return SparkSession.builder.appName(app_name).getOrCreate()

def main():
    spark = create_spark_session('ExampleApp')
    print('Spark session created:', spark)

if __name__ == '__main__':
    main()