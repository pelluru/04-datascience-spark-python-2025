from pyspark import SparkConf,SparkContext
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
import os
from collections import namedtuple

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.databricks:spark-xml_2.11:0.11.0 pyspark-shell'
conf = SparkConf().setMaster("local[*]").setAppName("test")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()

#Define a unified column list and impose using select for all the dataframe
collist = ["txnno","txndate","custno","amount","category","product","city","state","spendby"]
#Read file 3 as csv with header true
csvdf = spark.read.format("csv").option("header","true").load("D:/BigData/Pyspark/PysparkProjects/PySparkTransformations/Datasets/file3.txt").select(*collist)
print("Reading a file f3 CSV :-")
csvdf.show()

#Read file 4 as json and file 5 as parquet and show both the dataframe
jsondf = spark.read.format("json").load("D:/BigData/Pyspark/PysparkProjects/PySparkTransformations/Datasets/file4.json").select(*collist)
print("Reading a file f4 JSON :-")
jsondf.show()

parquetdf = spark.read.format("parquet").load("D:/BigData/Pyspark/PysparkProjects/PySparkTransformations/Datasets/file5.parquet").select(*collist)
print("Reading a file f5 PARQUET :-")
parquetdf.show()

#Read File6 as xml and show the dataframe
xmldf = spark.read.format("xml").option("rowtag","txndata").load("D:/BigData/Pyspark/PysparkProjects/PySparkTransformations/Datasets/file6.xml").select(*collist)
print("Reading a file f6 XML :-")
xmldf.show()

#Read file1 as an rdd and filter gymnastics rows
f1 = sc.textFile("D:/BigData/Pyspark/PysparkProjects/PySparkTransformations/Datasets/file1.txt")
print("Original Data :-")
for x in f1.take(5):
    print(x)
gymdata = f1.filter(lambda x: 'Gymnastics' in x)
print("Filtered Data :-")
for x in gymdata.take(5):
    print(x)

#Create a case class and Impose case class to it for schema rdd
#And filter product contains Gymnastics
schema = namedtuple("schema",["txnno","txndate","custno","amount","category","product","city","state","spendby"])
flatdata = gymdata.map(lambda x:x.split(","))
schemardd = flatdata.map(lambda x: schema(x[0],x[1],x[2],x[3],x[4],x[5],x[6],x[7],x[8]))
schemafilgymdata = schemardd.filter(lambda x: 'Gymnastics' in x.product)
print("Filtered Gymnastics Data using Schema RDD :-")
for x in schemafilgymdata.take(5):
    print(x)
#Convert that to Dataframe from schema rdd
print("Converted to Dataframe from schema rdd :-")
schemadf = schemafilgymdata.toDF().select(*collist)
schemadf.show()

#Read file2.txt convert that to Row Rdd
f2 = sc.textFile("D:/BigData/Pyspark/PysparkProjects/PySparkTransformations/Datasets/file2.txt")
flatdata1 = f2.map(lambda x:x.split(","))
rowrdd = flatdata1.map(lambda x: Row(x[0],x[1],x[2],x[3],x[4],x[5],x[6],x[7],x[8]))
print("Row RDD :-")
for x in rowrdd.take(5):
    print(x)
#Create dataframe using row rdd
rowschema = StructType([
    StructField("txnno",StringType(),True),
    StructField("txndate",StringType(),True),
    StructField("custno",StringType(),True),
    StructField("amount", StringType(), True),
    StructField("category", StringType(), True),
    StructField("product", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("spendby", StringType(), True)
  ])

rowshcemadf = spark.createDataFrame(rowrdd,rowschema).select(*collist)
print("Converted to Dataframe from row rdd :-")
rowshcemadf.show()

#Union all the dataframes
uniondf = schemadf.union(rowshcemadf).union(csvdf).union(jsondf).union(parquetdf).union(xmldf)
print("Union of all Dataframes :-")
uniondf.show()

#From Union df Get year from txn date and rename it with year  and add one column at the end as status 1 for cash and 0 for credit in spendby and filter txnno>50000
finalres = uniondf.withColumn("txndate",expr("split(txndate,'-')[2]")).withColumnRenamed("txndate","year").withColumn("status",expr("case when spendby ='cash' then 1 else 0 end")).filter(col("txnno")>5000)
print("Final Data :-")
finalres.show()

#Write as an parquet in local with mode Append and partition the category column
finalres.write.format("parquet").mode("append").partitionBy("category").save("D:/BigData/Pyspark/PysparkProjects/PySparkTransformations/procdata")