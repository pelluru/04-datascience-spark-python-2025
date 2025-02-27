from pyspark import SparkConf,SparkContext
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.ml.feature import Tokenizer
from pyspark.ml.feature import HashingTF
from pyspark.ml.pipeline import *
from pyspark.ml.regression import LinearRegression
import os
#Taking supportive jars from maven repository
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.mysql:mysql-connector-j:8.0.32,org.apache.spark:spark-mllib_2.11:2.4.7 pyspark-shell'
def addColumnIndex(spark, df):
 window = Window.orderBy("userid")
 new_df = df.withColumn("subjectid", row_number().over(window))
 return new_df
conf = SparkConf().setMaster("local[*]").setAppName("CurrencyConversion")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()
#Reading a day2.csv file
df = spark.read.format("csv").option("header", "true").option("inferschema", "true").load("D:\BigData\Pyspark\PysparkProjects\CurrencyConversions\Datasets/day2.csv")
df.show()

# Create a Tokenizer to split the "date" column into words
tokenizer = Tokenizer(inputCol="date", outputCol="words")

# Create a HashingTF to convert the words into feature vectors
hashingTF = HashingTF(numFeatures=1000, inputCol=tokenizer.getOutputCol(), outputCol="features")

# Create a LinearRegression model
lr = LinearRegression(maxIter=100, regParam=0.01, elasticNetParam=0.0001)

# Create a Pipeline that combines the Tokenizer, HashingTF, and LinearRegression stages
pipeline = Pipeline(stages=[tokenizer, hashingTF, lr])

# Fit the pipeline to the input DataFrame
model = pipeline.fit(df)

# Use the fitted model to make predictions on the input DataFrame
preddf = model.transform(df)

# Display the predicted values
preddf.show()

#Reading the data from RDBMS
currencyratedf = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/test").option("driver", "com.mysql.cj.jdbc.Driver").option("user", "root").option("password", "123456").option("dbtable", "currencyrate_tab").load()
currencyratedf.show()

#Performing the left join
joindf = preddf.join(currencyratedf, ["currency"], "left")
joindf.show()

#Converting the exsisitng amount into USD
procdf = joindf.withColumn("currencyinUSD", expr("case when currency in ('GBP','INR') then amount*value else amount end")).withColumn("currencyinUSD", col("currencyinUSD").cast(DecimalType(18, 2))).select("userid", "date", "label", "amount", "currency", "currencyinUSD", "words", "features", "prediction").orderBy(col("userid"))
procdf.show()

#Attaching the incremental subjectid & constant batchid column
addcoldf = addColumnIndex(spark,procdf).withColumn("batchid", lit(1))
addcoldf.show()

#Reading the max of batchid and max of subjectid from RDBMS
maxdf = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/test").option("driver", "com.mysql.cj.jdbc.Driver").option("user", "root").option("password", "123456").option("query", "select max(batchid) as maxid, max(subjectid) as maxsubjectid from pycurrencytab").load()
maxdf.show()

#Performing a cross join
crossdf = maxdf.crossJoin(addcoldf)
crossdf.show()

finaldf = crossdf.withColumn("batchid", expr("maxid+batchid")).withColumn("subjectid", expr("maxsubjectid+subjectid")).drop("maxid","maxsubjectid").withColumn("batchid", col("batchid").cast("integer")).withColumn("subjectid", col("subjectid").cast("integer"))
finaldf.show()

#Changing all columns to string
finstrdf = finaldf.select([col(c).cast("string") for c in finaldf.columns])

#Writing into the RDBMS
finstrdf.write.format("jdbc").option("url", "jdbc:mysql://localhost:3306/test").option("driver", "com.mysql.cj.jdbc.Driver").option("user", "root").option("password", "123456").option("dbtable", "pycurrencytab").mode("append").save()

