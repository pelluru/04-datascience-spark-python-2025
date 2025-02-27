package pack
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import Day1.addColumnIndex
object Day5 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("processing")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    val day5 = spark.read.format("csv").option("header", "true").load("D:/BigData/Sparkfolder/workspace/BatchidProcessing/Datasets/data55.csv")
    day5.show(100)

    val inbatch = addColumnIndex(spark, day5)
      .withColumnRenamed("id", "batchid")
    inbatch.show(100)

    val total = inbatch.count()
    val limit = 10
    val mod = total / limit

    val moddf = inbatch.withColumn("batchid", col("batchid") % mod + 1).sort("batchid").withColumn("batchid", expr("cast(batchid as int)"))
    moddf.show(100)
    val increid = addColumnIndex(spark, moddf)
      .select("id", "tdate", "custnno", "amt", "state", "batchid")
    increid.show(100)

    val maxbatchid = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/test")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("query", "select max(id) as max_id,max(batchid) as maxbatch_id from processtab")
      .load()
    maxbatchid.show()

    val finaldf = maxbatchid.crossJoin(increid).withColumn("id", expr("id+MAX_ID")).withColumn("batchid", expr("batchid+MAXBATCH_ID")).drop("MAXBATCH_ID", "MAX_ID")
    finaldf.show(100)

    finaldf.write.format("jdbc").mode("append")
      .option("url", "jdbc:mysql://localhost:3306/test")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "processtab").save()
  }
}