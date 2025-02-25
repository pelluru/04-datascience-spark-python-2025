package pack
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import Day1.addColumnIndex
object Day2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("processing")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    val day2 = spark.read.format("csv").option("header", "true").load("D:/BigData/Sparkfolder/workspace/BatchidProcessing/Datasets/data2.csv").withColumn("batchid", lit(1))
    day2.show()
    val maxbatchid = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/test")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("query", "select max(id) as max_id,max(batchid) as maxbatch_id from processtab")
      .load()
    maxbatchid.show()

    val joindf = day2.crossJoin(maxbatchid)
    joindf.show()

    val finaldf = addColumnIndex(spark, joindf).withColumn("id", expr("id+max_id")).withColumn("batchid", expr("batchid+maxbatch_id")).drop("max_id", "maxbatch_id").select("id", "tdate", "custnno", "amt", "state", "batchid")
    finaldf.show()

    finaldf.write.format("jdbc").mode("append")
      .option("url", "jdbc:mysql://localhost:3306/test")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "processtab").save()
  }
}