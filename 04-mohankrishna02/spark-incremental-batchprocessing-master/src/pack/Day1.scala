package pack
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
object Day1 {
  def addColumnIndex(spark: SparkSession, df: DataFrame) = {
    spark.sqlContext.createDataFrame(
      df.rdd.zipWithIndex.map {
        case (row, index) => Row.fromSeq(row.toSeq :+ index + 1)
      },
      StructType(df.schema.fields :+ StructField("id", LongType, false)))
  }
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("processing")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    val day1 = spark.read.format("csv").option("header", "true").load("D:/BigData/Sparkfolder/workspace/BatchidProcessing/Datasets/data1.csv")
    day1.show()
    val finaldf = addColumnIndex(spark, day1).withColumn("batchid", lit(1)).select("id", "tdate", "custnno", "amt", "state", "batchid")
    finaldf.show()
    finaldf.write.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/test")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "processtab")
      .save()
  }

}