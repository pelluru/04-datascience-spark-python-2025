package movierating

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object movierating {
   def main(args:Array[String]):Unit={
      val conf = new SparkConf().setMaster("local[*]").setAppName("movie rating")
      val sc = new SparkContext(conf)
      sc.setLogLevel("ERROR")
      val ss = SparkSession.builder.getOrCreate()
      import ss.implicits._
      // reading the data
      val movies = ss.read.format("csv").option("header",true).load("file:///D:/BigData/Sparkfolder/Intellij Projects/movie-rating/Datasets/movies.csv")
      // reading the data
      val ratings = ss.read.format("csv").option("header",true).load("file:///D:/BigData/Sparkfolder/Intellij Projects/movie-rating/Datasets/ratings.csv")
      // creating the temporary view
      movies.createOrReplaceTempView("movies")
      ratings.createOrReplaceTempView("ratings")
      // writing the sql queries
      ss.sql("select a.movie_id,a.movie_title,b.ratings from movies a,ratings b where a.movie_id = b.movie_id order by ratings desc").show()

     // another way using spark join method

      val joindata =  movies.join(ratings,"movie_id")
      val finaljoindata = joindata.orderBy($"ratings".desc)

      //writing the data as a parquet file to the target location

      finaljoindata.coalesce(1).write.format("parquet").save("file:///D:/BigData/Sparkfolder/Intellij Projects/movie-rating/movierating-parquet-data")


   }
}
