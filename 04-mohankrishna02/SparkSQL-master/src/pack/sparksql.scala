package pack
import org.apache.spark.sql.SparkSession
import org.apache.spark.{ SparkConf, SparkContext }
object sparksql {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("sqlrevision")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val ss = SparkSession.builder().getOrCreate()
    val df = ss.read.format("csv").option("header", true).load("D:/BigData/Sparkfolder/workspace/SparkSQL/Datasets/df.csv")
    val df1 = ss.read.format("csv").option("header", true).load("D:/BigData/Sparkfolder/workspace/SparkSQL/Datasets/df1.csv")
    val cust = ss.read.format("csv").option("header", true).load("D:/BigData/Sparkfolder/workspace/SparkSQL/Datasets/cust.csv")
    val prod = ss.read.format("csv").option("header", true).load("D:/BigData/Sparkfolder/workspace/SparkSQL/Datasets/prod.csv")

    df.show()
    df1.show()
    cust.show()
    prod.show()

    df.createOrReplaceTempView("df")
    df1.createOrReplaceTempView("df1")
    cust.createOrReplaceTempView("cust")
    prod.createOrReplaceTempView("prod")

    println("Validate Data :-")
    ss.sql("select * from df").show()

    println("select two columns from df :-")
    ss.sql("select id,tdate from df order by id").show()

    println("Select column with category filter = Exercise :-")
    ss.sql("select * from df where category = 'Exercise'").show()

    println("Multi Column filter :-")
    ss.sql("select * from df where category = 'Exercise' and spendby = 'cash'").show()

    println("Multi Value Filter :-")
    ss.sql("select * from df where category in ('Exercise','Gymnastics')").show()

    println("Like Filter :-")
    ss.sql("select * from df where product like '%Gymnastics%'").show()

    println("Not Filters :-")
    ss.sql("select * from df where category != 'Exercise'").show()

    println("Not In Filters :-")
    ss.sql("select * from df where category not in ('Exercise','Gymnastics')").show()

    println("Null Filters :-")
    ss.sql("select * from df where product is null").show()

    println("Max Function :-")
    ss.sql("select max(id) from df").show()

    println("Min Funtion :-")
    ss.sql("select min(id) from df").show()

    println("Count :-")
    ss.sql("select count(id) from df").show()

    println("Condition statement :-")
    ss.sql("select *,case when spendby = 'cash' then 1 else 0 end as status from df").show()

    println("Concat data :-")
    ss.sql("select id,category,concat(id,'-',category) as concatdata from df").show()

    println("Concat_ws data :-")
    ss.sql("select concat_ws('_',id,category,product) as concatdata from df").show(false)

    println("Lower Case data :-")
    ss.sql("select category,lower(category) as lower_category from df").show()

    println("Ceil data :-")
    ss.sql("select amount,ceil(amount) as ceil_amount from df").show()

    println("Round the data :-")
    ss.sql("select amount,round(amount) as round_amount from df").show()

    println("Replace Nulls using coleasc :-")
    ss.sql("select coalesce(product,'NA') from df").show()

    println("Trim the space :-")
    ss.sql("select trim(product) from df").show()

    println("Distinct the columns :-")
    ss.sql("select distinct category,spendby from df").show()

    println("Substring with Trim :-")
    ss.sql("select substring(trim(product),1,10) from df").show()

    println("Substring/Split operation :-")
    ss.sql("select substring_index(category,' ',1) from df").show()

    println("Union all :-")
    ss.sql("select * from df union all select * from df1").show()

    println("Union :-")
    ss.sql("select * from df union select * from df1 order by id").show()

    println("Aggregate Sum :-")
    ss.sql("select category, sum(amount) as total from df group by category").show()

    println("Aggregate sum with two columns :-")
    ss.sql("select category,spendby, sum(amount) as total from df group by category,spendby").show()

    println("Aggregate Count :-")
    ss.sql("select category,spendby,sum(amount) as total,count(amount) as count from df group by category,spendby").show()

    println("Aggregate Max :-")
    ss.sql("select category,max(amount) as max_amount from df group by category").show()

    println("Aggregate with Order Descending :-")
    ss.sql("select category,max(amount) as max_amount from df group by category order by category desc").show()

    println("Window Row Number :-")
    ss.sql("select category,amount, row_number() OVER ( partition by category order by amount desc ) AS row_number FROM df").show()

    println("Window Dense_rank Number :-")
    ss.sql("select category,amount, dense_rank() OVER ( partition by category order by amount desc ) AS dense_rank FROM df").show()

    println("Window rank Number :-")
    ss.sql("select category,amount, rank() OVER ( partition by category order by amount desc ) AS rank FROM df").show()

    println("Window Lead function :-")
    ss.sql("select category,amount, lead(amount) OVER ( partition by category order by amount desc ) AS lead FROM df").show()

    println("Window lag  function :-")
    ss.sql("select category,amount, lag(amount) OVER ( partition by category order by amount desc ) AS lag FROM df").show()

    println("Having function :-")
    ss.sql("select category, count(category) as count from df group by category having count(category)>1").show()

    println("Inner Join :-")
    ss.sql("select a.id,a.name,b.product from cust a join prod b on a.id=b.id").show()

    println("Left Join :-")
    ss.sql("select a.id,a.name,b.product from cust a left join prod b on a.id=b.id").show()

    println("Right Join :-")
    ss.sql("select a.id,a.name,b.product from cust a right join prod b on a.id=b.id").show()

    println("Full Join :-")
    ss.sql("select a.id,a.name,b.product from cust a full join prod b on a.id=b.id").show()

    println("left anti Join :-")
    ss.sql("select a.id,a.name from cust a left anti join  prod b on a.id=b.id").show()

    println("Date format :-")
    ss.sql("select id,tdate,from_unixtime(unix_timestamp(tdate,'MM-dd-yyyy'),'yyyy-MM-dd') as con_date from df").show()

    println("Sub query :-")
    ss.sql("""select sum(amount) as total , con_date from(select id,tdate,from_unixtime(unix_timestamp(tdate,'MM-dd-yyyy'),'yyyy-MM-dd') as con_date,amount,category,product,spendby from df) group by con_date """).show()
  }
}