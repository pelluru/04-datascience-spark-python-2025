## Spark Integrations

### Table of Contents

|Integrations                                                |
|------------------------------------------------------------|
|[Spark-MySQL Integration](#spark-mysql-integration)         |
|[Spark-MSSQL Integration](#spark-mssql-integration)         |
|[Spark-Cassandra Integration](#spark-cassandra-integration) |
|[Spark-GCP Integration](#spark-gcp-integration)             |
|[Spark-Azure Integration](#spark-azure-integration)         |
|[Spark-AWS Integration](#spark-aws-integration)             |
|[Spark-SNOWFLAKE Integration](#spark-snowflake-integration) |

### Spark-MySQL Integration
* Open Spark Shell 
```sh
spark-shell --packages mysql:mysql-connector-java:5.1.49
```
* Read the data from MySQL using Spark. By default MySQL run on port `3306`
```sh
val df = spark.read.format("jdbc")
.option("url","jdbc:mysql://localhost:3306/databasename")
.option("driver","com.mysql.jdbc.Driver")
.option("user","username")
.option("password","password")
.option("dbtable","tablename")
.load()
df.show()
```
* Write the data into MySQL using Spark
```sh
df.write.format("jdbc")
.option("url","jdbc:mysql://localhost:3306/database_name")
.option("driver","com.mysql.cj.jdbc.Driver")
.option("user","username")
.option("password","password")
.option("dbtable","target_tablename")
.save()

```
* In latest version of MySQL the driver name changed to `com.mysql.cj.jdbc.Driver`

**[⬆ Back to Top](#table-of-contents)**

### Spark-MSSQL Integration
* Open Spark Shell
```sh
spark-shell --packages com.microsoft.sqlserver:mssql-jdbc:12.2.0.jre8
```
* Read the data from MSSQL using Spark. By default MSSQL run on port `1433`
```sh
val df = spark.read
  .format("jdbc")
  .option("url", "jdbc:sqlserver://hostname(or)ipaddress:port;databaseName=database_name")
  .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .option("user","username")
  .option("password","password")
  .option("dbtable", "tableName")
  .load()
df.show()
```
* Write the data into MSSQL using Spark
```sh
 df.write
  .format("jdbc")
  .option("url", "jdbc:sqlserver://hostname(or)ipaddress:port;databaseName=database_name")
  .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .option("user","username")
  .option("password","password")
  .option("dbtable", "target_tableName")
  .save()
```
**[⬆ Back to Top](#table-of-contents)**

### Spark-Cassandra Integration
* Open Spark Shell
```sh
spark-shell --packages com.datastax.spark:spark-cassandra-connector_2.11:2.4.0
```
* Read the data from Cassandra using Spark. By default cassandra run on port `9042`
```sh
val df = spark.read
.format("org.apache.spark.sql.cassandra")
.option("spark.cassandra.connection.host","hostname")
.option("spark.cassandra.connection.port","9042")
.option("keyspace","Keyspacename")
.option("table","tablename")
.load()
			
df.show()
  ```
  * Write the data into Cassandra using Spark
  ```sh
  df.write
.format("org.apache.spark.sql.cassandra")
.option("spark.cassandra.connection.host","hostname")
.option("spark.cassandra.connection.port","9042")
.option("keyspace","keyspacename")
.option("table","target_tablename")
.save()
```


### Spark-GCP Integration

* Download the `GCS Connector Hadoop` Jar file

* Read the data from GCP Cloud Storage using Spark

```sh
val spark = SparkSession.builder()
    .config("spark.hadoop.fs.gs.impl","com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl","com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    .getOrCreate()

    val df = spark.read.format("csv").option("header", true)
    .option("fs.gs.project.id", "YOUR PROJECT ID")
    .option("google.cloud.auth.service.account.json.keyfile", "GOOGLE ACCOUNT JSON KEY FILE PATH")
    .load("BUCKET LOCATION")
    df.show()
```

* Write the data into the GCP Cloud Storage using Spark

```sh
val spark = SparkSession.builder()
    .config("spark.hadoop.fs.gs.impl","com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl","com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    .getOrCreate()

    val df = spark.read.format("csv").option("header", true)
    .option("fs.gs.project.id", "YOUR PROJECT ID")
    .option("google.cloud.auth.service.account.json.keyfile", "GOOGLE ACCOUNT JSON KEY FILE PATH")
    .load("BUCKET LOCATION")
    df.show()
val findf = df.write.format("csv").save("BUCKET LOCATION")
```

**[⬆ Back to Top](#table-of-contents)**


### Spark-Azure Integration

* Download the `Hadoop Azure` & `Azure Storage` Jar files.
* Read the data from Azure blob storage using spark.
```sh
val azurestorageaccountname = "azurestoragebigdata" // Your Storage Account Name
    val azurestorageaccountkey = "+eq8eZRvd9n0pnbBgGf/7iG9doneYtEQKDeNWieubtvGYaJ+7fW6r0s78KuHWVD2Yyyssiq8lSbJh73y4B+AStzzIXqA==" //Your Storage Account Key
    val containername = "test"       // Your Container Name
    val blobname = "usdata.csv"      // Your file name
    
    spark.conf.set(s"fs.azure.account.key.$azurestorageaccountname.blob.core.windows.net",
 azurestorageaccountkey)           //set the azure configuration
  
  val df = spark.read.format("csv").option("header", true).load(s"wasbs://$containername@$azurestorageaccountname.blob.core.windows.net/$blobname")
  df.show()
```
* Write the data into the Azure Blob Storage using Spark
```sh
val azurestorageaccountname = "azurestoragebigdata" // Your Storage Account Name
    val azurestorageaccountkey = "+eq8eZRvd9n0pnbBgGf/7iG9doneYtEQKDeNWieubtvGYaJ+7fW6r0s78KuHWVD2Yyyssiq8lSbJh73y4B+AStzzIXqA==" //Your Storage Account Key
    val containername = "test"       // Your Container Name
    val blobname = "usdata.csv"      // Your file name
    
    spark.conf.set(s"fs.azure.account.key.$azurestorageaccountname.blob.core.windows.net",
 azurestorageaccountkey)           //set the azure configuration
val findf = fildf.write.format("csv").save(s"wasbs://$containername@$azurestorageaccountname.blob.core.windows.net/filterdata.csv")
```
**[⬆ Back to Top](#table-of-contents)**

### Spark-AWS Integration

* Read the data from AWS S3 storage using spark.
```sh
    val df = spark.read.format("csv")
 .option("header", true)
 .option("fs.s3a.access.key", "YOUR ACCESS KEY")
 .option("fs.s3a.secret.key", "YOUR SECRET KEY")
 .load("FILEPATH")
```
* Write the data into the AWS S3 Storage using Spark
```sh
val finaldf = df.write.format("csv")
 .option("header", true)
 .option("fs.s3a.access.key", "YOUR ACCESS KEY")
 .option("fs.s3a.secret.key", "YOUR SECRET KEY")
 .load("TARGET DIRECTORY")
```
**[⬆ Back to Top](#table-of-contents)**


### Spark-SNOWFLAKE Integration

* For Spark Snowflake Integration you need Spark Snowflake Dependency. You can get the depencdency from here [Spark Snowflake Maven Repository](https://mvnrepository.com/artifact/net.snowflake/spark-snowflake)

* Read the data from Snowflake table using spark.
```sh
  val finaldf = df.read.format("snowflake")
.option("sfURL","SNOWFLAKE URL")
.option("sfAccount","ACCOUNTNAME")
.option("sfUser","USERNAME")
.option("sfPassword","PASSWORD")
.option("sfDatabase","DATABASE NAME")
.option("sfSchema","SCHEMA NAME")
.option("sfRole","ACCOUNTADMIN")
.option("sfWarehouse","COMPUTE_WH")
.option("dbtable","TABLE NAME")
.load()
```
* Write the data into the Snowflake table using Spark
```sh
df.write.mode("overwrite").format("snowflake")
.option("sfURL","SNOWFLAKE URL")
.option("sfAccount","ACCOUNTNAME")
.option("sfUser","USERNAME")
.option("sfPassword","PASSWORD")
.option("sfDatabase","DATABASE NAME")
.option("sfSchema","SCHEMA NAME")
.option("sfRole","ACCOUNTADMIN")
.option("sfWarehouse","COMPUTE_WH")
.option("dbtable","TABLE NAME")
.save()
```
**[⬆ Back to Top](#table-of-contents)**


