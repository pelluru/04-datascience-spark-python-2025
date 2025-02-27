## Movie Rating Analysis using Spark
This is a simple project that demonstrates how to use Apache Spark to analyze movie ratings data. The project uses Spark's DataFrame API to load and process data from two CSV files: one containing movie ratings data and another containing movie metadata.

#### The project performs the following tasks:

* Load movie ratings data from a CSV file into a Spark DataFrame.
* Load movie metadata from a CSV file into a separate Spark DataFrame.
* Join the two DataFrames to create a single DataFrame containing both rating and metadata information.
* Sort the results by rating in descending order.
* Display the top-rated movies.
* Save the data in the local file system as Parquet file.

## Technologies Used
#### The following technologies were used to implement this project:

* Apache Spark: A fast and general-purpose distributed computing system.
* Scala: A high-level programming language that runs on the Java Virtual Machine (JVM).
* SBT: A build tool for Scala projects.

## Expected Output
#### The following is a output of the project:

```
+--------+--------------------+-------+
|movie_id|         movie_title|ratings|
+--------+--------------------+-------+
|      10|         777 Charlie|    8.9|
|       6|Rocketry: The Nam...|    8.8|
|       5|             Kantara|    8.6|
|       8|          Sita Ramam|    8.6|
|       3|       KGF Chapter 2|    8.4|
|       4|              Vikram|    8.4|
|       2|  The Kashmiri Files|    8.3|
|       7|               Major|    8.2|
|       1|                 RRR|      8|
|       9|Ponniyin Selvan :...|    7.9|
+--------+--------------------+-------+
```
## Output Data in Parquet Format
```
PAR1 rl, 6 (91   9   ,10   6   5 8 3 4<2   7   1   9 ÖØ, 6 (Vikram
777 Charlie   «ðR   
   777 Charlie   Rocketry: The Nambi Effect    Kantara
   Sita Ramam
   KGFFLpter 2   Vikram   HtKashmiri Files   Major   RRRxdPonniyin Selvan : Part OnelTL  6   8.9  8	  6	  4	  3	 02   8   7.9  ,6 (8.97.9   4   ˆ´±>  LHspark_schema %movie_id%  %
movie_title%  % ratings%  <&5 movie_id¨¢&<6 (91      &ª5 
movie_title®°&ª<6 (Vikram
777 Charlie      &Ú5 ratingsàÌ&Ú<6 (8.97.9 ,     ¶ ,org.apache.spark.version2.4.7 )org.apache.spark.sql.parquet.row.metadataä{"type":"struct","fields":[{"name":"movie_id","type":"string","nullable":true,"metadata":{}},{"name":"movie_title","type":"string","nullable":true,"metadata":{}},{"name":"ratings","type":"string","nullable":true,"metadata":{}}]} Jparquet-mr version 1.10.1 (build a89df8f9932b6ef6633d06069e50c9b7970bebd1)<       §  PAR1
```
