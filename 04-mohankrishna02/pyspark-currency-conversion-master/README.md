## PySpark Linear Regression with Currency Conversion and MySQL Integration
### Description
The Spark Linear Regression with Currency Conversion and MySQL Integration project is an example of how to use Spark to perform linear regression, convert currency rates using data from a MySQL table, and integrate the results into a MySQL database.

The project is implemented using Python and utilizes Apache Spark for data processing, MySQL for storing the currency rate data and the regression results.

### The project consists of the following steps:

* Read the pred1.csv file using Spark.
* Perform linear regression with the specified parameters (setMaxIter = 100, elasticNetParam = 0.001).
* Convert the currency into USD using data from a MySQL table named currency_rates.
* Add the subjectid and batchid columns to the final dataframe.
* Write the final dataframe into a MySQL table.

The Spark Linear Regression with Currency Conversion and MySQL Integration project is a useful example for anyone looking to use Spark for data processing and integrate the results into a database. It demonstrates how to perform linear regression on a dataset, convert currency rates using data from a MySQL table, and integrate the results into a MySQL database for further analysis. The project is also a good starting point for developers who want to learn more about Spark, Scala, and MySQL integration.

## Expected Output:
```
mysql> select * from pycurrencytab;
+--------+-----------+-------+--------+----------+---------------+-------------+--------------------+--------------------+-----------+---------+
| userid | date      | label | amount | currency | currencyinUSD | words       | features           | prediction         | subjectid | batchid |
+--------+-----------+-------+--------+----------+---------------+-------------+--------------------+--------------------+-----------+---------+
| 1      | 01-jan-17 | 9.15  | 50     | GBP      | 60.50         | [01-jan-17] | (1000,[595],[1.0]) | 9.253452470074857  | 1         | 1       |
| 2      | 02-jan-17 | 9.25  | 70     | USD      | 70.00         | [02-jan-17] | (1000,[605],[1.0]) | 9.254239753285216  | 2         | 1       |
| 3      | 03-jan-17 | 9.29  | 80     | INR      | 0.96          | [03-jan-17] | (1000,[987],[1.0]) | 9.290705557579921  | 3         | 1       |
| 4      | 04-jan-18 | 9.3   | 90     | GBP      | 108.90        | [04-jan-18] | (1000,[455],[1.0]) | 9.299817931137751  | 4         | 1       |
| 5      | 01-jan-17 | 9.35  | 50     | INR      | 0.60          | [01-jan-17] | (1000,[595],[1.0]) | 9.253452470074857  | 5         | 1       |
| 6      | 06-jan-17 | 9.43  | 75     | INR      | 0.90          | [06-jan-17] | (1000,[479],[1.0]) | 9.4183318178474    | 6         | 1       |
| 7      | 07-Jan-17 | 10.15 | 60     | GBP      | 72.60         | [07-jan-17] | (1000,[829],[1.0]) | 10.708321116958286 | 7         | 2       |
| 8      | 07-Jan-17 | 11.25 | 100    | USD      | 100.00        | [07-jan-17] | (1000,[829],[1.0]) | 10.708321116958286 | 8         | 2       |
| 9      | 08-Jan-17 | 12.29 | 110    | INR      | 1.32          | [08-jan-17] | (1000,[135],[1.0]) | 12.293030678973722 | 9         | 2       |
| 10     | 09-Jan-18 | 13.3  | 120    | GBP      | 145.20        | [09-jan-18] | (1000,[895],[1.0]) | 13.298350057059706 | 10        | 2       |
| 11     | 10-Jan-17 | 14.35 | 130    | INR      | 1.56          | [10-jan-17] | (1000,[727],[1.0]) | 14.343488648704835 | 11        | 2       |
| 12     | 11-Jan-17 | 15.43 | 140    | INR      | 1.68          | [11-jan-17] | (1000,[88],[1.0])  | 15.418488381345158 | 12        | 2       |
+--------+-----------+-------+--------+----------+---------------+-------------+--------------------+--------------------+-----------+---------+
```

## Technologies/Tools Used:
* PySpark - A fast and general purpose distributed processing frame work. To install the PySpark, you can use the following command
```sh
pip install pyspark==<PySparkVersion>
```
* Python - A high level programming language. You can dwonload the python from here <https://www.python.org/downloads/>
* PyCharm - PyCharm is an integrated development environment used for programming in Python. You can dwonload the PyCharm from here <https://www.jetbrains.com/pycharm/download/#section=windows>

## Documentation:
PySpark Documentation - <https://spark.apache.org/docs/latest/>

Python Documentation - <https://docs.python.org/3/tutorial/index.html>
