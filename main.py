# This is a demo of SQL functions using PySpark.

import pandas
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import Row

#   Simulated Snowflake Implementation

#   use warehouse EMPORDERS_WAREHOUSE
#   use database EMPORDERS_DATABASE
#   use schema PUBLIC

#   create table "Orders" (
#   id numeric,
#   item string,
#   quantity_ordered numeric,
#   item_cost numeric
#   );

#   create table "Employees" (
#   id numeric,
#   lastname string,
#   firstname string,
#   gender string
#   );

# Create a Spark session
spark = SparkSession.builder.getOrCreate()


Orders = pandas.read_csv('/Users/carstenjuliansavage/Desktop/R Working Directory/r-sql-demo-files/orders.csv')
Employees = pandas.read_csv('/Users/carstenjuliansavage/Desktop/R Working Directory/r-sql-demo-files/employees.csv')

# Create spark instances of the tables
Orders_Spark = spark.createDataFrame(Orders)
Employees_Spark = spark.createDataFrame(Employees)
Orders_Spark.show()
Employees_Spark.show()

# Create referenceable tables
Orders_Spark.createOrReplaceTempView("Orders_Spark")
Employees_Spark.createOrReplaceTempView("Employees_Spark")

EmployeeOrdersInner_Spark = spark.sql("SELECT * "
                                      "FROM Orders_Spark a "
                                      "INNER JOIN Employees_Spark b "
                                      "USING (id)")

EmployeeOrdersInner_Spark.show()

EmployeeOrdersInner_Spark.createOrReplaceTempView("EmployeeOrdersInner_Spark")

# Dense Rank
Employee_Cost_Rank = spark.sql("SELECT *, DENSE_RANK() OVER (ORDER BY item_cost desc) AS Cost_RANK "
                               "FROM EmployeeOrdersInner_Spark "
                               "ORDER BY firstname asc")

Employee_Cost_Rank.createOrReplaceTempView("Employee_Cost_Rank")

Employee_Cost_Final = Employee_Cost_Rank.toPandas()

# Rank
spark.sql("SELECT *, RANK() OVER (PARTITION BY firstname ORDER BY item_cost desc) AS Cost_RANK "
          "FROM EmployeeOrdersInner_Spark "
          "ORDER BY firstname, item_cost desc").show()

# Running Total
spark.sql("SELECT *, SUM(item_cost) OVER (ORDER BY id, item, quantity_ordered asc) AS Running_Total "
          "FROM EmployeeOrdersInner_Spark "
          "ORDER BY Running_Total, item_cost asc").show()
