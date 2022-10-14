# This is a demo of SQL functions using PySpark.

import pandas
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession.builder.getOrCreate()

Orders = pandas.read_csv('/Users/carstenjuliansavage/Desktop/R Working Directory/r-sql-demo-files/orders.csv')
Employees = pandas.read_csv('/Users/carstenjuliansavage/Desktop/R Working Directory/r-sql-demo-files/employees.csv')

Orders_Spark = spark.createDataFrame(Orders)
Employees_Spark = spark.createDataFrame(Employees)
Orders_Spark.show()
Employees_Spark.show()
Orders_Spark.createOrReplaceTempView("Orders_Spark")
Employees_Spark.createOrReplaceTempView("Employees_Spark")

EmployeeOrdersInner_Spark = spark.sql("SELECT * from Orders_Spark a INNER JOIN Employees_Spark b USING (id)")

EmployeeOrdersInner_Spark.show()

EmployeeOrdersInner_Spark.createOrReplaceTempView("EmployeeOrdersInner_Spark")

Employee_Cost_Rank = spark.sql("SELECT *, DENSE_RANK() OVER (ORDER BY item_cost desc) AS Cost_RANK FROM EmployeeOrdersInner_Spark ORDER BY firstname asc")

Employee_Cost_Rank.createOrReplaceTempView("Employee_Cost_Rank")

Employee_Cost_Final = Employee_Cost_Rank.toPandas()