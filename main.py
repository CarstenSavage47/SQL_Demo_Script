# This is a demo of SQL functions using PySpark.

import pandas
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import Row
import openpyxl

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

#   After the tables have been created, upload corresponding files into the tables to load the data.

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

Telco = pandas.read_excel('/Users/carstenjuliansavage/Desktop/R Working Directory/Useful Datasets/Telco_customer_churn.xlsx')

DF_B = pandas.read_excel('/Users/carstenjuliansavage/PycharmProjects/SQL_DEMO_Scripts/DF_B.xlsx')

Telco = Telco.filter(['CustomerID'])

Telco_Spark = spark.createDataFrame(Telco)
Telco_Spark.show()

DF_B_Spark = spark.createDataFrame(DF_B)
DF_B_Spark.show()

# Create referenceable tables
Telco_Spark.createOrReplaceTempView("Telco_Spark")

# Create referenceable tables
DF_B_Spark.createOrReplaceTempView("DF_B_Spark")

# Substring example
spark.sql("SELECT * FROM (SELECT *, SUBSTRING(CustomerID,1,4) AS ID_NUMBER "
          "FROM Telco_Spark) d "
          "WHERE ID_NUMBER = 3668 OR ID_NUMBER = 9305 ").show()

spark.sql("SELECT CustomerID AS CUST_ID, CASE "
          "WHEN CustomerID LIKE '1%' THEN 'Ones' "
          "WHEN CustomerID LIKE '5%' THEN 'Fives' "
          "WHEN CustomerID LIKE '9%' THEN 'Nines' "
          "ELSE 'Uncategorized' "
          "END CUST_CATEGORY "
          " FROM Telco_Spark").show()

# Inner and outer queries, case when, inner join with another table
spark.sql("SELECT * FROM (SELECT CustomerID AS CUST_ID, "
          "CASE "
            "WHEN CustomerID LIKE '1%' THEN 'Ones' "
            "WHEN CustomerID LIKE '5%' THEN 'Fives' "
            "WHEN CustomerID LIKE '9%' THEN 'Nines' "
            "ELSE 'Uncategorized' "
          "END CUST_CATEGORY "
          "FROM Telco_Spark) a "
          "INNER JOIN DF_B_Spark b ON a.CUST_ID=b.CustomerID "
          "WHERE CUST_CATEGORY NOT LIKE 'Uncategorized' "
          "ORDER BY CUST_ID ASC").show()

Meta_Customer_Revenue = pandas.read_excel('/Users/carstenjuliansavage/Desktop/R Working Directory/Useful Datasets/Meta_Customer_Revenue.xlsx')
Meta_Spark = spark.createDataFrame(Meta_Customer_Revenue)
Meta_Spark.show()

# Create referenceable tables
Meta_Spark.createOrReplaceTempView("Meta_Spark")

spark.sql("SELECT *, "
          "AVG(total_order_cost) OVER (ORDER BY ORDER_DATE "
          "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS MOVING_AVERAGE "
          "FROM Meta_Spark").show()