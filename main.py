# This is a sample Python script.

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

EmployeeOrders_Spark = spark.sql("SELECT * from Orders_Spark a LEFT JOIN Employees_Spark b ON a.id = b.id")

EmployeeOrders_Spark.show()

EmployeeOrders = EmployeeOrders_Spark.toPandas()






Diamonds = pandas.read_csv('/Users/carstenjuliansavage/Desktop/R Working Directory/diamonds.csv')

Diamonds_Spark = spark.createDataFrame(Diamonds)

Diamonds_Spark.show()

Diamonds_Spark.printSchema()

Diamonds_Spark.show(5)

Diamonds_Spark.show(2, vertical=True)

(Diamonds_Spark
 .select(["x", "y", "z"])
 .describe()
 .show()
 )

Diamonds_Spark.collect()

#Diamonds_Pandas = Diamonds_Spark.toPandas()

Diamonds_Spark.select(Diamonds_Spark.carat).show()
import numpy
Diamonds_Spark.withColumn('carat2', 2*(Diamonds_Spark.carat)).show()

Diamonds_Spark.filter(Diamonds_Spark.x > 4).show()

Diamonds_Spark.groupby(['carat','cut']).avg().show()

Diamonds_Spark.createOrReplaceTempView("DIASPARK")
spark.sql("SELECT count(*) from DIASPARK").show()
spark.sql("SELECT * from DIASPARK WHERE CUT LIKE 'I%'").show()