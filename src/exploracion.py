from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("NASA Exploration") \
    .getOrCreate()

# Cargar dataset
logs = spark.read.csv("data.csv", header=True)

print("Total de registros:")
print(logs.count())

# Mostrar ejemplos
print("Primeras filas:")
logs.show(10)

print("Columnas:")
print(logs.columns)

logs.describe().show()