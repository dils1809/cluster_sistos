from pyspark.sql import SparkSession

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName("NASA Exploration") \
    .getOrCreate()

# Cargar dataset
logs = spark.read.csv(
    "data.csv",
    header=True
)

# Mostrar columnas originales
print("Columnas originales:")
print(logs.columns)

# Eliminar columna basura "_c0"
if "_c0" in logs.columns:
    logs = logs.drop("_c0")

# Mostrar columnas después de limpieza
print("Columnas después de eliminar _c0:")
print(logs.columns)

# Mostrar primeras filas
print("Primeras filas:")
logs.show(10)

# Ver esquema
print("Schema:")
logs.printSchema()

# Estadísticas básicas
print("Estadísticas:")
logs.describe().show()