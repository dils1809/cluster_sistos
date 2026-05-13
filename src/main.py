from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, from_unixtime, avg

spark = SparkSession.builder \
    .appName("NASA Pipeline") \
    .getOrCreate()

# CARGA
logs = spark.read.csv("data.csv", header=True)

# LIMPIEZA

# Mostrar columnas originales
print("Columnas originales:")
print(logs.columns)

# Eliminar columna inválida
df = logs.drop("_c0")
print("Columna _c0 eliminada.")

# Mostrar columnas después de limpieza
print("Columnas después de eliminar _c0:")
print(df.columns)

# limpiar registros inválidos
df = df.filter(col("host").isNotNull())
df = df.filter(col("url").isNotNull())
df = df.filter(col("response").isNotNull())

#convertir tipos
df = df.withColumn("response", col("response").cast("int"))

df = df.withColumn(
    "bytes",
    when(col("bytes") == "-", 0).otherwise(col("bytes").cast("int"))
)

# convertir timestamp a fecha
df = df.withColumn("datetime", from_unixtime(col("time")))

# Métricas
total_original = logs.count()
total_clean = df.count()

print("Total original:", total_original)
print("Total limpio:", total_clean)
print("Filas eliminadas:", total_original - total_clean)
print("Porcentaje eliminado:", ((total_original - total_clean) / total_original) * 100)

df.show(10)
df.printSchema()

# Generar csv limpio con hadoop
df.coalesce(1).write.mode("overwrite").option("header", True).csv("data_clean")

# ANÁLISIS