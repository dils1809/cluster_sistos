from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, from_unixtime
import time

spark = SparkSession.builder \
    .appName("NASA Pipeline ETL") \
    .enableHiveSupport() \
    .getOrCreate()

t_inicio = time.time()

# PASO 1 — CARGA DESDE HDFS
print("\n" + "=" * 60)
print("PASO 1: CARGANDO DATASET DESDE HDFS")
print("=" * 60)

logs = spark.read.csv("hdfs:///nasa/data.csv", header=True)

print("Columnas originales:", logs.columns)
print("Primeras filas del dataset crudo:")
logs.show(10, truncate=False)

# PASO 2 — LIMPIEZA

print("\n" + "=" * 60)
print("PASO 2: LIMPIEZA DE DATOS")
print("=" * 60)

# Eliminar columna de índice inválida si existe
if "_c0" in logs.columns:
    df = logs.drop("_c0")
    print("Columna _c0 eliminada.")
else:
    df = logs

print("Columnas después de limpieza:", df.columns)

# Filtrar registros inválidos
df = df.filter(col("host").isNotNull())
df = df.filter(col("url").isNotNull())
df = df.filter(col("response").isNotNull())

# Convertir tipos de datos
df = df.withColumn("response", col("response").cast("int"))

df = df.withColumn(
    "bytes",
    when(col("bytes") == "-", 0).otherwise(col("bytes").cast("int"))
)

# Convertir timestamp Unix a fecha legible
df = df.withColumn("datetime", from_unixtime(col("time")))


# PASO 3 — MÉTRICAS DE LIMPIEZA

print("\n" + "=" * 60)
print("PASO 3: MÉTRICAS DE LIMPIEZA")
print("=" * 60)

total_original = logs.count()
total_clean    = df.count()
eliminados     = total_original - total_clean
pct_eliminado  = (eliminados / total_original) * 100

print(f"Total registros originales : {total_original:,}")
print(f"Total registros limpios    : {total_clean:,}")
print(f"Filas eliminadas           : {eliminados:,}")
print(f"Porcentaje eliminado       : {pct_eliminado:.2f}%")
print(f"Nodos en el clúster        : {spark.sparkContext.defaultParallelism}")

print("\nMuestra del dataset limpio:")
df.show(10, truncate=False)
df.printSchema()

# PASO 4 — GUARDAR EN HDFS

print("\n" + "=" * 60)
print("PASO 4: GUARDANDO DATASET LIMPIO EN HDFS")
print("=" * 60)

df.write.mode("overwrite").option("header", True).csv("hdfs:///nasa/clean_data")

t_total = time.time() - t_inicio
print(f"Dataset limpio guardado en: hdfs:///nasa/clean_data")
print(f"Tiempo total del pipeline ETL: {t_total:.2f} segundos")
print("=" * 60)

spark.stop()
