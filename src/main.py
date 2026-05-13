from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, from_unixtime, avg

spark = SparkSession.builder \
    .appName("NASA Pipeline") \
    .getOrCreate()

# CARGA
logs = spark.read.csv("data.csv", header=True) #cambiar la ruta si se subió el otro csv de data_clean a "hdfs:///user/hadoop/data_clean.csv"


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

#---------------------------------------------------------
# ANÁLISIS
# --------------------------------------------------------
# ANALISIS 1: Top 10 URLs mas visitadas
# --------------------------------------------------------
print("\n" + "=" * 55)
print("ANALISIS 1: Top 10 URLs mas visitadas")
print("=" * 55)
 
df.groupBy("url") \
    .count() \
    .orderBy(desc("count")) \
    .show(10, truncate=False)
 
# --------------------------------------------------------
# ANALISIS 2: Top 10 hosts con mas peticiones
# --------------------------------------------------------
print("\n" + "=" * 55)
print("ANALISIS 2: Top 10 hosts mas activos")
print("=" * 55)
 
df.groupBy("host") \
    .count() \
    .orderBy(desc("count")) \
    .show(10, truncate=False)
 
# --------------------------------------------------------
# ANALISIS 3: Distribucion de codigos de respuesta HTTP
# --------------------------------------------------------
print("\n" + "=" * 55)
print("ANALISIS 3: Codigos de respuesta HTTP")
print("=" * 55)
 
df.groupBy("response") \
    .count() \
    .withColumn(
        "tipo",
        when(col("response").between(200, 299), "Exitoso")
        .when(col("response").between(300, 399), "Redireccion")
        .when(col("response").between(400, 499), "Error cliente")
        .when(col("response").between(500, 599), "Error servidor")
        .otherwise("Otro")
    ) \
    .orderBy("response") \
    .show()
 
# --------------------------------------------------------
# ANALISIS 4: Trafico por hora del dia
# --------------------------------------------------------
print("\n" + "=" * 55)
print("ANALISIS 4: Peticiones por hora del dia")
print("=" * 55)
 
df.withColumn("hora", hour(col("datetime"))) \
    .groupBy("hora") \
    .count() \
    .orderBy("hora") \
    .show(24)
 
# --------------------------------------------------------
# ANALISIS 5: Trafico por dia de la semana
# --------------------------------------------------------
print("\n" + "=" * 55)
print("ANALISIS 5: Peticiones por dia de la semana")
print("=" * 55)
 
df.withColumn("dia", dayofweek(col("datetime"))) \
    .withColumn(
        "nombre_dia",
        when(col("dia") == 1, "Domingo")
        .when(col("dia") == 2, "Lunes")
        .when(col("dia") == 3, "Martes")
        .when(col("dia") == 4, "Miercoles")
        .when(col("dia") == 5, "Jueves")
        .when(col("dia") == 6, "Viernes")
        .when(col("dia") == 7, "Sabado")
    ) \
    .groupBy("dia", "nombre_dia") \
    .count() \
    .orderBy("dia") \
    .show()
 
# --------------------------------------------------------
# ANALISIS 6: Metodos HTTP
# --------------------------------------------------------
print("\n" + "=" * 55)
print("ANALISIS 6: Distribucion de metodos HTTP")
print("=" * 55)
 
df.groupBy("method") \
    .count() \
    .orderBy(desc("count")) \
    .show()
 
# --------------------------------------------------------
# ANALISIS 7: Top URLs por tamano promedio de respuesta
# --------------------------------------------------------
print("\n" + "=" * 55)
print("ANALISIS 7: Top 10 URLs por bytes promedio")
print("=" * 55)
 
df.filter(col("bytes") > 0) \
    .groupBy("url") \
    .agg(
        spark_round(avg("bytes"), 2).alias("bytes_promedio"),
        count("url").alias("visitas")
    ) \
    .filter(col("visitas") > 5) \
    .orderBy(desc("bytes_promedio")) \
    .show(10, truncate=False)
 
# --------------------------------------------------------
# ANALISIS 8: Bytes totales por tipo de recurso
# --------------------------------------------------------
print("\n" + "=" * 55)
print("ANALISIS 8: Bytes totales por tipo de recurso")
print("=" * 55)
 
df.withColumn(
    "extension",
    when(col("url").contains(".gif"), "gif")
    .when(col("url").contains(".jpg"), "jpg")
    .when(col("url").contains(".jpeg"), "jpeg")
    .when(col("url").contains(".html"), "html")
    .when(col("url").contains(".htm"), "htm")
    .when(col("url").contains(".txt"), "txt")
    .when(col("url").contains(".mpg"), "mpg")
    .when(col("url").contains(".zip"), "zip")
    .otherwise("otro")
) \
    .groupBy("extension") \
    .agg(
        spark_sum("bytes").alias("bytes_totales"),
        count("extension").alias("peticiones")
    ) \
    .orderBy(desc("bytes_totales")) \
    .show()
 
# --------------------------------------------------------
# ANALISIS 9: Hosts con mas errores HTTP (4xx y 5xx)
# --------------------------------------------------------
print("\n" + "=" * 55)
print("ANALISIS 9: Top 10 hosts con mas errores")
print("=" * 55)
 
df.filter(col("response") >= 400) \
    .groupBy("host") \
    .count() \
    .orderBy(desc("count")) \
    .show(10, truncate=False)
 
# --------------------------------------------------------
# ANALISIS 10: Resumen general
# --------------------------------------------------------
print("\n" + "=" * 55)
print("ANALISIS 10: Resumen general del trafico")
print("=" * 55)
 
total = df.count()
errores = df.filter(col("response") >= 400).count()
exitosos = df.filter(col("response").between(200, 299)).count()
bytes_total = df.agg(spark_sum("bytes")).collect()[0][0]
 
print(f"Total de peticiones      : {total:,}")
print(f"Peticiones exitosas (2xx): {exitosos:,} ({100*exitosos/total:.1f}%)")
print(f"Errores (4xx + 5xx)      : {errores:,} ({100*errores/total:.1f}%)")
print(f"Bytes transferidos total : {bytes_total:,}")
print(f"Promedio bytes/peticion  : {bytes_total // total:,}")
 
spark.stop()