from pyspark.sql import SparkSession
import time

spark = SparkSession.builder \
    .appName("NASA Experiment") \
    .enableHiveSupport() \
    .getOrCreate()

t_inicio = time.time()

# CARGAR TABLA HIVE COMO DATAFRAME

print("\n" + "=" * 60)
print("CARGANDO TABLA HIVE: nasa_partitioned")
print("=" * 60)

df_hive = spark.sql("SELECT * FROM nasa_partitioned")
df_hive.createOrReplaceTempView("logs")

print("Muestra de la tabla Hive:")
df_hive.show(10)

# Q1 — TOTAL DE REGISTROS

print("\n" + "=" * 60)
print("Q1: TOTAL DE REGISTROS EN EL CLÚSTER")
print("=" * 60)
t1 = time.time()

total = df_hive.count()
print(f"Total de registros procesados: {total:,}")
print(f"Nodos participando: {spark.sparkContext.defaultParallelism}")
print(f"Tiempo Q1: {time.time() - t1:.2f}s")

# Q2 — DISTRIBUCIÓN DE CÓDIGOS HTTP

print("\n" + "=" * 60)
print("Q2: DISTRIBUCIÓN DE CÓDIGOS HTTP")
print("=" * 60)
t2 = time.time()

spark.sql("""
    SELECT
        response,
        COUNT(*) AS total_peticiones,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM logs), 2) AS porcentaje
    FROM logs
    GROUP BY response
    ORDER BY total_peticiones DESC
""").show()

print(f"Tiempo Q2: {time.time() - t2:.2f}s")

# Q3 — TOP 10 URLS MÁS VISITADAS

print("\n" + "=" * 60)
print("Q3: TOP 10 URLs MÁS VISITADAS")
print("=" * 60)
t3 = time.time()

spark.sql("""
    SELECT
        url,
        COUNT(*) AS visitas
    FROM logs
    GROUP BY url
    ORDER BY visitas DESC
    LIMIT 10
""").show(truncate=False)

print(f"Tiempo Q3: {time.time() - t3:.2f}s")


# Q4 — TRÁFICO POR HORA DEL DÍA

print("\n" + "=" * 60)
print("Q4: TRÁFICO POR HORA DEL DÍA")
print("=" * 60)
t4 = time.time()

spark.sql("""
    SELECT
        SUBSTR(datetime, 12, 2) AS hora,
        COUNT(*)                AS requests
    FROM logs
    WHERE datetime IS NOT NULL
    GROUP BY SUBSTR(datetime, 12, 2)
    ORDER BY hora
""").show(24)

print(f"Tiempo Q4: {time.time() - t4:.2f}s")

# Q5 — HOSTS CON MÁS ERRORES (4xx + 5xx)

print("\n" + "=" * 60)
print("Q5: HOSTS CON MÁS ERRORES (4xx y 5xx)")
print("=" * 60)
t5 = time.time()

spark.sql("""
    SELECT
        host,
        COUNT(*) AS errores
    FROM logs
    WHERE response >= 400
    GROUP BY host
    ORDER BY errores DESC
    LIMIT 10
""").show(truncate=False)

print(f"Tiempo Q5: {time.time() - t5:.2f}s")

# RESUMEN FINAL
t_total = time.time() - t_inicio

print("\n" + "=" * 60)
print("RESUMEN DE TIEMPOS — TABLA COMPARATIVA")
print("=" * 60)
print(f"  Q1 Total registros   : {time.time() - t1:.2f}s  ← anotar vs Hive")
print(f"  Q2 Status codes      : {time.time() - t2:.2f}s  ← anotar vs Hive")
print(f"  Q3 Top 10 URLs       : {time.time() - t3:.2f}s  ← anotar vs Hive")
print(f"  Q4 Tráfico por hora  : {time.time() - t4:.2f}s  ← anotar vs Hive")
print(f"  Q5 Errores por host  : {time.time() - t5:.2f}s")
print(f"  TIEMPO TOTAL         : {t_total:.2f}s")
print("=" * 60)

spark.stop()
