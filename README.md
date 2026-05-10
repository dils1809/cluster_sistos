# Procesamiento del Dataset NASA con PySpark

Este módulo del laboratorio se encarga del procesamiento del dataset de logs web de la NASA utilizando Apache Spark.

Se realizan las siguientes etapas:
- Exploración de datos
- Limpieza y transformación
- Análisis

El objetivo es preparar los datos para su ejecución en un entorno distribuido (Hadoop Cluster).

---

## Dataset
### Fuente del dataset:
Descargarlo:
https://www.kaggle.com/datasets/souhagaa/nasa-access-log-dataset-1995/code

---

## Instrucciones de descarga

1. Descargar el dataset desde el enlace  
2. Descomprimir el archivo  
4. Guardarlo (si es necesario) como: data.csv
5. Colocarlo en la raiz del proyecto

## Requisitos

- Python 3.x  
- PySpark 3.4.1  
- Java 11 o 17
  
´´´
Instalar dependencias: pip install pyspark==3.4.1
´´´

## Exploración de datos

Archivo: `exploracion.py`

Se realiza:
- Conteo de registros (~3 millones)
- Visualización de columnas
- Estadísticas descriptivas

### Hallazgos:

- Dataset de gran tamaño
- Presencia de columna adicional (`_c0`)
- Inconsistencias en el encabezado
- Valores especiales en `bytes` (ej: "-")
- Tipos de datos incorrectos

---
