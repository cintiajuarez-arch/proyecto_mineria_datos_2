# Proyecto Integrador - Entrega 1: Diseño e Infraestructura Base de Datos
**Plataforma de Analítica e Ingesta Multi-Tenant para Cloud Provider**

---

## 📌 Descripción del Proyecto

Este repositorio contiene la arquitectura e implementación base para la ingesta y almacenamiento de datos en un **Data Lake Medallion** (zona Bronze) utilizando **Apache Spark (PySpark)** en **Google Colab**. 

El sistema implementa una **arquitectura híbrida (Lambda)** que permite la ingesta determinista de archivos maestros/facturación mediante **procesamiento Batch** y el consumo continuo de eventos de telemetría y uso mediante **Structured Streaming** con tolerancia a fallos.

---

## 🏗️ Arquitectura del Data Lake

El Data Lake está estructurado bajo el patrón **Medallion Architecture** dentro de Google Drive:

```text
/content/drive/MyDrive/Minería de Datos II/Proyectos/datalake/
├── landing/                   # Archivos de origen inmutables (.csv y .jsonl)
│   ├── customers_orgs.csv
│   ├── users.csv
│   ├── resources.csv
│   ├── support_tickets.csv
│   ├── marketing_touches.csv
│   ├── nps_surveys.csv
│   ├── billing_monthly.csv
│   └── usage_events_stream/   # Micro-lotes de eventos JSONL (v1 y v2)
├── bronze/                    # Formato Parquet + Metadatos de auditoría
│   ├── customers_orgs/
│   ├── users/
│   ├── resources/
│   ├── support_tickets/
│   ├── marketing_touches/
│   ├── nps_surveys/
│   ├── billing_monthly/
│   └── usage_events/
└── checkpoints/               # Estado de Structured Streaming para tolerancia a fallos
    └── usage_events/

```

🛠️ Requisitos Previos e Infraestructura
Entorno de Cómputo: Google Colab (Python 3.10+ / Java Runtime Environment 11).

Almacenamiento: Cuenta de Google Drive con la estructura de carpetas configurada.

Bibliotecas Necesarias: pyspark, findspark (se instalan automáticamente al inicio del notebook).

🚀 Guía de Instalación y Ejecución Paso a Paso
Paso 1: Configurar el Almacenamiento en Google Drive
Accede a tu Google Drive.

Crea la siguiente ruta de carpetas:
MyDrive/Minería de Datos II/Proyectos/datalake/landing/

Copia los archivos del dataset provistos por la materia dentro de la carpeta landing/:

Deposita los 7 archivos .csv directamente en landing/.

Deposita la carpeta usage_events_stream/ (con sus archivos .jsonl) dentro de landing/.

Paso 2: Cargar el Notebook en Google Colab
Abre Google Colab.

Sube el notebook del proyecto (01_entrega1_ingesta_bronze.ipynb).

Asegúrate de estar conectado a una instancia de runtime estándar (CPU es suficiente).

Paso 3: Ejecución de las Celdas del Notebook
El notebook está organizado en 4 bloques secuenciales:

1. Setup e Inicialización de PySpark
Monta Google Drive e instala e inicializa la sesión de Spark:

```
from google.colab import drive
drive.mount('/content/drive')

!pip install -q pyspark findspark

import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DataLake_Ingestion_Bronze") \
    .config("spark.sql.parquet.compression.codec", "snappy") \
    .getOrCreate()
```

2. Definición de Rutas y Esquemas Explícitos (StructType)
Establece la ruta base /content/drive/MyDrive/Minería de Datos II/Proyectos/datalake y declara los esquemas explícitos para garantizar el tipado estricto y el soporte de evolución de esquemas v1/v2 (genai_tokens, carbon_kg).

3. Ingesta Batch a Capa Bronze (Maestros y Facturación)
Ejecuta la lectura de los 7 archivos CSV e inserta metadatos de auditoría (ingest_ts y source_file), guardando el resultado en formato Parquet en la zona /bronze/.

4. Ingesta Streaming a Capa Bronze (usage_events_stream)
Ejecuta la ingesta continua utilizando Structured Streaming:

```
events_stream_df = spark.readStream \
    .schema(events_schema) \
    .json(f"{LANDING_PATH}/usage_events_stream") \
    .withColumn("ingest_ts", current_timestamp()) \
    .withColumn("source_file", input_file_name())

query = events_stream_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .option("path", f"{BRONZE_PATH}/usage_events") \
    .start()

# Aguarda el procesamiento del micro-lote inicial y detiene el stream de forma limpia
import time
time.sleep(10)
query.stop()
```

Paso 4: Verificación de Resultados
Al finalizar la ejecución de la última celda del notebook, se desplegarán en pantalla los logs de validación:

Esquema Parquet generado en la Capa Bronze.

Conteo total de registros procesados.

Muestra de los primeros 3 registros confirmando las columnas técnicas agregadas (ingest_ts y source_file).

📋 Resumen de Decisiones de Arquitectura
Formatos: Se seleccionó Parquet por su compresión columnar y eficiencia en lectura I/O distribuida.

Campos Técnicos: Todas las tablas Bronze incluyen ingest_ts (TimestampType) e source_file (StringType) para garantizar la trazabilidad y el linaje de datos.

Control de Fallos: Implementación de checkpointLocation para reanudar flujos streaming en caso de interrupción de runtime en Colab.
