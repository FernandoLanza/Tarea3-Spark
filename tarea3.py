# Importamos las librerías necesarias
print("Importando librerías...\n")
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col
from pyspark.sql.types import FloatType

# Inicializa la sesión de Spark
print("Inicializando SPARK...\n")
spark = SparkSession.builder.appName('AnalisisPenetracionInternet').getOrCreate()

# Define la ruta del archivo .csv en HDFS
file_path = 'hdfs://localhost:9000/Tarea3/4py7-br84.csv'

# Lee el archivo .csv
print("Leyendo archivo...\n")
df = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load(file_path)

# Imprime el esquema para revisar las columnas y sus tipos de datos
print("Imprimiendo esquema...\n")
df.printSchema()

# Convertir columnas a tipo adecuado
df = df.withColumn("no_accesos_fijos_a_internet", col("no_accesos_fijos_a_internet").cast(FloatType()))
df = df.withColumn("poblaci_n_dane", col("poblaci_n_dane").cast(FloatType()))
df = df.withColumn("indice", col("indice").cast(FloatType()))

# Calcular el porcentaje de penetración de Internet
df = df.withColumn("penetracion_internet", (col("no_accesos_fijos_a_internet") / col("poblaci_n_dane")) * 100)

# Estadísticas básicas
print("Calculando estadísticas básicas...\n")
stats = df.groupBy("departamento").agg(
    F.mean("penetracion_internet").alias("media"),
    F.expr("percentile_approx(penetracion_internet, 0.5)").alias("mediana"),
    F.stddev("penetracion_internet").alias("desviacion_estandar")
)
stats.orderBy("departamento").show()

# Evolución de la penetración de Internet
print("Calculando la evolución de la penetración de Internet...\n")
evolucion = df.groupBy("a_o", "trimestre").agg(
    F.mean("penetracion_internet").alias("penetracion_media")
).orderBy("a_o", "trimestre")
total_filas = evolucion.count()
evolucion.show(total_filas)

# Análisis de estacionalidad
print("Identificando patrones estacionales...\n")
estacionalidad = df.groupBy("trimestre").agg(
    F.mean("penetracion_internet").alias("penetracion_media_trimestral")
).orderBy("trimestre")
estacionalidad.show()

# Departamentos con mayor y menor penetración
print("Identificando departamentos con mayor y menor penetración...\n")
mayor_penetracion = df.groupBy("departamento").agg(
    F.max("penetracion_internet").alias("max_penetracion")
).orderBy("max_penetracion", ascending=False)

menor_penetracion = df.groupBy("departamento").agg(
    F.min("penetracion_internet").alias("min_penetracion")
).orderBy("min_penetracion")

print("Departamentos con mayor penetración:\n")
mayor_penetracion.show(5)

print("Departamentos con menor penetración:\n")
menor_penetracion.show(5)

# Finalizar la sesión de Spark
print("Finalizando sesión\n")
spark.stop()
