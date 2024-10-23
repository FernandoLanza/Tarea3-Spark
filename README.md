Análisis de la Penetración de Internet en Colombia.

Descripción del Proyecto:
Este proyecto tiene como objetivo analizar la penetración de Internet en diversas regiones de Colombia, utilizando Apache Spark. 
La baja conectividad en ciertas áreas puede limitar el acceso a recursos y oportunidades digitales, mientras que las áreas con alta penetración pueden aprovechar la infraestructura para su desarrollo. 
Este análisis resalta la necesidad de una estrategia nacional de conectividad.

Características Principales:
* Ingesta de Datos: Carga y transformación de datos desde archivos CSV en HDFS.
* Análisis de Datos: Utilización de RDDs, DataFrames y Spark SQL para realizar análisis estadísticos y calcular la penetración de Internet.
* Evolución Temporal: Análisis de la evolución de la penetración de Internet a lo largo del tiempo.
* Estacionalidad: Identificación de patrones estacionales en la penetración de Internet.
* Comparativa Regional: Identificación de departamentos con mayor y menor penetración de Internet.

Tecnologías Utilizadas:
* Apache Spark: Para procesamiento de datos a gran escala.
* PySpark: Interfaz de Python para trabajar con Spark.
* HDFS: Sistema de archivos distribuido utilizado para almacenar los datos.

Instrucciones para la Ejecución:
Pre-requisitos
* Instalación de Apache Spark: Asegúrate de tener Apache Spark instalado en tu máquina. Puedes seguir las instrucciones de instalación de Spark.
* Instalación de Python y PySpark: Asegúrate de tener Python y PySpark instalados. Puedes instalar PySpark utilizando pip:
  pip install pyspark
* Acceso a HDFS: Asegúrate de tener acceso a un clúster de HDFS donde se encuentren los datos.

Ejecución del Proyecto:
* Clonar el repositorio:
  git clone https://github.com/FernandoLanza/Tarea3-Spark
  cd Tarea3-Spark
* Modificar la ruta del archivo: Asegúrate de que la ruta del archivo CSV en el código (file_path) apunte al archivo correcto en tu HDFS.
* Ejecutar el Código: Puedes ejecutar el código utilizando el siguiente comando:
   python3 tarea3.py
* Resultados: se mostrarán en la consola. Asegúrate de revisar las estadísticas básicas, la evolución de la penetración de Internet y los departamentos con mayor y menor penetración.
