Paso 1: Descarga los archivos necesarios

Descarga el archivo .env.

Descarga el archivo docker-compose.yml.

La carpeta DAG y descarga el archivo "etl_emae".

La carpeta SCRIPT y descarga los archivos "commons" y "EMAE_ETL_spark"


Paso 2: Inicia los servicios

Utiliza el siguiente comando para iniciar los servicios y construir los contenedores:

docker-compose up --build

Paso 3: Accede a Airflow

Una vez que los servicios estén levantados acceder a Airflow en http://localhost:8080/ desde el navegador web.

Paso 4: Configura las conexiones en Airflow

En la pestaña "Admin" -> "Connections" de Airflow, crea una nueva conexión con los siguientes datos para Redshift:

Conn Id: redshift_default
Conn Type: Amazon Redshift
Host: dirección del host de Redshift
Database: nombre de la base de datos de Redshift
Schema: nombre del esquema de Redshift
User: usuario de Redshift
Password: contraseña de Redshift
Port: 5439
Crea otra nueva conexión con los siguientes datos para Spark:

Conn Id: spark_default
Conn Type: Spark
Host: spark://spark
Port: 7077
Extra: {"queue": "default"}
Paso 5: Configura las variables en Airflow

En la pestaña "Admin" -> "Variables" de Airflow, crea una nueva variable con los siguientes datos:

Key: driver_class_path
Value: /tmp/drivers/postgresql-42.5.2.jar

Crea otra nueva variable con los siguientes datos:

Key: spark_scripts_dir
Value: /opt/airflow/scripts

Paso 7: Ejecutar el DAG
Ejecuta el DAG llamado etl_emae para iniciar el proceso de extracción, transformación y carga de datos.
