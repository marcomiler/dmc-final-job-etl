#########################################################
# ETL UTILIZADA PARA OBTENER LAS TABLAS DEL ESQUEMA ERP #
#       Y ALMACENARLAS EN EL DATALAKE - CAPA RAW        #
#########################################################

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import boto3
from botocore.exceptions import ClientError
import json

#################################
# OBTENER CREDENCIALES DE BD
#################################
def get_secret(secret_name, region_name):
    logger.info("PASO 2 - Obtener credenciales BD")
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        logger.error(f"Error al obtener el secreto: {str(e)}")
    return json.loads(get_secret_value_response['SecretString'])

#################################
# OBTENER TABLAS DEL ESQUERMA
#################################
def get_tables(spark, jdbc_conf, schema_name):
    logger.info(f"PASO 3 - Obtener Tablas de esquema {schema_name}")

    query = f"""
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = '{schema_name}'
    """

    df = spark.read.format("jdbc").options(**jdbc_conf).option("query", query).load()
    tables = [row["table_name"] for row in df.collect()]
    return tables

#########################################################
# RECORRER TABLAS Y ALMACENARLAS EN EL CATÁLOGO DE GLUE
#########################################################
def process_table(index, table_name):
    """
    Procesa una tabla individual y la guarda en S3 como CSV utilizando glue_context.getSink.
    """
    
     # Configuración de la consulta
    query = f"""
    SELECT * FROM {schema_name}.{table_name}
    """
    
    logger.info(f"PASO 4.{index}.1 - Extrayendo información de tabla {table_name}")

    df = spark.read.format("jdbc").options(**jdbc_conf).option("query", query.format()).load()
        
    # Convertir DataFrame a DynamicFrame
    dynamic_frame = DynamicFrame.fromDF(df, glue_context, f"{schema_name}_{table_name}")
    
    # Configurar el sink para escribir en S3 y actualizar el catálogo
    output_path = f"{s3_path_base}/{schema_name}_{table}"
    sink = glue_context.getSink(
        connection_type     = "s3",
        path                = output_path,
        enableUpdateCatalog = True,
        transformation_ctx  = "sink",
        updateBehavior      = "UPDATE_IN_DATABASE",
        partitionKeys       = [],
    )

    logger.info(f"PASO 4.{index}.2 - Resultados guardados en {output_path}")
    
    # Configurar las propiedades del catálogo
    sink.setCatalogInfo(
        catalogDatabase  = data_catalog_db,
        catalogTableName = f"{schema_name}_{table_name}"
    )
    
    # Configurar el formato de salida
    sink.setFormat("csv", options={
        "separator": ",",
        "quoteChar": '"',
        "delimiter": ",",
        "escaper": "\\",
        "writeHeader": True,
        "compression": "none",
        "skip.header.line.count": "1"
    })
    
    # Escribir los datos
    sink.writeFrame(dynamic_frame)
    
    logger.info(f"PASO 4.{index}.3 - Actualizando catalogo para {schema_name}_{table}")


# Inicializar el job de Glue y configurar las variables
args         = getResolvedOptions(sys.argv, ['JOB_NAME', 'schemaName', 'secretName', 's3BucketLake', 'dataCatalogDb'])
sc           = SparkContext()
glue_context = GlueContext(sc)
spark        = glue_context.spark_session
job          = Job(glue_context)
logger       = glue_context.get_logger()
job.init(args['JOB_NAME'], args)


# Configurar las variables basadas en los argumentos de entrada
logger.info("PASO 1 - Seteo de parametros")
schema_name     = args['schemaName']
data_catalog_db = args['dataCatalogDb']
s3_bucket_lake  = args['s3BucketLake']
s3_path_base    = f"s3://{s3_bucket_lake}/raw"
secret_name     = args['secretName']
region          = "us-east-1"

secret = get_secret(secret_name, region)

# Configurar la conexión JDBC
jdbc_conf = {
    "url": f"jdbc:postgresql://{secret['host']}:5432/dmc",
    "user": secret['username'],
    "password": secret['password'],
    "driver": "org.postgresql.Driver"
}

tables = get_tables(spark, jdbc_conf, schema_name)

for index, table in enumerate(tables):
    process_table(index, table)

try:
    # Realiza el commit del Job
    job.commit()
    logger.info("PASO 5 - Trabajo de Glue finalizado correctamente")
except Exception as e:
    logger.error(f"Error al finalizar el trabajo de Glue: {e}")
    raise e