#########################################################
#  ETL UTILIZADA PARA OBTENER LAS TABLAS DE LA CAPA STG #
# PARA APLICAR REGLAS DE NEGOCIO Y PREPARARLAS PARA SER #
#             SER ENVIADAS AL DATAWAREHOUSE             # 
#########################################################

## PDTA: ME HUBIERA GUSTADO TERMINAR PERO AFINANDO DETALLES
## ME GANÓ EL TIEMPO. PERO DEJO UN EJEMPLO USANDO SQL
## QUE PRETENDO USAR EN LOS CÁLCULOS DE LAS VENTAS Y DEMÁS.

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
import re

#Iniciar instancias
args            = getResolvedOptions(sys.argv, ['JOB_NAME', 'analyticsCatalogDb', 'bucketLake'])
sc              = SparkContext.getOrCreate()
glueContext     = GlueContext(sc)
spark           = glueContext.spark_session
logger          = glueContext.get_logger()
job             = Job(glueContext)
job.init(args['JOB_NAME'], args)

#Seteo de parametros
logger.info("PASO 1 - Seteo de parametros")
analyticsCatalogDb      = args['analyticsCatalogDb']
s3BucketLake            = args['bucketLake']     
stage_s3_path_stg       = f"s3://{s3BucketLake}/stg"
stage_s3_path_analytics = f"s3://{s3BucketLake}/analytics"


# Obtener tabla de capa stage
def get_table_stg(table):
  try:
    logger.info(f"PASO 2 - Obtener tabla stg: {table}")

    dynamic_frame = glueContext.create_dynamic_frame.from_options(
      connection_type="s3",
      format="parquet",
      connection_options={
        "paths": [f"{stage_s3_path_stg}/{table}/"],
        "recurse": True
      },
      transformation_ctx="dynamic_frame"
    )

    return dynamic_frame.toDF()

  except Exception as e:
    logger.error(f"Error procesando tabla: {table}: {e}")

# Guardar tabla en capa analytics
def save_table_process(table, df_process):
  try:
    #################################
    # Escribe en S3 los Resultados
    #################################

    # Convertir de nuevo a DynamicFrame para guardar en S3
    tbl_gdf = DynamicFrame.fromDF(df_process, glueContext, "tbl_gdf")

    output_path = f"{stage_s3_path_analytics}/{table}/"
    sink = glueContext.getSink(
      path                = output_path,
      connection_type     = "s3",
      compression         = "snappy",
      enableUpdateCatalog = True,
      transformation_ctx  = "sink",
      updateBehavior      = "UPDATE_IN_DATABASE"        
    )
    
    #################################
    # ACTUALIZA EL CATÁLOGO DE GLUE
    #################################
    sink.setFormat("glueparquet")
    sink.setCatalogInfo(
      catalogDatabase  = analyticsCatalogDb,
      catalogTableName = table
    )
    sink.writeFrame(tbl_gdf)

    logger.info(f"PASO 4: {table} procesada y almacenada en Stage")
  except Exception as e:
    logger.error(f"Error al guardar tabla: {table}: {e}")

def get_client_data():
  table_name = "erp_cliente"
  get_table_stg(table_name).createOrReplaceTempView("tmp_erp_cliente")   

  logger.info(f"PASO 3 - afinar tipos de datos para el envío al datawarehouse - {table_name}")

  df_client = spark.sql("""
    SELECT
      CAST(cl_id AS INT)        AS cliente_id,
      CAST(nom AS VARCHAR(50))  AS cliente_nombres,
      CAST(mail AS VARCHAR(50)) AS cliente_correo,
      CAST(tel AS VARCHAR(50))  AS cliente_telefono,
      CAST(dir AS VARCHAR(50))  AS cliente_direccion
    FROM tmp_erp_cliente
  """)

  save_table_process(table_name, df_client)

get_client_data()

try:
  # Realiza el commit del Job
  job.commit()
  logger.info("PASO 3 - Trabajo de Glue finalizado correctamente")
except Exception as e:
  logger.error(f"Error al finalizar el trabajo de Glue: {e}")
  raise e