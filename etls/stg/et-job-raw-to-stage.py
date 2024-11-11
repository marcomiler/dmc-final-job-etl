#########################################################
#  ETL UTILIZADA PARA OBTENER LAS TABLAS DE LA CAPA RAW #
#    REALIZAR TRANSFORMACIONES BÁSICAS Y ALMACENARLAS   #
#                  EN LA CAPA STAGE                     # 
#########################################################

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, trim, lower
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.sql.types import StringType

#Iniciar instancias
args            = getResolvedOptions(sys.argv, ['JOB_NAME', 'stageCatalogDb', 'bucketLake'])
sc              = SparkContext.getOrCreate()
glueContext     = GlueContext(sc)
spark           = glueContext.spark_session
logger          = glueContext.get_logger()
job             = Job(glueContext)
job.init(args['JOB_NAME'], args)

#Seteo de parametros
logger.info("PASO 1 - Seteo de parametros")
stageCatalogDb  = args['stageCatalogDb']
s3BucketLake    = args['bucketLake']     
stage_s3_path_raw   = f"s3://{s3BucketLake}/raw"
stage_s3_path_stg   = f"s3://{s3BucketLake}/stg"

# Tablas Raw
raw_tables = ["erp_cliente", "erp_venta", "erp_tienda", "erp_detalle_venta", "erp_articulo", "erp_punto_venta", "erp_vendedor"]


# Realizar transformaciones básicas
def transform_to_string_and_clean(df):
    # Convierte todas las columnas a String
    for column in df.columns:
        df = df.withColumn(column, col(column).cast(StringType()))
    
    df = df.select([trim(col(c)).alias(c) for c in df.columns])   # Elimina espacios en blanco
    df = df.select([lower(col(c)).alias(c) for c in df.columns])  # Convierte texto a minúsculas
    
    return df


def process_table(index, table):
    try:
        logger.info(f"PASO 2.{index} Inicio - Procesando tabla: {table}")

        dynamic_frame = glueContext.create_dynamic_frame.from_options(
            format_options={
                "quoteChar": "\"",
                "withHeader": True,
                "separator": ","
            },
            connection_type="s3",
            format="csv",
            connection_options={
                "paths": [f"{stage_s3_path_raw}/{table}/"],
                "recurse": True
            },
            transformation_ctx="df_raw"
        )

        df = dynamic_frame.toDF()

        # Aplicar la transformación a String y limpieza básica
        transformed_df = transform_to_string_and_clean(df)

        # Convertir de nuevo a DynamicFrame para guardar en S3
        tbl_gdf = DynamicFrame.fromDF(transformed_df, glueContext, "tbl_gdf")

        #################################
        # Escribe en S3 los Resultados
        #################################
        output_path = f"{stage_s3_path_stg}/{table}/"
        sink = glueContext.getSink(
            path                = output_path,
            connection_type     = "s3",
            compression         = "snappy",
            enableUpdateCatalog = True,
            transformation_ctx  = "sink",
            updateBehavior      = "UPDATE_IN_DATABASE",
            
        )
        
        #################################
        # ACTUALIZA EL CATÁLOGO DE GLUE
        #################################
        sink.setFormat("glueparquet")
        sink.setCatalogInfo(
            catalogDatabase  = stageCatalogDb,
            catalogTableName = table
        )
        sink.writeFrame(tbl_gdf)

        logger.info(f"PASO 2.{index} Fin - Tabla: {table} procesada y almacenada en Stage")
        
    except Exception as e:
        logger.error(f"Error procesando tabla: {table}: {e}")

for index, table in enumerate(raw_tables):
    process_table(index, table)


try:
    # Realiza el commit del Job
    job.commit()
    logger.info("PASO 3 - Trabajo de Glue finalizado correctamente")
except Exception as e:
    logger.error(f"Error al finalizar el trabajo de Glue: {e}")
    raise e