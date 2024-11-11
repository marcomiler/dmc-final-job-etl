# PROYECTO FINAL INGENIERÍA DE DATOS

Proyecto final desarrollado en AWS - Ingeniería de datos en DMC. En este repositorio encuentra, principalmente, los script implementados en AWS glue (python con pyspark).

## Evidencia AWS Data Catalog Capa Raw y Stage

![Img aws data catalog - erp cliente raw.](./imgs/glue/data-catalog/datacatalog_erp_cliente_raw.png "Img aws data catalog . erp cliente raw.")

`Imagen de Tabla erp cliente - Capa Raw - Data Catalog RAW (los tipos de datos no han sufrido ninguna transformación)`

![Img aws data catalog erp cliente.](./imgs/glue/data-catalog/datacatalog_erp_cliente_stg.png "Img aws data catalog ra erp cliente.")

`Imagen de Tabla erp cliente - Capa Stage - Data Catalog Stage (los tipos de datos han sido convertidos a string)`

## Pruebas de consultas interactivas realizadas en AWS Athena - Capa Stage y Analytic

![Img athena erp venta.](./imgs/athena/athena_erp_venta.png "Img athena erp venta.")

`Imagen de consulta en AWS Athena a la capa Stage - Tabla venta`

![Img athena erp detalle venta.](./imgs/athena/athena_erp_detalle_venta.png "Img athena erp detalle venta.")

`Imagen de consulta en AWS Athena a la capa Stage - Tabla detalle venta`

![Img athena erp cliente analytics venta.](./imgs/athena/athena-analytics.png "Img athena cliente analytics.")

`Imagen de consulta en AWS Athena a la capa Analytics - Tabla cliente`

### Logs de las ETL:

![Img aws glue - jobs logs - raw.](./imgs/glue/logs/glue-job-raw-log.png "Img aws glue - jobs logs - raw.")

`Imágen de evidencia de los logs de la etl que obtiene las tablas del esquema erp y las almacena en el datacatalog de glue - Capa raw`

![Img aws glue - jobs logs - stg.](./imgs/glue/logs/glue-job-raw-log.png "Img aws glue - jobs logs - stg.")

`Imágen de evidencia de los logs de la etl que obtiene las tablas la capa raw, realiza transformaciones básicas y las almacena en el data cataglog de glue - Capa stg`
