from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from util import dbmysql


PATH_TRUSTED = '/workspaces/trabalho03_eEDB_011/0_data/trusted/reclamacao'
TABLE_MYSQL = 'DW.DM_INSTITUICAO'

spark = SparkSession.builder.appName("REFINED_INST").config("spark.jars", "/workspaces/trabalho03_eEDB_011/drives/mysql-connector-java-8.0.22.jar").getOrCreate()

spark.read.parquet(PATH_TRUSTED).createOrReplaceTempView('file_csv')

df = spark.sql('''
    select monotonically_increasing_id() as ID_INST, INSTITUIO_FINANCEIRA,CNPJ_IF FROM (select distinct UPPER(INSTITUIO_FINANCEIRA) AS INSTITUIO_FINANCEIRA, CNPJ_IF from file_csv)
    ''')

df.show()

dbmysql.write_mysql(df,TABLE_MYSQL)