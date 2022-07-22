#tratar os dados arquivo CSV

#tratar os dados da api

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PATH_RAW = '/workspaces/trabalho03_eEDB_011/0_data/raw/reclamacao'
PATH_TRUSTED = '/workspaces/trabalho03_eEDB_011/0_data/trusted/reclamacao'


spark = SparkSession.builder.appName("TRUSTED_CSV").getOrCreate()

df = spark.read.parquet(PATH_RAW)
df = df.drop('C14')

#TYPE
df = df.withColumn('ANO',F.col('ANO').cast('int'))

df = df.withColumn('QUANTIDADE_DE_RECLAMAES_REGULADAS_PROCEDENTES',F.col('QUANTIDADE_DE_RECLAMAES_REGULADAS_PROCEDENTES').cast('bigint'))                                                 
df = df.withColumn('QUANTIDADE_DE_RECLAMAES_REGULADAS_OUTRAS',F.col('QUANTIDADE_DE_RECLAMAES_REGULADAS_OUTRAS').cast('bigint'))                                                  
df = df.withColumn('QUANTIDADE_DE_RECLAMAES_NO_REGULADAS',F.col('QUANTIDADE_DE_RECLAMAES_NO_REGULADAS').cast('bigint'))                                                      
df = df.withColumn('QUANTIDADE_TOTAL_DE_RECLAMAES',F.col('QUANTIDADE_TOTAL_DE_RECLAMAES').cast('bigint'))                                                  
df = df.withColumn('QUANTIDADE_TOTAL_DE_CLIENTES_CCS_E_SCR',F.col('QUANTIDADE_TOTAL_DE_CLIENTES_CCS_E_SCR').cast('bigint'))                                                 
df = df.withColumn('QUANTIDADE_DE_CLIENTES_CCS',F.col('QUANTIDADE_DE_CLIENTES_CCS').cast('bigint'))                                               
df = df.withColumn('QUANTIDADE_DE_CLIENTES_SCR',F.col('QUANTIDADE_DE_CLIENTES_SCR').cast('bigint'))

df = df.withColumn('QUANTIDADE_DE_RECLAMAES_REGULADAS_PROCEDENTES',F.coalesce(F.col('QUANTIDADE_DE_RECLAMAES_REGULADAS_PROCEDENTES'),F.lit(0)))                                                 
df = df.withColumn('QUANTIDADE_DE_RECLAMAES_REGULADAS_OUTRAS',F.coalesce(F.col('QUANTIDADE_DE_RECLAMAES_REGULADAS_OUTRAS'),F.lit(0)))                                                    
df = df.withColumn('QUANTIDADE_DE_RECLAMAES_NO_REGULADAS',F.coalesce(F.col('QUANTIDADE_DE_RECLAMAES_NO_REGULADAS'),F.lit(0)))                                                       
df = df.withColumn('QUANTIDADE_TOTAL_DE_RECLAMAES',F.coalesce(F.col('QUANTIDADE_TOTAL_DE_RECLAMAES'),F.lit(0)))                                                 
df = df.withColumn('QUANTIDADE_TOTAL_DE_CLIENTES_CCS_E_SCR',F.coalesce(F.col('QUANTIDADE_TOTAL_DE_CLIENTES_CCS_E_SCR'),F.lit(0)))                                                 
df = df.withColumn('QUANTIDADE_DE_CLIENTES_CCS',F.coalesce(F.col('QUANTIDADE_DE_CLIENTES_CCS'),F.lit(0)))                                               
df = df.withColumn('QUANTIDADE_DE_CLIENTES_SCR',F.coalesce(F.col('QUANTIDADE_DE_CLIENTES_SCR'),F.lit(0)))

#descastando CNPJ nullo

df = df.where(F.col('CNPJ_IF').cast('bigint').isNotNull())

df.write.mode('overwrite').parquet(PATH_TRUSTED)

