#tratar os dados da api

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PATH_RAW = '/workspaces/trabalho03_eEDB_011/0_data/raw/api_reclamacao'
PATH_TRUSTED = '/workspaces/trabalho03_eEDB_011/0_data/trusted/api_reclamacao'


spark = SparkSession.builder.appName("TRUSTED_API").getOrCreate()

df = spark.read.parquet(PATH_RAW)


df = df.withColumn('value_2',F.explode(df.value))

#FLAT
df = df.withColumn('CODIGOSERVICO', F.col('value_2').CodigoServico)
df = df.withColumn('DATAVIGENCIA', F.col('value_2').DataVigencia)
df = df.withColumn('PERIODICIDADE', F.col('value_2').Periodicidade)
df = df.withColumn('SERVICO', F.col('value_2').Servico)
df = df.withColumn('TIPOVALOR', F.col('value_2').TipoValor)
df = df.withColumn('UNIDADE', F.col('value_2').Unidade)
df = df.withColumn('VALORMAXIMO', F.col('value_2').ValorMaximo)

#TYPE

df = df.withColumn('DATAVIGENCIA', F.to_date(F.col('DATAVIGENCIA'), 'yyyy-MM-dd'))
df = df.withColumn('VALORMAXIMO', F.col('VALORMAXIMO').cast('float'))
df = df.withColumnRenamed('cnpj', 'CNPJ')
df = df.select('CNPJ','CODIGOSERVICO','DATAVIGENCIA','PERIODICIDADE','SERVICO','TIPOVALOR','UNIDADE', 'VALORMAXIMO')


df.write.mode('overwrite').parquet(PATH_TRUSTED)




