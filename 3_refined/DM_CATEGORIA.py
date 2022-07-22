from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from util import dbmysql


PATH_TRUSTED = '/workspaces/trabalho03_eEDB_011/0_data/trusted/reclamacao'
TABLE_MYSQL = 'DW.DM_CATEGORIA'



spark = SparkSession.builder.appName("REFINED_CAT").config("spark.jars", "/workspaces/trabalho03_eEDB_011/drives/mysql-connector-java-8.0.22.jar").getOrCreate()



spark.read.parquet(PATH_TRUSTED).createOrReplaceTempView('file_csv')

df = spark.sql('''
    select monotonically_increasing_id() as ID_CAT, CATEGORIA from (select  distinct upper(CATEGORIA) as CATEGORIA from file_csv)
    ''')

df.show()


dbmysql.write_mysql(df,TABLE_MYSQL)





