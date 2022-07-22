from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from util import dbmysql


PATH_TRUSTED = '/workspaces/trabalho03_eEDB_011/0_data/trusted/reclamacao'
TABLE_MYSQL = 'DW.DM_TIPO'

spark = SparkSession.builder.appName("REFINED_TIP").config("spark.jars", "/workspaces/trabalho03_eEDB_011/drives/mysql-connector-java-8.0.22.jar").getOrCreate()

spark.read.parquet(PATH_TRUSTED).createOrReplaceTempView('file_csv')

df = spark.sql('''
    select monotonically_increasing_id() as ID_IP, TIPO FROM (select distinct UPPER(TIPO) AS TIPO from file_csv)
    ''')

df.show()

dbmysql.write_mysql(df,TABLE_MYSQL)