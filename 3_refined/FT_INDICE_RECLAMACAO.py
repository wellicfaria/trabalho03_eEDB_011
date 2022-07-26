from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from util import dbmysql


PATH_TRUSTED_API = '/workspaces/trabalho03_eEDB_011/0_data/trusted/api_reclamacao'
PATH_TRUSTED_FATO = '/workspaces/trabalho03_eEDB_011/0_data/trusted/reclamacao'
TABLE_MYSQL = 'DW.FT_INDICE_RECLAMACAO'

spark = SparkSession.builder.appName("REFINED_FT").config("spark.jars", "/workspaces/trabalho03_eEDB_011/drives/mysql-connector-java-8.0.22.jar").getOrCreate()

spark.read.parquet(PATH_TRUSTED_API).createOrReplaceTempView('API_RECLAMACAO')
spark.read.parquet(PATH_TRUSTED_FATO).createOrReplaceTempView('RECLAMACAO')
dbmysql.read_mysql('DW.DM_CATEGORIA',spark).createOrReplaceTempView('DM_CATEGORIA')
dbmysql.read_mysql('DW.DM_TIPO',spark).createOrReplaceTempView('DM_TIPO')
dbmysql.read_mysql('DW.DM_INDICE',spark).createOrReplaceTempView('DM_INDICE')
dbmysql.read_mysql('DW.DM_INSTITUICAO',spark).createOrReplaceTempView('DM_INSTITUICAO')



df = spark.sql('''
    SELECT
        RECLAMACAO.ANO,
        RECLAMACAO.TRIMESTRE,
        DM_CATEGORIA.ID_CAT,
        DM_TIPO.ID_IP,
        DM_INSTITUICAO.ID_INST,
        DM_INDICE.ID_IND,
        RECLAMACAO.QUANTIDADE_DE_RECLAMAES_REGULADAS_PROCEDENTES,
        RECLAMACAO.QUANTIDADE_DE_RECLAMAES_REGULADAS_OUTRAS,
        RECLAMACAO.QUANTIDADE_DE_RECLAMAES_NO_REGULADAS,
        RECLAMACAO.QUANTIDADE_TOTAL_DE_RECLAMAES,
        RECLAMACAO.QUANTIDADE_TOTAL_DE_CLIENTES_CCS_E_SCR,
        RECLAMACAO.QUANTIDADE_DE_CLIENTES_CCS,
        RECLAMACAO.QUANTIDADE_DE_CLIENTES_SCR,
        API_RECLAMACAO.VALORMAXIMO
    FROM RECLAMACAO
    LEFT JOIN API_RECLAMACAO ON RECLAMACAO.CNPJ_IF = API_RECLAMACAO.CNPJ
    LEFT JOIN DM_CATEGORIA ON UPPER(RECLAMACAO.CATEGORIA) = UPPER(DM_CATEGORIA.CATEGORIA)
    LEFT JOIN DM_TIPO ON UPPER(RECLAMACAO.TIPO) = UPPER(DM_TIPO.TIPO)
    LEFT JOIN DM_INDICE ON UPPER(RECLAMACAO.NDICE) = UPPER(DM_INDICE.INDICE)
    LEFT JOIN DM_INSTITUICAO ON UPPER(RECLAMACAO.INSTITUIO_FINANCEIRA) = UPPER(DM_INSTITUICAO.INSTITUIO_FINANCEIRA) AND RECLAMACAO.CNPJ_IF = DM_INSTITUICAO.CNPJ_IF
    WHERE TRIM(UPPER(API_RECLAMACAO.TIPOVALOR)) == 'REAL'
    ''')



dbmysql.write_mysql(df,TABLE_MYSQL)