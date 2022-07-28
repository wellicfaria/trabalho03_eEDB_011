{{ config(materialized='table') }}

select 
    MD5(A.INDICE) as ID_IND, 
    A.INDICE 
FROM (select distinct COALESCE(upper(NDICE),-1) as INDICE from stage.file_csv_trusted WHERE NDICE IS NOT NULL) A
    
