{{ config(materialized='table') }}

select 
    MD5(A.CATEGORIA) AS ID_CAT,
    A.CATEGORIA 
from (select distinct 
        upper(CATEGORIA) as CATEGORIA 
      from 
          stage.file_csv_trusted) as A

