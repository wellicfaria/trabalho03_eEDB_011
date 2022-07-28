{{ config(materialized='table') }}

select 
  MD5(A.TIPO) as ID_IP, 
  A.TIPO
FROM (select distinct 
        UPPER(TIPO) AS TIPO 
      from stage.file_csv_trusted) A

    
