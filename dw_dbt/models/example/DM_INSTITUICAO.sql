{{ config(materialized='table') }}

select 
    MD5(CONCAT(A.INSTITUIO_FINANCEIRA, A.CNPJ_IF)) as ID_INST, 
    A.INSTITUIO_FINANCEIRA,
    A.CNPJ_IF 
FROM (select distinct 
        UPPER(INSTITUIO_FINANCEIRA) AS INSTITUIO_FINANCEIRA, 
        CNPJ_IF 
      from stage.file_csv_trusted) A

    
