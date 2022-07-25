
from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
with DAG(
    '1_DAG',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function,
        # 'on_success_callback': some_other_function,
        # 'on_retry_callback': another_function,
        # 'sla_miss_callback': yet_another_function,
        # 'trigger_rule': 'all_success'
    },
    description='DAG para rodar o Trabalho T3',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['T3'],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    # t2 = BashOperator(
    #     task_id='sleep',
    #     depends_on_past=False,
    #     bash_command='sleep 5',
    #     retries=3,
    # )
    #INICIO RAW
    raw_t2 = BashOperator(
        task_id='file_csv_raw',
        depends_on_past=False,
        bash_command='python /workspaces/trabalho03_eEDB_011/1_raw/file_csv_raw.py',
    )
    raw_t3 = BashOperator(
        task_id='api_raw',
        depends_on_past=False,
        bash_command='python /workspaces/trabalho03_eEDB_011/1_raw/api_raw.py',
    )

    #INICIO TRUSTED 
    trusted_t4 = BashOperator(
        task_id='file_csv_trusted',
        depends_on_past=False,
        bash_command='python /workspaces/trabalho03_eEDB_011/2_trusted/file_csv_trusted.py',
    )
    trusted_t5 = BashOperator(
        task_id='api_trusted',
        depends_on_past=False,
        bash_command='python /workspaces/trabalho03_eEDB_011/2_trusted/api_trusted.py',
    )
    
    #INICIO REFINED
    refined_t6 = BashOperator(
        task_id='DM_CATEGORIA',
        depends_on_past=False,
        bash_command='python /workspaces/trabalho03_eEDB_011/3_refined/DM_CATEGORIA.py',
    )
    refined_t7 = BashOperator(
        task_id='DM_INDICE',
        depends_on_past=False,
        bash_command='python /workspaces/trabalho03_eEDB_011/3_refined/DM_INDICE.py',
    )
    refined_t8 = BashOperator(
        task_id='DM_TIPO',
        depends_on_past=False,
        bash_command='python /workspaces/trabalho03_eEDB_011/3_refined/DM_TIPO.py',
    )
    refined_t9 = BashOperator(
        task_id='DM_INSTITUICAO',
        depends_on_past=False,
        bash_command='python /workspaces/trabalho03_eEDB_011/3_refined/DM_INSTITUICAO.py',
    )
    refined_t10 = BashOperator(
        task_id='FT_INDICE_RECLAMACAO',
        depends_on_past=False,
        bash_command='python /workspaces/trabalho03_eEDB_011/3_refined/FT_INDICE_RECLAMACAO.py',
    )

    t1 >> raw_t2 >> raw_t3 >> trusted_t4 >> trusted_t5 >> [refined_t6, refined_t7, refined_t8, refined_t9] >> refined_t10