from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

DBT_PROFILE_DIR = "/opt/airflow/data_pipeline/profiles"
DBT_PROJECT_DIR = "/opt/airflow/data_pipeline"

# def dbt_get_year_quarter(**kwargs):
#     year = kwargs['dag_run'].conf.get('year')
#     quarter = kwargs['dag_run'].conf.get('quarter')
#     return f"""

#     """

year = '2024'
quarter='4'

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 1, 1)
}

with DAG(
    dag_id='dbt_dag',
    start_date=datetime(2025, 2, 1),
    schedule_interval='@daily',
    tags = ['sec_datapipelines'],
    default_args=default_args,
    description='Runs dbt pipeline To create and validate fact tables',
    catchup=False
) as dag:
    # dbt_deps = BashOperator(
    # task_id='dbt_deps',
    # bash_command=f'cd {DBT_PROJECT_DIR} && dbt deps --profiles-dir {DBT_PROFILE_DIR}',
    # dag=dag
    # )
    dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command=f"cd {DBT_PROJECT_DIR} && dbt run",
    dag=dag
    )

# Create tasks for different dbt commands
# dbt_debug = BashOperator(
#     task_id='dbt_debug',
#     bash_command=f'cd {DBT_PROJECT_DIR} && dbt debug --profiles-dir {DBT_PROFILE_DIR}',
#     dag=dag
# )

# dbt_seed = BashOperator(
#     task_id='dbt_seed',
#     bash_command=f'cd {DBT_PROJECT_DIR} && dbt seed --profiles-dir {DBT_PROFILE_DIR}',
#     dag=dag
# )

dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command=f'cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROFILE_DIR}',
    dag=dag
)

# Set task dependencies
dbt_run >> dbt_test