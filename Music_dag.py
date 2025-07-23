from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from _functions.cleaning import extract_Music_data, extract_API_Data, combine_sources, cleaning , cleaning_Clustering, load_to_db,load_to_db_Clustering

# Define the DAG
default_args = {
    "owner": "data_engineering_team",
    "depends_on_past": False,
    'start_date': days_ago(2),
    "retries": 0,
}

with DAG(
    dag_id = 'Music_dag',
    schedule_interval = '@once', # could be @daily, @hourly, etc or a cron expression '* * * * *'
    default_args = default_args,
    tags = ['pipeline', 'etl'],
)as dag:
    # Define the tasks
    extract_Music = PythonOperator(
        task_id = 'extract_Music',
        python_callable = extract_Music_data,
        op_kwargs = {
            'filename': '/opt/airflow/data/df_no_genres.csv',
            'output_path': '/opt/airflow/data/df_no_genres.parquet'
        }
    )

    extract_data_API= PythonOperator(
        task_id = 'extract_data_API',
        python_callable = extract_API_Data,
        op_kwargs = {
            'filename': '/opt/airflow/data/artists_combines.csv',
            'output_path': '/opt/airflow/data/artists_combines.parquet'
        }
    )

    df_Music_Merge =PythonOperator(
        task_id = 'df_Music_Merge',
        python_callable = combine_sources,
        op_kwargs = {
            'filename': '/opt/airflow/data/df_no_genres.parquet',
            'filename1':'/opt/airflow/data/artists_combines.parquet',
            'output_path': '/opt/airflow/data/df_Music_combined.parquet'
        }

    )

    df_Cleaning=PythonOperator(
        task_id = 'df_Cleaning',
        python_callable = cleaning,
        op_kwargs = {
            'filename': '/opt/airflow/data/df_Music_combined.parquet',
            'output_path': '/opt/airflow/data/df_Music_combined_cleaned.parquet'
        }


    )

    df_Cleaning_Modelling=PythonOperator(
        task_id = 'df_Cleaning_Modelling',
        python_callable = cleaning_Clustering,
        op_kwargs = {
            'filename': '/opt/airflow/data/df_Music_combined.parquet',
            'output_path': '/opt/airflow/data/df_Music_combined_clustering.parquet'
        }


    )

    load_to_postgres = PythonOperator(
        task_id = 'load_to_postgres',
        python_callable = load_to_db,
        op_kwargs = {
            'filename': '/opt/airflow/data/df_Music_combined_cleaned.parquet',
            'table_name': 'Music_cleaned',
            'postgres_opt': {
                'user': 'root',
                'password': 'root',
                'host': 'pgdatabase',
                'port': 5432,
                'db': 'data_engineering'
            }
        }
    )


    load_to_postgres_Modelling = PythonOperator(
        task_id = 'load_to_postgres_Modelling',
        python_callable = load_to_db_Clustering,
        op_kwargs = {
            'filename': '/opt/airflow/data/df_Music_combined_clustering.parquet',
            'table_name': 'Music_cleaned_Modelling',
            'postgres_opt': {
                'user': 'root',
                'password': 'root',
                'host': 'pgdatabase',
                'port': 5432,
                'db': 'data_engineering'
            }
        }
    )

    # Define the task dependencies
    extract_Music >> df_Music_Merge >> df_Cleaning >> load_to_postgres
    extract_data_API  >> df_Music_Merge >> df_Cleaning_Modelling >> load_to_postgres_Modelling 